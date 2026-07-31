## [2026-03-31] Inicialização de lições aprendidas

**Context:** Setup inicial do repositório e automação CI/CD.
**What went wrong:** O arquivo ainda não existia no início de uma nova task.
**Root cause:** Checklist de início não foi aplicado de forma consistente.
**Prevention:** Sempre criar e ler `lessonLearned.md` antes de qualquer alteração técnica.
## [2026-03-31] Workflow visível e publicação NuGet bloqueada por dependências

**Context:** Correção de GitHub Actions para release OSS.
**What went wrong:** O pipeline só disparava por tag e os projetos não tinham versões em `PackageReference`, causando falha NU1015 no `dotnet restore/pack`.
**Root cause:** Setup inicial incompleto de CI (sem `workflow_dispatch`) e metadados de dependências não finalizados.
**Prevention:** Incluir gatilho manual no workflow de release e validar `dotnet build` + `dotnet pack` localmente antes do primeiro push de release.

## [2026-07-30] Porte: não inventar chamadas SQL diretas quando o MSSQL delega

**Context:** Polimento final do porte; `PurgeInstanceStateAsync(PurgeInstanceFilter)` crashava em runtime.
**What went wrong:** O porte inventou uma chamada direta a `purge_instance_state_by_time($1, $2)` passando uma string CSV de status como 2º arg, mas a função SQL declara esse parâmetro como `SMALLINT` → `PostgresException (invalid input syntax for type smallint)`. O MSSQL original **não** faz isso: monta um `SqlOrchestrationQuery`, coleta IDs via `GetManyOrchestrationsAsync` (loop paginado), e deleta por array de IDs.
**Root cause:** Ao portar um método que no MSSQL delega para query-then-delete, o porte inventou um atalho com tipos incompatíveis em vez de espelhar o fluxo reutilizando as primitivas já portadas.
**Prevention:** Ao portar um método, primeiro ler a implementação MSSQL completa. Se ela delegar para outro método (query → delete), portar o mesmo fluxo reutilizando o que já existe. Verificar tipos paramétricos contra a assinatura da função SQL antes de chamar. Adicionar um teste de integração que exerça o caminho (não havia teste de purga — justamente por isso o bug sobreviveu).

## [2026-07-30] AddWithValue + placeholders posicionais podem não casar overloads de função PostgreSQL

**Context:** `GetManyOrchestrationsAsync` falhava com `42883: function ... does not exist`.
**What went wrong:** Usar `cmd.Parameters.AddWithValue(int)` envia o tipo como `integer`, mas a função SQL declara `p_page_size SMALLINT`; `AddWithValue(string)` envia `text`, mas a função quer `VARCHAR(200)`. Combinado com `DateTime` de `Kind=Unspecified` (inferido como `timestamp without time zone` vs `timestamptz`), o PostgreSQL não consegue resolver o overload → erro 42883. Isto só apareceu porque o teste de purga foi o primeiro a chamar `GetManyOrchestrationsAsync`.
**Root cause:** Confiar na inferência de tipos do Npgsql para overloads de função com tipos estritos (SMALLINT/VARCHAR/timestamptz) e em `DateTime` sem `Kind=Utc`.
**Prevention:** Para chamadas a funções PostgreSQL com assinatura estrita, declarar `NpgsqlParameter` com `NpgsqlDbType` explícito (e parâmetros **posicionais** sem nome, casando com `$1..$N`). Sempre normalizar `DateTime` para `Kind=Utc` (`DateTime.SpecifyKind`) ao passar para colunas/parâmetros `TIMESTAMP WITH TIME ZONE`.

## [2026-07-30] Deploy de schema concorrente + testes de integração compartilhando DB

**Context:** Testes de integração falhavam intermitentemente com `23505 duplicate key`/`40P01 deadlock` em `CREATE SCHEMA`, e orquestrações davam timeout.
**What went wrong:** (1) Múltiplas classes de teste (xUnit paralelo) e múltiplos serviços por classe disparavam `CREATE SCHEMA IF NOT EXISTS` em transações concorrentes — o `IF NOT EXISTS` não é atômico entre transações, então a perdedora viola `pg_namespace_nspname_index` ou detecta deadlock. (2) Classes paralelas compartilham o mesmo DB/schema/task hub, competindo por work-item locks.
**Root cause:** Deploy de schema não era resiliente a concorrência; e testes de integração que compartilham um recurso físico rodavam em paralelo.
**Prevention:** Tornar o deploy idempotente-retryable: capturar `23505`/`40P01` no `DeploySchemaAsync` e retentar (os scripts são idempotentes). Para testes de integração que compartilham DB/schema/task hub, serializar via `[CollectionDefinition]` + `[Collection("...")]` em todas as classes. Validar estabilidade rodando a suíte 3x (schema quente e frio).

## [2026-07-30] Materialização de DateTime via JSON perde DateTimeKind (Issue #3)

**Context:** Issue #3 do time Argus — provider quebra no sidecar gRPC v2 do DTFx (`Timestamp.FromDateTime` exige `Utc`).
**What went wrong:** Timestamps lidos de **JSON** (`JsonElement.GetDateTime()` em `PostgreSqlUtils`) vinham com `Kind=Unspecified`/`Local`, ao passo que os lidos do **`NpgsqlDataReader`** eram normalizados via `GetUtcDateTime`. As colunas são `TIMESTAMPTZ` (valores UTC), mas o round-trip JSON derrubava o `Kind`. Afetava `HistoryEvent.Timestamp`, `TimerCreatedEvent.FireAt`/`TimerFiredEvent.FireAt` (via `GetVisibleTime`) e `DistributedTraceContext.ActivityStartTime`. Separadamente, `OrchestrationState.Tags` (field público `IDictionary<string,string>`) ficava `null` → `MapField.Add(null)` estourava.
**Root cause:** Caminho de leitura por JSON não espelhava a normalização do caminho por reader; e `Tags` nunca era inicializado (não há coluna de schema).
**Prevention:** Em qualquer materialização de `DateTime` a partir de JSON/reader, sempre `DateTime.SpecifyKind(value, DateTimeKind.Utc)` — espelhar o helper `GetUtcDateTime` existente. Não deixar coleções em `OrchestrationState`/`HistoryEvent` como `null` quando consumidores assumem não-nulo (o backend in-memory do DTFx retorna vazio). Para testar internos, usar `InternalsVisibleTo` + teste unitário focado em `GetHistoryEvent` com JSON sintético, além do teste de integração.

## [2026-07-30] Mesma classe de bug em métodos gêmeos + isolamento de task hub em testes

**Context:** Bug 42883 em `GetManyOrchestrationsAsync` corrigido; depois `GetOrchestrationWithQueryAsync` (método "gêmeo", mesma função SQL) tinha o mesmo bug latente; e testes flaky em warm runs.
**What went wrong:** (1) Ao corrigir um método que chama uma função SQL com tipos estritos, esqueceu-se de aplicar o mesmo fix ao método público `GetOrchestrationWithQueryAsync`, que chama a MESMA função `query_many_orchestrations` com `AddWithValue` não-tipado — latente, só não crashava por inferência sortuda do Npgsql. (2) Testes de integração compartilhavam o task hub `"TestHub"`: o `OrchestrationsTests` cria instâncias que ficam `Pending` para sempre (sem worker registrado); em warm runs subsequentes, o worker do `PurgeTests` pegava essas instâncias residuais primeiro (menor sequence_number), falhava ao despachar (nome desconhecido), abandonava, e nunca chegava à instância de teste → timeout.
**Root cause:** (1) Não auditar todos os callers de uma função ao corrigir seu tipo de parâmetro. (2) Falta de isolamento entre classes de teste — task hub compartilhado acumula resíduo que contamina workers de outras classes.
**Prevention:** Ao corrigir o binding de parâmetros de uma função SQL, grep por TODOS os callers dessa função e aplicar o fix a cada um (gêmeos). Para testes de integração no mesmo DB físico, dar a cada classe de teste um `TaskHubName` único (isolamento por namespace no mesmo schema) — assim resíduo de uma classe nunca é pego pelo worker de outra. Validar warm runs (sem drop de schema entre execuções), não só cold runs.

## [2026-07-30] Testes de runtime expuseram 3 bugs reais (sub-orch, ContinueAsNew, eventos)

**Context:** Item 1 do caminho 1.0.0 — adicionar testes de integração para caminhos centrais do runtime não cobertos.
**What went wrong:** Os testes que PASSAM confirmam caminhos sãos: **atividade** (`lock_next_task`/`complete_tasks`) e **timer** (`TimerCreated`/`TimerFired` + `visible_time`) funcionam end-to-end. Mas três testes expuseram bugs reais:
1. **Sub-orchestration**: o `SubOrchestrationInstanceCompletedEvent` não chega ao parent — a folha completa mas o parent fica `Running` para sempre. Log confirmou que `orchestratorMessages` está **vazio** no checkpoint da sub que completa, e o provider filtrava só `ExecutionStartedEvent` (fix aplicado: repassar todas as mensagens, mas insuficiente — o evento não está em `orchestratorMessages` nesta versão do runtime; precisa investigar a via de propagação de sub-completion do DTFx).
2. **ContinueAsNew**: o `continuedAsNewMessage` era **completamente ignorado** pelo provider (não havia código que o processasse); a orquestração completa na primeira execução em vez de reiniciar. Reverteram-se tentativas; a semântica de continue-as-new exige tratamento dedicado no SQL/C#.
3. **Evento externo**: `OrchestrationContext.WaitForExternalEvent` não é resolvível em DurableTask.Core 3.7.1 — precisa confirmar a API v1.

**Root cause:** O porte focou no happy-path (create/get-state/purge) e deixou os caminhos de mensagens inter-orquestração (sub-completion, continue-as-new) sem roteamento correto. O `CompleteTaskOrchestrationWorkItemAsync` não repassa `orchestratorMessages`/`continuedAsNewMessage` como o MSSQL faz (MSSQL: `AddOrchestrationEventsParameter(orchestratorMessages, timerMessages, continuedAsNewMessage)` sem filtro).
**Prevention:** Ao portar `CompleteTaskOrchestrationWorkItemAsync`, repassar TODAS as listas de mensagens (`orchestratorMessages`, `timerMessages`, `continuedAsNewMessage`) ao SQL, espelhando o MSSQL — não filtrar por tipo de evento. Validar com testes de sub-orch/ContinueAsNew/eventos (não só atividade/timer). Os bugs foram registrados como testes `[Fact(Skip=...)]` com motivo, e o filtro de `orchestratorMessages` foi corrigido (repassar todas). Sub-completion/ContinueAsNew ainda exigem trabalho dedicado — itens separados no caminho para 1.0.0.

## [2026-07-30] Sub-completion routing fix + 2 sub-bugs restantes (parentInstanceId, resultado, ContinueAsNew)

**Context:** Item 20 — corrigir roteamento de mensagens inter-orquestração (sub-completion + ContinueAsNew).
**What was done / found:**
1. **`parentInstanceId` ausente no JSON**: o `lock_next_orchestration` montava o JSON de histórico e de new_events SEM `parentInstanceId`. Sem isso, `ExecutionStartedEvent.ParentInstance` ficava null → o runtime DTFx nunca criava o `SubOrchestrationInstanceCompletedEvent` (gating: `ParentInstance != null`). **Fix**: adicionar `'parentInstanceId', v_parent_instance_id` em AMBOS os build points (history linha ~324 e new_events linha ~270). Após o fix, o completion event passou a ser gerado e roteado ao parent (visível no histórico do parent).
2. **`continuedAsNewMessage` ignorado**: o parâmetro era declarado e nunca usado. **Fix (defensivo)**: roteá-lo a `orchestrationEvents`. MAS ContinueAsNew ainda não funciona — a detecção de continue-as-new no SQL compara o execution-id da instância com o passado, e o novo execution-id do `continuedAsNewMessage` não chega a essa comparação. Precisa tratamento dedicado.
3. **Resultado da sub não aplicado no replay**: após o parent receber `SubOrchestrationInstanceCompleted`, ele NÃO completa — o resultado (payload) da sub não está sendo aplicado durante o replay do parent. Em investigação.

**Key debugging insight:** para descobrir #1, log temporário em `GetHistoryEvent` mostrou o JSON cru — revelando que vinha de new_events (campos `dequeueCount`/`waitTimeSeconds`), não de history, e que NENHUM dos dois tinha `parentInstanceId`. Sem logar o payload cru, o sintoma (parent sempre null) parecia absurdo.
**Prevention:** ao debugar roteamento de mensagens DTFx, logar o JSON cru que chega aos materializadores (`GetHistoryEvent`/`GetTaskMessage`), não só o objeto C# resultante. Espelhar SEMPRE o MSSQL: todo campo que o SQL do MSSQL emite no result set (ex: `@parentInstanceID` em logic.sql:742) deve ter equivalente no PostgreSQL. ContinueAsNew e resultado-de-sub ainda são bugs abertos (testes skipados com motivo).

## [2026-07-30] ContinueAsNew corrigido; sub-orch isolado em task_id mismatch

**Context:** Continuação do item 20 — fechar ContinueAsNew e sub-orchestration.
**What was done / found:**
1. **ContinueAsNew corrigido** ✅. Três peças juntas: (a) rotear `continuedAsNewMessage` aos `orchestrationEvents` (o provider o ignorava completamente); (b) o orchestrator precisa de um **yield point** antes de `ContinueAsNew` (espelhar MSSQL: `await ctx.CreateTimer(...)` antes) — sem ele, completa na 1ª execução; (c) o `WaitForOrchestrationAsync` do teste deve buscar por **instance id com execution id null**, pois ContinueAsNew troca o execution id (buscar pelo execution id original dá timeout mesmo a instância já estando Completed no banco).
2. **Sub-orchestration: bug do `task_id` mismatch**. O `SubOrchestrationInstanceCompletedEvent` chega ao parent com `TaskScheduledId=-1`, enquanto o `SubOrchestrationInstanceCreatedEvent` tem `EventId=0` — não casam, então o runtime não resolve a task pendente e o parent nunca completa. A materialização é idêntica ao MSSQL (que também usa `InstanceId=""` placeholder), então o bug está em como o `task_id` é **gravado** para esses eventos (o completion é gravado com -1). Ainda em investigação.

**Key insight (ContinueAsNew):** o `WaitForOrchestrationAsync` filtrando por execution-id mascara o sucesso — a orquestração completava no banco (output correto) mas o wait dava timeout. Ao suspeitar de bug do provider, sempre **inspecionar o estado final no banco** (status + output) antes de concluir que falhou.
**Prevention:** ao testar ContinueAsNew, esperar por instance-id (não execution-id). Para sub-orchestration, o casamento created↔completed é por `task_id` — garantir que o completion carregue o `TaskScheduledId` do created correspondente.

## [2026-07-30] Sub-orchestration corrigido: persistir ParentInstance.TaskScheduleId no ExecutionStarted

**Context:** Último bloqueador de correção para 1.0.0 — sub-orchestration nunca completava.
**Root cause (definitiva, via leitura do código DTFx):** o runtime NÃO escaneia o histórico do parent para casar o completion. Ele lê `runtimeState.ParentInstance.TaskScheduleId` da **própria folha** (sub) e põe esse valor como `TaskScheduledId` do `SubOrchestrationInstanceCompletedEvent` (`TaskOrchestrationDispatcher.cs:1100`). Esse `ParentInstance.TaskScheduleId` vem do `task_id` da row `ExecutionStarted` da folha. O PostgreSQL gravava `task_id = -1` para ExecutionStarted porque `GetTaskEventId` caía no default `_ => evt.EventId` e `ExecutionStartedEvent.EventId` é **sempre -1** (DTFx despacha assim). Resultado: a folha lia `TaskScheduleId = -1`, o completion vinha com `-1`, não casava com nenhum created → parent travava.
**Fix (uma linha):** adicionar arm `EventType.ExecutionStarted => ((ExecutionStartedEvent)evt).ParentInstance?.TaskScheduleId ?? -1` em `GetTaskEventId`. Assim a row ExecutionStarted da folha armazena o schedule-id do parent, o runtime lê de volta, e o completion casa. O caminho de leitura (`PostgreSqlUtils.cs:102 TaskScheduleId = GetTaskId(reader)`) já estava correto.
**Beco sem saída evitado:** tentei popular `SubOrchestrationInstanceCreatedEvent.InstanceId` (placeholder `""`) — inútil, pois o runtime casa por `TaskScheduleId` carregado na folha, não por InstanceId do created. MSSQL também usa `InstanceId=""` placeholder; o valor real viaja pelo `TaskId` da row ExecutionStarted da folha (`SqlUtils.cs:129`).
**Prevention:** ao portar, cada evento DTFx tem semântica própria de "qual field é o id". `ExecutionStarted.EventId` é sempre -1 (não é um id útil); o `task_id` dessa row deve carregar `ParentInstance.TaskScheduleId`. Sempre ler o código do runtime DTFx (`TaskOrchestrationDispatcher`) para saber QUEM consome cada campo — não assumir pelo nome.

## [2026-07-30] Eventos externos + migrations runner (fechamento 1.0.0)

**Context:** Itens 5 e 2 do caminho 1.0.0.
**Done:**
1. **Eventos externos (Item 5)**: o provider JÁ suportava eventos externos (`SendTaskOrchestrationMessageAsync` → `add_orchestration_event` → replay de `EventRaisedEvent`). O teste skipado estava em premissa falsa — `WaitForExternalEvent<T>()` **não existe** em DTFx Core 3.7.1. O padrão canônico é `TaskOrchestration<TOutput,TInput>` + `TaskCompletionSource<T>` + override de `OnEvent`. Reescrito o teste — passou sem tocar no provider.
2. **Migrations runner (Item 2)**: baseline (`schema.postgresql.sql`) é idempotente (fresh-install); upgrades aplicam `Scripts/migrations/migration-{semver}.postgresql.sql` em ordem semver, cada um registrado em `dt.versions`. PostgreSQL tem `ADD COLUMN IF NOT EXISTS` nativo (mais simples que o boilerplate `IF NOT EXISTS` do MSSQL). Verificado com migration real (`migration-1.1.0` adiciona coluna) + teste de upgrade — idempotente em warm runs.

**Key insight (eventos externos):** não assumir APIs do DTFx por analogia com Azure Durable Functions. DTFx standalone é mais low-level; sempre checar a doc/code do core (`docs/features/external-events.md`). Antes de marcar algo como "limitação do provider", confirmar que não é "limitação do teste".
**Prevention:** para migrations, versionar o baseline E os deltas no mesmo esquema (`dt.versions`), parsear semver tolerando sufixos de pré-release (strip `-beta.1` → `1.0.0`), e tornar migrations idempotentes (DDL `IF NOT EXISTS`). Cada migration em sua própria transação + registro de versão.








