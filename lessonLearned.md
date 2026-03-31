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
