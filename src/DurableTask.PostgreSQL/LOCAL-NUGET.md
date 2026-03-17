# Local NuGet Package Setup

## Overview

O projeto `DurableTask.PostgreSQL` está configurado para publicar automaticamente em um feed NuGet local durante o desenvolvimento. Isso permite que múltiplos projetos compartilhem a mesma versão enquanto ainda estamos evoluindo a biblioteca.

## Configuração Automática

### Build Automático
A cada build em **Release**, o pacote é automaticamente:
1. Gerado (`.nupkg`)
2. Copiado para `D:\Code\LocalPackage\`

```bash
# Ao executar:
dotnet build src/DurableTask.PostgreSQL -c Release

# Resultado:
📦 Package published to local NuGet feed: D:\Code\LocalPackage\DurableTask.PostgreSQL.1.0.0-alpha.nupkg
```

### Desabilitar (se necessário)
Para builds rápidos em Debug sem gerar pacote:
```bash
dotnet build src/DurableTask.PostgreSQL -c Debug
# Pacote NÃO é gerado em Debug mode
```

Ou desabilitar temporariamente:
```bash
dotnet build src/DurableTask.PostgreSQL -c Release /p:GeneratePackageOnBuild=false
```

---

## Como Usar em Outros Projetos

### 1. Adicionar Feed Local (One-time setup)

#### Opção A: Via NuGet.config (Recomendado)
Crie ou edite `nuget.config` na raiz do projeto que vai consumir:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
    <add key="LocalPackages" value="D:\Code\LocalPackage" />
  </packageSources>
</configuration>
```

#### Opção B: Via Comando (Global)
```bash
dotnet nuget add source D:\Code\LocalPackage --name LocalPackages
```

### 2. Instalar o Pacote

```bash
# No projeto que vai usar DurableTask.PostgreSQL
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha
```

### 3. Atualizar para Nova Versão

Após atualizar o código em `DurableTask.PostgreSQL`:

```bash
# 1. Build para gerar novo pacote
cd D:\Code\Janus
dotnet build src/DurableTask.PostgreSQL -c Release

# 2. No projeto consumidor, limpar cache e reinstalar
dotnet nuget locals all --clear
dotnet restore --force
```

---

## Versionamento durante Desenvolvimento

### Versão Atual
```xml
<Version>1.0.0-alpha</Version>
```

### Mudando a Versão
Edite `DurableTask.PostgreSQL.csproj`:

```xml
<!-- Para nova feature -->
<Version>1.1.0-alpha</Version>

<!-- Para release candidate -->
<Version>1.0.0-rc1</Version>

<!-- Para versão estável -->
<Version>1.0.0</Version>
```

### Estratégia Recomendada
Durante desenvolvimento ativo:
- Use sufixo `-alpha` (ex: `1.0.0-alpha`, `1.1.0-alpha`)
- NuGet trata versões com sufixo como **pré-release**
- Permite instalar sem `--prerelease` flag

```bash
# Funciona com -alpha suffix
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha
```

---

## Estrutura de Diretórios

```
D:\Code\
├── LocalPackage\                              # Feed NuGet local
│   └── DurableTask.PostgreSQL.1.0.0-alpha.nupkg
│
├── Janus\                                     # Projeto produtor
│   └── src\
│       └── DurableTask.PostgreSQL\
│           ├── DurableTask.PostgreSQL.csproj  # GeneratePackageOnBuild=true
│           └── ...
│
└── OutroProjeto\                              # Projeto consumidor
    ├── nuget.config                           # Aponta para D:\Code\LocalPackage
    └── OutroProjeto.csproj
        └── <PackageReference Include="DurableTask.PostgreSQL" Version="1.0.0-alpha" />
```

---

## Troubleshooting

### Pacote não encontrado
```bash
# Verificar se o pacote existe
ls D:/Code/LocalPackage/*.nupkg

# Verificar fontes configuradas
dotnet nuget list source

# Limpar cache e forçar restore
dotnet nuget locals all --clear
dotnet restore --force
```

### Versão antiga sendo usada
```bash
# Limpar cache de pacotes
dotnet nuget locals all --clear

# No projeto consumidor
dotnet clean
dotnet restore --no-cache
dotnet build
```

### Erro "NU1101: Unable to find package"
Certifique-se que:
1. O feed `D:\Code\LocalPackage` está configurado em `nuget.config`
2. O pacote existe: `ls D:/Code/LocalPackage/DurableTask.PostgreSQL.*.nupkg`
3. A versão solicitada corresponde à gerada

---

## Exemplo Completo: Novo Projeto

```bash
# 1. Criar novo projeto
mkdir D:/Code/MeuProjeto
cd D:/Code/MeuProjeto
dotnet new console

# 2. Configurar feed local
cat > nuget.config << EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="LocalPackages" value="D:\Code\LocalPackage" />
  </packageSources>
</configuration>
EOF

# 3. Instalar DurableTask.PostgreSQL
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha

# 4. Usar no código
cat > Program.cs << 'EOF'
using DurableTask.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddDurableTaskPostgreSql(
    connectionString: "Host=localhost;Database=mydb;...",
    configure: settings =>
    {
        settings.TaskHubName = "MyHub";
    });

Console.WriteLine("✅ DurableTask.PostgreSQL configured!");
EOF

# 5. Build e executar
dotnet build
dotnet run
```

---

## Migração para NuGet Privado (Futuro)

Quando configurar feed NuGet privado da empresa:

### 1. Desabilitar publicação local
Em `DurableTask.PostgreSQL.csproj`:
```xml
<GeneratePackageOnBuild>false</GeneratePackageOnBuild>
<!-- Remover custom target PublishToLocalNuGet -->
```

### 2. Configurar CI/CD
```yaml
# Azure DevOps / GitHub Actions
- task: DotNetCoreCLI@2
  inputs:
    command: 'pack'
    projects: 'src/DurableTask.PostgreSQL/*.csproj'
    versioningScheme: 'byEnvVar'
    versionEnvVar: 'PackageVersion'

- task: NuGetCommand@2
  inputs:
    command: 'push'
    packagesToPush: '**/*.nupkg'
    nuGetFeedType: 'internal'
    publishVstsFeed: 'MyCompany/DurableTask'
```

### 3. Atualizar projetos consumidores
```xml
<!-- nuget.config -->
<packageSources>
  <clear />
  <add key="CompanyFeed" value="https://pkgs.dev.azure.com/company/_packaging/feed/nuget/v3/index.json" />
  <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
</packageSources>
```

---

## Benefícios da Abordagem Local

✅ **Desenvolvimento rápido**: Build automático gera pacote
✅ **Múltiplos projetos**: Janus + outros podem usar mesma versão
✅ **Sem infraestrutura**: Não precisa configurar servidor NuGet ainda
✅ **Iteração rápida**: Rebuild → Restore → Test
✅ **Transição suave**: Fácil migrar para feed privado depois

---

## Comandos Úteis

```bash
# Rebuild e republicar pacote
dotnet build src/DurableTask.PostgreSQL -c Release --no-incremental

# Verificar versão instalada em projeto
dotnet list package | grep DurableTask

# Forçar reinstalação em projeto consumidor
dotnet remove package DurableTask.PostgreSQL
dotnet add package DurableTask.PostgreSQL --version 1.0.0-alpha

# Listar todos os pacotes no feed local
ls -lh D:/Code/LocalPackage/*.nupkg
```

---

**Configurado em:** 2026-02-15
**Feed Local:** `D:\Code\LocalPackage`
**Versão Atual:** `1.0.0-alpha`
**Status:** ✅ Automático em builds Release
