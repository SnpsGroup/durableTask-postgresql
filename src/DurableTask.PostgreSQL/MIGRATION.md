# Migration from Janus.Infrastructure to Standalone Project

## Overview

DurableTask.PostgreSQL was originally part of `Janus.Infrastructure` as an embedded component in the `Workflows/DurableTask.PostgreSQL` folder. It has now been extracted into a **standalone project** that can be shared across the company via NuGet package.

## Changes Made

### 1. Project Structure
**Before:**
```
src/Janus.Infrastructure/
  └── Workflows/
      └── DurableTask.PostgreSQL/
          ├── PostgreSqlOrchestrationService.cs
          ├── PostgreSqlOrchestrationServiceSettings.cs
          ├── PostgreSqlTypes.cs
          ├── ServiceCollectionExtensions.cs
          ├── README.md
          ├── INTEGRATION.md
          └── Scripts/
              ├── schema.postgresql.sql
              └── logic.postgresql.sql
```

**After:**
```
src/DurableTask.PostgreSQL/           # New standalone project
  ├── DurableTask.PostgreSQL.csproj    # New project file
  ├── PostgreSqlOrchestrationService.cs
  ├── PostgreSqlOrchestrationServiceSettings.cs
  ├── PostgreSqlTypes.cs
  ├── ServiceCollectionExtensions.cs
  ├── README.md
  ├── INTEGRATION.md
  ├── MIGRATION.md                     # This file
  ├── build-package.ps1                # NuGet packaging script
  └── Scripts/
      ├── schema.postgresql.sql
      └── logic.postgresql.sql

src/Janus.Infrastructure/
  ├── Janus.Infrastructure.csproj      # Updated to reference DurableTask.PostgreSQL
  └── Workflows/                       # DurableTask.PostgreSQL removed
```

### 2. Namespace Changes
**Before:**
```csharp
namespace Janus.Infrastructure.Workflows.DurableTask.PostgreSQL;
```

**After:**
```csharp
namespace DurableTask.PostgreSQL;
```

### 3. Project References
**Janus.Infrastructure.csproj** now includes:
```xml
<ItemGroup>
  <ProjectReference Include="..\DurableTask.PostgreSQL\DurableTask.PostgreSQL.csproj" />
</ItemGroup>
```

**DurableTask.PostgreSQL.csproj** dependencies:
```xml
<ItemGroup>
  <PackageReference Include="Microsoft.Azure.DurableTask.Core" />
  <PackageReference Include="Npgsql" />
  <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
  <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions" />
  <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
</ItemGroup>
```

### 4. Using Directives
**Before:**
```csharp
using Janus.Infrastructure.Workflows.DurableTask.PostgreSQL;
```

**After:**
```csharp
using DurableTask.PostgreSQL;
```

---

## Migration Guide for Existing Code

### For Janus Project
No code changes required! The project reference automatically makes the types available with the new namespace.

**Updated files:**
- `src/Janus.Infrastructure/DependencyInjection/DurableTaskExtensions.cs` - using directive updated

### For Other Projects (Company-wide)

1. **Install the package:**
   ```bash
   dotnet add package DurableTask.PostgreSQL --version 1.0.0
   ```

2. **Update using directives:**
   ```diff
   - using Janus.Infrastructure.Workflows.DurableTask.PostgreSQL;
   + using DurableTask.PostgreSQL;
   ```

3. **Register services (no changes needed):**
   ```csharp
   services.AddDurableTaskPostgreSql(
       connectionString: "Host=localhost;Database=mydb;...",
       configure: settings =>
       {
           settings.TaskHubName = "MyHub";
           settings.MaxConcurrentOrchestrations = 10;
       });
   ```

---

## Building and Publishing the Package

### Local Development
```bash
# Build the project
dotnet build src/DurableTask.PostgreSQL/DurableTask.PostgreSQL.csproj

# Pack as NuGet
cd src/DurableTask.PostgreSQL
.\build-package.ps1 -Version "1.0.0-alpha"

# Package will be created at:
# artifacts/packages/DurableTask.PostgreSQL.1.0.0-alpha.nupkg
```

### Publishing to Private NuGet Feed
```bash
# Push to company NuGet server
.\build-package.ps1 -Version "1.0.0" -Push -NuGetSource "https://nuget.company.com/v3/index.json"
```

### Publishing to NuGet.org (if open-sourced)
```bash
dotnet nuget push artifacts/packages/DurableTask.PostgreSQL.1.0.0.nupkg \
  --source https://api.nuget.org/v3/index.json \
  --api-key YOUR_API_KEY
```

---

## Benefits of Separation

### 1. **Reusability**
- Can be used across multiple projects in the company
- Not tied to Janus-specific infrastructure

### 2. **Versioning**
- Independent versioning from Janus
- Semantic versioning (e.g., 1.0.0, 1.1.0, 2.0.0)
- Stable releases independent of Janus sprints

### 3. **Testing**
- Easier to write unit/integration tests
- Test harness independent of Janus

### 4. **Open Source Potential**
- Can be published to NuGet.org
- Community contributions possible
- Benefit from external testing and improvements

### 5. **Clean Dependencies**
- Only depends on DurableTask.Core and Npgsql
- No coupling to Janus domain/application layers

---

## Version History

### v1.0.0-alpha (2026-02-15)
- **Separated from Janus.Infrastructure**
- Namespace changed to `DurableTask.PostgreSQL`
- Added NuGet packaging support
- Updated documentation

### v0.2.0-alpha (2026-02-14)
- Core operations implemented
- Integration with Janus.Infrastructure

### v0.1.0-poc (2026-01-20)
- Initial proof of concept
- Basic schema and procedures

---

## Rollback Plan (if needed)

If issues arise, you can temporarily revert to the embedded version:

1. **Copy files back:**
   ```bash
   cp -r src/DurableTask.PostgreSQL/* src/Janus.Infrastructure/Workflows/DurableTask.PostgreSQL/
   ```

2. **Revert namespace:**
   ```diff
   - namespace DurableTask.PostgreSQL;
   + namespace Janus.Infrastructure.Workflows.DurableTask.PostgreSQL;
   ```

3. **Remove project reference:**
   ```diff
   - <ProjectReference Include="..\DurableTask.PostgreSQL\DurableTask.PostgreSQL.csproj" />
   ```

4. **Revert using directives:**
   ```diff
   - using DurableTask.PostgreSQL;
   + using Janus.Infrastructure.Workflows.DurableTask.PostgreSQL;
   ```

---

## Support

For questions or issues:
- **Internal:** Contact Janus SGA Team
- **External (if open-sourced):** Open issue on GitHub

---

**Migration Date:** 2026-02-15
**Migrated By:** Janus Development Team
**Status:** ✅ Complete and Verified
