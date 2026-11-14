# Restricted runtime role

Deploy schema upgrades using the schema owner; run workers and clients with an
unrelated login that belongs to `dt_runtime`. Opt in during each deployment:

```csharp
var deploySettings = new PostgreSqlOrchestrationServiceSettings
{
    ConnectionString = ownerConnectionString,
    AutoDeploySchema = true,
    DeployRuntimePermissions = true,
};
using var deployer = new PostgreSqlOrchestrationService(deploySettings, logger);
await deployer.CreateAsync();
```

Deployment creates a cluster-wide `dt_runtime` role if absent (requires
`CREATEROLE` for that first deployment), then grants privileges within the
configured `SchemaName`. Existing installations can have a DBA provision the
restricted NOLOGIN role first. Deployment does not create application logins or
grant membership. A DBA must do that separately:

```sql
CREATE ROLE application_worker LOGIN PASSWORD '<set through your secret-management process>';
GRANT dt_runtime TO application_worker;
```

Configure workers and clients with the runtime login and `AutoDeploySchema = false`.
`DeployRuntimePermissions` defaults to false, so existing deployments do not
unexpectedly require role-management privileges. Keep it enabled on future
owner deployments: the provider recreates SQL functions and must reapply grants.
The embedded `Scripts/permissions.postgresql.sql` can also be run by a DBA after
schema deployment (replace the `dt` schema identifier for custom schemas).

The runtime role has schema usage, routine execution, DML on the five runtime
tables, sequence usage/read privileges, and read-only access to settings and
schema versions. It cannot create/drop provider objects or change configuration
or migration history. It is NOLOGIN and has no superuser, create-role,
create-database, replication, or row-security-bypass attributes. Existing
application users must not inherit extra privileges that undo these restrictions.

All provider functions use invoker security. `current_task_hub()` therefore
evaluates `CURRENT_USER` as the active runtime role for `TaskHubMode = '1'`,
including after upgrading a schema that used the former definer function.
Mode 0 continues to use the connection application name. The permissions script
revokes the default function EXECUTE grant from PUBLIC and grants it to
`dt_runtime`; other non-owner consumer roles must join that role or be granted
explicit execution privileges by their DBA.

This model separates runtime operation from schema administration. It does not
provide a security boundary between task hubs: the runtime role has direct
access to runtime tables, and `application_name` is controlled by the client.
Use separate databases/roles or a separately designed row-security/function
boundary for mutually untrusted tenants. Granting this cluster-wide role in
multiple schemas intentionally gives its members access to all those schemas.
