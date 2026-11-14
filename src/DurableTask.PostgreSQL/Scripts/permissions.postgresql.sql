-- Optional least-privilege runtime role. Execute as the schema owner after schema,
-- logic and migrations; the deployer needs CREATEROLE only when the role is absent.
-- This grants runtime DML, not isolation between task hubs (tables remain accessible).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dt_runtime') THEN
        BEGIN
            CREATE ROLE dt_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                NOREPLICATION NOBYPASSRLS;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            -- Roles are cluster-wide: another database/schema deploy can create it.
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dt_runtime') THEN
                RAISE;
            END IF;
        END;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dt_runtime'
        AND (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) THEN
        RAISE EXCEPTION 'Existing dt_runtime role must be a restricted NOLOGIN role';
    END IF;
END;
$$;

GRANT USAGE ON SCHEMA dt TO dt_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    dt.instances, dt.payloads, dt.history, dt.new_events, dt.new_tasks TO dt_runtime;
GRANT SELECT ON dt.global_settings, dt.versions TO dt_runtime;
GRANT USAGE, SELECT ON SEQUENCE
    dt.new_events_sequence_number_seq, dt.new_tasks_sequence_number_seq TO dt_runtime;

-- The provider's routines are invoker functions. Their callers still need the
-- table/sequence privileges above; no owner privileges are inherited through them.
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA dt FROM PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dt TO dt_runtime;
GRANT USAGE ON TYPE dt.orchestration_lock_result, dt.task_result, dt.message_id,
    dt.history_event, dt.orchestration_event, dt.task_event TO dt_runtime;
