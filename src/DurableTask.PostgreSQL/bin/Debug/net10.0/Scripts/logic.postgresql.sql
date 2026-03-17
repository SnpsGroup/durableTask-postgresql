-- Copyright (c) Janus SGA Contributors.
-- Licensed under the MIT License.
--
-- DurableTask.PostgreSQL Logic (Stored Procedures/Functions)
-- PostgreSQL port of DurableTask.SqlServer logic.sql
--
-- Target: PostgreSQL 17+
-- Version: 0.1.0-poc

-- =============================================================================
-- CORE LIFECYCLE OPERATIONS
-- =============================================================================

-- Function: dt.create_instance
-- Purpose: Create new orchestration instance with deduplication support
-- Parameters:
--   p_name: Orchestration type name
--   p_version: Optional version string
--   p_instance_id: Optional user-provided instance ID (auto-generated if NULL)
--   p_execution_id: Optional execution ID (auto-generated if NULL)
--   p_input_text: Optional input payload as JSONB text
--   p_start_time: Optional scheduled start time (NULL = immediate)
--   p_dedupe_statuses: Comma-separated list of statuses that prevent duplicate creation
--   p_trace_context: Optional distributed tracing context
-- Returns: Created instance ID
-- Complexity: Medium (deduplication logic, collision detection)
CREATE OR REPLACE FUNCTION dt.create_instance(
    p_name VARCHAR(300),
    p_version VARCHAR(100) DEFAULT NULL,
    p_instance_id VARCHAR(100) DEFAULT NULL,
    p_execution_id VARCHAR(50) DEFAULT NULL,
    p_input_text TEXT DEFAULT NULL,
    p_start_time TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_dedupe_statuses TEXT DEFAULT 'Pending,Running',
    p_trace_context VARCHAR(800) DEFAULT NULL
)
RETURNS VARCHAR(100)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_instance_id VARCHAR(100);
    v_execution_id VARCHAR(50);
    v_existing_status VARCHAR(30);
    v_input_payload_id UUID;
    v_dedupe_list TEXT[];
    v_event_type VARCHAR(30) := 'ExecutionStarted';
    v_runtime_status VARCHAR(30) := 'Pending';
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Generate IDs if not provided
    v_instance_id := COALESCE(p_instance_id, gen_random_uuid()::TEXT);
    v_execution_id := COALESCE(p_execution_id, gen_random_uuid()::TEXT);

    -- Parse deduplication statuses
    v_dedupe_list := string_to_array(p_dedupe_statuses, ',');

    -- Check for instance ID collision (only if user provided ID)
    IF p_instance_id IS NOT NULL THEN
        -- Lock row to prevent race conditions
        SELECT runtime_status INTO v_existing_status
        FROM dt.instances
        WHERE task_hub = v_task_hub
          AND instance_id = v_instance_id
        FOR UPDATE;

        -- Reject if active instance exists with same ID
        IF v_existing_status = ANY(v_dedupe_list) THEN
            RAISE EXCEPTION 'Cannot create instance with ID ''%'' because a % instance already exists',
                v_instance_id, v_existing_status
                USING ERRCODE = 'unique_violation';
        END IF;

        -- Purge completed instance if exists (allow reuse of ID)
        IF v_existing_status IS NOT NULL THEN
            -- Delete in order to avoid FK violations (even though we don't have FKs for perf)
            DELETE FROM dt.payloads WHERE task_hub = v_task_hub AND instance_id = v_instance_id;
            DELETE FROM dt.history WHERE task_hub = v_task_hub AND instance_id = v_instance_id;
            DELETE FROM dt.new_events WHERE task_hub = v_task_hub AND instance_id = v_instance_id;
            DELETE FROM dt.new_tasks WHERE task_hub = v_task_hub AND instance_id = v_instance_id;
            DELETE FROM dt.instances WHERE task_hub = v_task_hub AND instance_id = v_instance_id;
        END IF;
    END IF;

    -- *** IMPORTANT ***
    -- Maintain consistent table access order to prevent deadlocks
    -- Table order: Payloads → Instances → NewEvents

    -- Insert payload if provided
    IF p_input_text IS NOT NULL THEN
        v_input_payload_id := gen_random_uuid();
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text)
        VALUES (v_task_hub, v_instance_id, v_input_payload_id, p_input_text::JSONB);
    END IF;

    -- Insert instance
    INSERT INTO dt.instances (
        task_hub,
        instance_id,
        execution_id,
        name,
        version,
        runtime_status,
        input_payload_id,
        trace_context,
        created_time,
        last_updated_time
    ) VALUES (
        v_task_hub,
        v_instance_id,
        v_execution_id,
        p_name,
        p_version,
        v_runtime_status,
        v_input_payload_id,
        p_trace_context,
        NOW(),
        NULL  -- Only set on updates, not creation
    );

    -- Insert initial ExecutionStarted event
    INSERT INTO dt.new_events (
        task_hub,
        instance_id,
        execution_id,
        event_type,
        name,
        runtime_status,
        visible_time,
        payload_id,
        trace_context
    ) VALUES (
        v_task_hub,
        v_instance_id,
        v_execution_id,
        v_event_type,
        p_name,
        v_runtime_status,
        p_start_time,  -- NULL = immediate, non-NULL = scheduled
        v_input_payload_id,
        p_trace_context
    );

    RETURN v_instance_id;
END;
$$;

COMMENT ON FUNCTION dt.create_instance IS 'Create new orchestration instance with collision detection and deduplication';

-- =============================================================================
-- WORK ITEM MANAGEMENT (CRITICAL FOR CONCURRENCY)
-- =============================================================================

-- Type: dt.orchestration_lock_result
-- Purpose: Return type for lock_next_orchestration (composite type for multiple result sets)
DROP TYPE IF EXISTS dt.orchestration_lock_result CASCADE;
CREATE TYPE dt.orchestration_lock_result AS (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    runtime_status VARCHAR(30),
    parent_instance_id VARCHAR(100),
    version VARCHAR(100),
    new_events JSONB,
    history JSONB
);

-- Function: dt.lock_next_orchestration
-- Purpose: Dequeue next orchestration work item (CRITICAL for dispatcher)
-- Parameters:
--   p_batch_size: Max number of events to return
--   p_locked_by: Worker identifier
--   p_lock_expiration: Lock lease expiration time
-- Returns: Composite result with instance metadata, new events, and history
-- Complexity: HIGH (3 result sets, complex locking, SKIP LOCKED critical)
CREATE OR REPLACE FUNCTION dt.lock_next_orchestration(
    p_batch_size INTEGER,
    p_locked_by VARCHAR(100),
    p_lock_expiration TIMESTAMP WITH TIME ZONE
)
RETURNS SETOF dt.orchestration_lock_result
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_now TIMESTAMP WITH TIME ZONE;
    v_instance_id VARCHAR(100);
    v_execution_id VARCHAR(50);
    v_runtime_status VARCHAR(30);
    v_parent_instance_id VARCHAR(100);
    v_version VARCHAR(100);
    v_result dt.orchestration_lock_result;
BEGIN
    v_task_hub := dt.current_task_hub();
    v_now := NOW();

    -- *** IMPORTANT ***
    -- Maintain consistent table access order to prevent deadlocks
    -- Table order: Instances → NewEvents → Payloads → History

    -- Lock first active instance with pending events
    -- SKIP LOCKED is CRITICAL for high-concurrency scenarios
    WITH candidate AS (
        SELECT i.instance_id, i.execution_id, i.runtime_status, i.parent_instance_id, i.version
        FROM dt.instances i
        INNER JOIN dt.new_events e ON
            e.task_hub = v_task_hub AND
            e.instance_id = i.instance_id
        WHERE i.task_hub = v_task_hub
          AND (i.lock_expiration IS NULL OR i.lock_expiration < v_now)
          AND (e.visible_time IS NULL OR e.visible_time < v_now)
        ORDER BY e.sequence_number  -- FIFO
        LIMIT 1
        FOR UPDATE OF i SKIP LOCKED
    )
    UPDATE dt.instances i
    SET locked_by = p_locked_by,
        lock_expiration = p_lock_expiration
    FROM candidate
    WHERE i.task_hub = v_task_hub
      AND i.instance_id = candidate.instance_id
    RETURNING i.instance_id, i.execution_id, i.runtime_status, i.parent_instance_id, i.version
    INTO v_instance_id, v_execution_id, v_runtime_status, v_parent_instance_id, v_version;

    -- Return NOTHING if no work available
    IF v_instance_id IS NULL THEN
        RETURN;
    END IF;

    -- Collect new events (up to batch size)
    v_result.new_events := (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'sequenceNumber', n.sequence_number,
                'timestamp', n."timestamp",
                'visibleTime', n.visible_time,
                'dequeueCount', n.dequeue_count,
                'instanceId', n.instance_id,
                'executionId', n.execution_id,
                'eventType', n.event_type,
                'name', n.name,
                'runtimeStatus', n.runtime_status,
                'taskId', n.task_id,
                'reason', p.reason,
                'payloadText', p.text,
                'payloadId', n.payload_id,
                'waitTimeSeconds', EXTRACT(EPOCH FROM (v_now - n."timestamp"))::INTEGER,
                'traceContext', n.trace_context
            ) ORDER BY n.sequence_number
        ), '[]'::JSONB)
        FROM (
            SELECT *
            FROM dt.new_events n
            WHERE n.task_hub = v_task_hub
              AND n.instance_id = v_instance_id
              AND (n.visible_time IS NULL OR n.visible_time < v_now)
            ORDER BY n.sequence_number
            LIMIT p_batch_size
        ) n
        LEFT JOIN dt.payloads p ON
            p.task_hub = v_task_hub AND
            p.instance_id = n.instance_id AND
            p.payload_id = n.payload_id
    );

    -- Bail if no events returned (race condition - another worker took them)
    IF jsonb_array_length(v_result.new_events) = 0 THEN
        -- Unlock instance
        UPDATE dt.instances
        SET locked_by = NULL,
            lock_expiration = NULL
        WHERE task_hub = v_task_hub
          AND instance_id = v_instance_id;
        RETURN;
    END IF;

    -- Collect full history for the instance
    v_result.history := (
        SELECT COALESCE(jsonb_agg(hist_obj ORDER BY seq_num), '[]'::JSONB)
        FROM (
            SELECT
                h.sequence_number AS seq_num,
                jsonb_build_object(
                    'instanceId', h.instance_id,
                    'executionId', h.execution_id,
                    'sequenceNumber', h.sequence_number,
                    'eventType', h.event_type,
                    'name', h.name,
                    'runtimeStatus', h.runtime_status,
                    'taskId', h.task_id,
                    'timestamp', h."timestamp",
                    'isPlayed', h.is_played,
                    'visibleTime', h.visible_time,
                    'reason', p.reason,
                    -- Optimization: skip payload for TaskScheduled (never replayed)
                    'payloadText', CASE
                        WHEN h.event_type IN ('TaskScheduled', 'SubOrchestrationInstanceCreated')
                        THEN NULL
                        ELSE p.text
                    END,
                    'payloadId', h.data_payload_id,
                    'traceContext', h.trace_context
                ) AS hist_obj
            FROM dt.history h
            LEFT JOIN dt.payloads p ON
                p.task_hub = v_task_hub AND
                p.instance_id = h.instance_id AND
                p.payload_id = h.data_payload_id
            WHERE h.task_hub = v_task_hub
              AND h.instance_id = v_instance_id
            ORDER BY h.sequence_number
        ) ordered_history
    );

    -- Populate result
    v_result.instance_id := v_instance_id;
    v_result.execution_id := v_execution_id;
    v_result.runtime_status := v_runtime_status;
    v_result.parent_instance_id := v_parent_instance_id;
    v_result.version := v_version;
    v_result.history := COALESCE(v_result.history, '[]'::JSONB);

    RETURN NEXT v_result;
END;
$$;

COMMENT ON FUNCTION dt.lock_next_orchestration IS 'Dequeue next orchestration work item with SKIP LOCKED for concurrency';

-- Function: dt.lock_next_task
-- Purpose: Dequeue next activity task work item (CRITICAL for activity workers)
-- Parameters:
--   p_locked_by: Worker identifier
--   p_lock_expiration: Lock lease expiration time
-- Returns: Task details if locked, NULL if no work available
-- Complexity: HIGH (SKIP LOCKED critical, dequeue counter)
CREATE OR REPLACE FUNCTION dt.lock_next_task(
    p_locked_by VARCHAR(100),
    p_lock_expiration TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
    sequence_number BIGINT,
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    event_type VARCHAR(40),
    task_id INTEGER,
    visible_time TIMESTAMP WITH TIME ZONE,
    "timestamp" TIMESTAMP WITH TIME ZONE,
    dequeue_count INTEGER,
    version VARCHAR(100),
    payload_text JSONB,
    wait_time_seconds INTEGER,
    trace_context VARCHAR(800)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_now TIMESTAMP WITH TIME ZONE;
    v_sequence_number BIGINT;
BEGIN
    v_task_hub := dt.current_task_hub();
    v_now := NOW();

    -- *** IMPORTANT ***
    -- Maintain consistent table access order to prevent deadlocks
    -- Table order: NewTasks → Payloads

    -- Lock next available task (SKIP LOCKED for concurrency)
    WITH candidate AS (
        SELECT t.sequence_number
        FROM dt.new_tasks t
        WHERE t.task_hub = v_task_hub
          AND (t.lock_expiration IS NULL OR t.lock_expiration < v_now)
          AND (t.visible_time IS NULL OR t.visible_time < v_now)
        ORDER BY t.sequence_number  -- FIFO order
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    UPDATE dt.new_tasks t
    SET locked_by = p_locked_by,
        lock_expiration = p_lock_expiration,
        dequeue_count = t.dequeue_count + 1
    FROM candidate
    WHERE t.task_hub = v_task_hub
      AND t.sequence_number = candidate.sequence_number
    RETURNING t.sequence_number INTO v_sequence_number;

    -- Return NULL if no work available
    IF v_sequence_number IS NULL THEN
        RETURN;
    END IF;

    -- Return task details
    RETURN QUERY
    SELECT
        t.sequence_number,
        t.instance_id,
        t.execution_id,
        t.name,
        'TaskScheduled'::VARCHAR(40) AS event_type,
        t.task_id,
        t.visible_time,
        t."timestamp",
        t.dequeue_count,
        t.version,
        p.text AS payload_text,
        EXTRACT(EPOCH FROM (v_now - t."timestamp"))::INTEGER AS wait_time_seconds,
        t.trace_context
    FROM dt.new_tasks t
    LEFT JOIN dt.payloads p ON
        p.task_hub = v_task_hub AND
        p.instance_id = t.instance_id AND
        p.payload_id = t.payload_id
    WHERE t.task_hub = v_task_hub
      AND t.sequence_number = v_sequence_number;
END;
$$;

COMMENT ON FUNCTION dt.lock_next_task IS 'Dequeue next activity task with SKIP LOCKED for concurrency';

-- =============================================================================
-- HELPER FUNCTIONS (FOR POC)
-- =============================================================================

-- Function: dt.query_single_orchestration
-- Purpose: Get single instance state with payloads
-- Parameters:
--   p_instance_id: Instance to query
-- Returns: Instance details with denormalized payloads
CREATE OR REPLACE FUNCTION dt.query_single_orchestration(
    p_instance_id VARCHAR(100)
)
RETURNS TABLE (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    version VARCHAR(100),
    created_time TIMESTAMP WITH TIME ZONE,
    last_updated_time TIMESTAMP WITH TIME ZONE,
    completed_time TIMESTAMP WITH TIME ZONE,
    runtime_status VARCHAR(20),
    input_text JSONB,
    output_text JSONB,
    custom_status_text JSONB,
    trace_context VARCHAR(800)
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
BEGIN
    v_task_hub := dt.current_task_hub();

    RETURN QUERY
    SELECT
        i.instance_id,
        i.execution_id,
        i.name,
        i.version,
        i.created_time,
        i.last_updated_time,
        i.completed_time,
        i.runtime_status,
        p_input.text AS input_text,
        p_output.text AS output_text,
        p_custom.text AS custom_status_text,
        i.trace_context
    FROM dt.instances i
    LEFT JOIN dt.payloads p_input ON
        p_input.task_hub = v_task_hub AND
        p_input.instance_id = i.instance_id AND
        p_input.payload_id = i.input_payload_id
    LEFT JOIN dt.payloads p_output ON
        p_output.task_hub = v_task_hub AND
        p_output.instance_id = i.instance_id AND
        p_output.payload_id = i.output_payload_id
    LEFT JOIN dt.payloads p_custom ON
        p_custom.task_hub = v_task_hub AND
        p_custom.instance_id = i.instance_id AND
        p_custom.payload_id = i.custom_status_payload_id
    WHERE i.task_hub = v_task_hub
      AND i.instance_id = p_instance_id;
END;
$$;

COMMENT ON FUNCTION dt.query_single_orchestration IS 'Query single instance with denormalized payloads';

-- =============================================================================
-- LOCK RENEWAL (HEARTBEAT)
-- =============================================================================

-- Function: dt.renew_orchestration_locks
-- Purpose: Extend lock lease for orchestration (heartbeat)
-- Parameters:
--   p_instance_id: Instance to renew
--   p_locked_by: Worker identifier (must match the current lock owner)
--   p_lock_expiration: New expiration time
-- Returns: Number of rows updated (1 if successful, 0 if not found/expired/not owner)
CREATE OR REPLACE FUNCTION dt.renew_orchestration_locks(
    p_instance_id VARCHAR(100),
    p_locked_by VARCHAR(100),
    p_lock_expiration TIMESTAMP WITH TIME ZONE
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_rows_affected INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    UPDATE dt.instances
    SET lock_expiration = p_lock_expiration
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id
      AND locked_by = p_locked_by;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RETURN v_rows_affected;
END;
$$;

COMMENT ON FUNCTION dt.renew_orchestration_locks IS 'Renew orchestration lock lease (heartbeat)';

-- Function: dt.renew_task_locks
-- Purpose: Extend lock lease for multiple tasks (heartbeat)
-- Parameters:
--   p_sequence_numbers: Array of task sequence numbers to renew
--   p_lock_expiration: New expiration time
-- Returns: Number of rows updated
CREATE OR REPLACE FUNCTION dt.renew_task_locks(
    p_sequence_numbers BIGINT[],
    p_lock_expiration TIMESTAMP WITH TIME ZONE
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_rows_affected INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    UPDATE dt.new_tasks
    SET lock_expiration = p_lock_expiration
    WHERE task_hub = v_task_hub
      AND sequence_number = ANY(p_sequence_numbers);

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RETURN v_rows_affected;
END;
$$;

COMMENT ON FUNCTION dt.renew_task_locks IS 'Renew task lock leases in batch (heartbeat)';

-- =============================================================================
-- TASK COMPLETION
-- =============================================================================

-- Type: dt.task_result
-- Purpose: Represent completed task result for bulk operations
DROP TYPE IF EXISTS dt.task_result CASCADE;
CREATE TYPE dt.task_result AS (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    event_type VARCHAR(40),
    task_id INTEGER,
    visible_time TIMESTAMP WITH TIME ZONE,
    payload_text JSONB,
    payload_id UUID,
    reason TEXT,
    trace_context VARCHAR(800)
);

-- Function: dt.complete_tasks
-- Purpose: Mark activity tasks as completed and insert results as new events
-- Parameters:
--   p_completed_sequence_numbers: Array of sequence numbers to delete
--   p_results: Array of task results to insert as events
-- Returns: Array of deleted sequence numbers (for verification)
-- Complexity: HIGH (multi-step transaction with validation)
CREATE OR REPLACE FUNCTION dt.complete_tasks(
    p_completed_sequence_numbers BIGINT[],
    p_results dt.task_result[]
)
RETURNS BIGINT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_existing_instance_id VARCHAR(100);
    v_deleted_sequence_numbers BIGINT[];
    v_expected_count INTEGER;
    v_actual_count INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();
    v_expected_count := array_length(p_completed_sequence_numbers, 1);

    -- Ensure instance exists and is running before handling task results
    -- Hold lock to avoid race conditions
    SELECT DISTINCT r.instance_id INTO v_existing_instance_id
    FROM unnest(p_results) AS r
    INNER JOIN dt.instances i ON
        i.task_hub = v_task_hub AND
        i.instance_id = r.instance_id AND
        i.execution_id = r.execution_id AND
        i.runtime_status IN ('Running', 'Suspended')
    FOR UPDATE OF i;

    -- If instance found, save results to new_events
    IF v_existing_instance_id IS NOT NULL THEN
        -- Insert payloads first
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text, reason)
        SELECT v_task_hub, r.instance_id, r.payload_id, r.payload_text, r.reason
        FROM unnest(p_results) AS r
        WHERE r.payload_id IS NOT NULL;

        -- Insert new events
        INSERT INTO dt.new_events (
            task_hub, instance_id, execution_id, name, event_type,
            task_id, visible_time, payload_id, trace_context
        )
        SELECT
            v_task_hub, r.instance_id, r.execution_id, r.name, r.event_type,
            r.task_id, r.visible_time, r.payload_id, r.trace_context
        FROM unnest(p_results) AS r;
    END IF;

    -- Delete completed tasks and collect deleted sequence numbers
    WITH deleted AS (
        DELETE FROM dt.new_tasks
        WHERE task_hub = v_task_hub
          AND sequence_number = ANY(p_completed_sequence_numbers)
        RETURNING sequence_number
    )
    SELECT array_agg(sequence_number) INTO v_deleted_sequence_numbers
    FROM deleted;

    -- Verify all tasks were deleted (race condition check)
    GET DIAGNOSTICS v_actual_count = ROW_COUNT;
    IF v_actual_count <> v_expected_count THEN
        RAISE EXCEPTION 'Failed to delete all completed task events. Expected %, got %. Tasks may have been deleted by another worker.',
            v_expected_count, v_actual_count
            USING ERRCODE = '40001';  -- serialization_failure for retry
    END IF;

    RETURN v_deleted_sequence_numbers;
END;
$$;

COMMENT ON FUNCTION dt.complete_tasks IS 'Complete activity tasks and insert results as orchestration events';

-- =============================================================================
-- CHECKPOINT ORCHESTRATION (MOST CRITICAL & COMPLEX)
-- =============================================================================

-- Type: dt.message_id
-- Purpose: Identify messages to delete (instance_id + sequence_number)
DROP TYPE IF EXISTS dt.message_id CASCADE;
CREATE TYPE dt.message_id AS (
    instance_id VARCHAR(100),
    sequence_number BIGINT
);

-- Type: dt.history_event
-- Purpose: History event for bulk insert
DROP TYPE IF EXISTS dt.history_event CASCADE;
CREATE TYPE dt.history_event AS (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    sequence_number BIGINT,
    event_type VARCHAR(40),
    name VARCHAR(300),
    runtime_status VARCHAR(30),
    task_id INTEGER,
    "timestamp" TIMESTAMP WITH TIME ZONE,
    is_played BOOLEAN,
    visible_time TIMESTAMP WITH TIME ZONE,
    reason TEXT,
    payload_text JSONB,
    payload_id UUID,
    parent_instance_id VARCHAR(100),
    version VARCHAR(100),
    trace_context VARCHAR(800)
);

-- Type: dt.orchestration_event
-- Purpose: New orchestration events to insert
DROP TYPE IF EXISTS dt.orchestration_event CASCADE;
CREATE TYPE dt.orchestration_event AS (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    event_type VARCHAR(40),
    name VARCHAR(300),
    runtime_status VARCHAR(30),
    task_id INTEGER,
    visible_time TIMESTAMP WITH TIME ZONE,
    reason TEXT,
    payload_text JSONB,
    payload_id UUID,
    parent_instance_id VARCHAR(100),
    version VARCHAR(100),
    trace_context VARCHAR(800)
);

-- Type: dt.task_event
-- Purpose: New task events to schedule
DROP TYPE IF EXISTS dt.task_event CASCADE;
CREATE TYPE dt.task_event AS (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    event_type VARCHAR(40),
    task_id INTEGER,
    visible_time TIMESTAMP WITH TIME ZONE,
    locked_by VARCHAR(100),
    lock_expiration TIMESTAMP WITH TIME ZONE,
    reason TEXT,
    payload_text JSONB,
    payload_id UUID,
    version VARCHAR(100),
    trace_context VARCHAR(800)
);

-- Function: dt.checkpoint_orchestration
-- Purpose: Atomic state transition for orchestration execution
-- This is the MOST COMPLEX procedure - handles:
--   - Custom status updates
--   - ContinueAsNew (execution ID changes)
--   - Completion detection
--   - Payload management
--   - History append
--   - Event deletion (processed events)
--   - New event insertion (orchestration + task events)
--   - Sub-orchestration creation
-- Parameters:
--   p_instance_id: Instance being checkpointed
--   p_execution_id: Current execution ID
--   p_runtime_status: New status (Running, Completed, Failed, Terminated)
--   p_custom_status_payload: Optional custom status JSON
--   p_deleted_events: Events to delete (already processed)
--   p_new_history_events: History to append
--   p_new_orchestration_events: New orchestration events to insert
--   p_new_task_events: New tasks to schedule
-- Returns: void (throws exception on failure)
CREATE OR REPLACE FUNCTION dt.checkpoint_orchestration(
    p_instance_id VARCHAR(100),
    p_execution_id VARCHAR(50),
    p_runtime_status VARCHAR(30),
    p_custom_status_payload TEXT DEFAULT NULL,
    p_deleted_events dt.message_id[] DEFAULT NULL,
    p_new_history_events dt.history_event[] DEFAULT NULL,
    p_new_orchestration_events dt.orchestration_event[] DEFAULT NULL,
    p_new_task_events dt.task_event[] DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_input_payload_id UUID;
    v_custom_status_payload_id UUID;
    v_existing_output_payload_id UUID;
    v_existing_custom_status_payload TEXT;
    v_existing_execution_id VARCHAR(50);
    v_is_continue_as_new BOOLEAN := FALSE;
    v_is_completed BOOLEAN;
    v_output_payload_id UUID;
    v_rows_affected INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Check for existing custom status and execution ID
    SELECT
        i.input_payload_id,
        i.custom_status_payload_id,
        i.output_payload_id,
        p.text::TEXT,
        i.execution_id
    INTO
        v_input_payload_id,
        v_custom_status_payload_id,
        v_existing_output_payload_id,
        v_existing_custom_status_payload,
        v_existing_execution_id
    FROM dt.instances i
    LEFT JOIN dt.payloads p ON
        p.task_hub = v_task_hub AND
        p.instance_id = i.instance_id AND
        p.payload_id = i.custom_status_payload_id
    WHERE i.task_hub = v_task_hub
      AND i.instance_id = p_instance_id;

    -- ContinueAsNew case: delete existing runtime state
    IF v_existing_execution_id IS NOT NULL AND v_existing_execution_id <> p_execution_id THEN
        v_is_continue_as_new := TRUE;

        -- Delete history and collect payload IDs
        WITH deleted_history AS (
            DELETE FROM dt.history
            WHERE task_hub = v_task_hub
              AND instance_id = p_instance_id
            RETURNING data_payload_id
        )
        DELETE FROM dt.payloads
        WHERE task_hub = v_task_hub
          AND instance_id = p_instance_id
          AND payload_id IN (
              SELECT data_payload_id FROM deleted_history
              UNION ALL
              SELECT unnest(ARRAY[v_input_payload_id, v_custom_status_payload_id, v_existing_output_payload_id])
          );

        v_existing_custom_status_payload := NULL;
    END IF;

    -- Custom status handling
    IF v_existing_custom_status_payload IS NULL AND p_custom_status_payload IS NOT NULL THEN
        -- Case #1: Setting custom status for first time
        v_custom_status_payload_id := gen_random_uuid();
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text)
        VALUES (v_task_hub, p_instance_id, v_custom_status_payload_id, p_custom_status_payload::JSONB);
    ELSIF v_existing_custom_status_payload IS NOT NULL AND v_existing_custom_status_payload <> p_custom_status_payload THEN
        -- Case #2: Updating existing custom status
        UPDATE dt.payloads
        SET text = p_custom_status_payload::JSONB
        WHERE task_hub = v_task_hub
          AND instance_id = p_instance_id
          AND payload_id = v_custom_status_payload_id;
    END IF;

    -- Update input payload ID if ContinueAsNew
    IF v_is_continue_as_new THEN
        SELECT h.payload_id INTO v_input_payload_id
        FROM unnest(p_new_history_events) AS h
        WHERE h.event_type = 'ExecutionStarted'
        ORDER BY h.sequence_number DESC
        LIMIT 1;
    END IF;

    -- Determine if completed
    v_is_completed := p_runtime_status IN ('Completed', 'Failed', 'Terminated');

    -- Get output payload ID if completed
    IF v_is_completed THEN
        SELECT h.payload_id INTO v_output_payload_id
        FROM unnest(p_new_history_events) AS h
        WHERE h.event_type IN ('ExecutionCompleted', 'ExecutionTerminated')
        ORDER BY h.sequence_number DESC
        LIMIT 1;
    END IF;

    -- Insert history event payloads
    IF p_new_history_events IS NOT NULL THEN
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text, reason)
        SELECT v_task_hub, h.instance_id, h.payload_id, h.payload_text, h.reason
        FROM unnest(p_new_history_events) AS h
        WHERE h.payload_text IS NOT NULL OR h.reason IS NOT NULL;
    END IF;

    -- Update instance
    UPDATE dt.instances
    SET execution_id = p_execution_id,
        runtime_status = p_runtime_status,
        last_updated_time = NOW(),
        completed_time = CASE WHEN v_is_completed THEN NOW() ELSE NULL END,
        lock_expiration = NULL,  -- Release lock
        custom_status_payload_id = v_custom_status_payload_id,
        input_payload_id = v_input_payload_id,
        output_payload_id = v_output_payload_id
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    IF v_rows_affected = 0 THEN
        RAISE EXCEPTION 'The instance % does not exist', p_instance_id
            USING ERRCODE = '02000';  -- no_data
    END IF;

    -- Create sub-orchestration instances from new events
    IF p_new_orchestration_events IS NOT NULL THEN
        INSERT INTO dt.instances (
            task_hub, instance_id, execution_id, name, version,
            parent_instance_id, runtime_status, trace_context
        )
        SELECT DISTINCT
            v_task_hub, e.instance_id, e.execution_id, e.name, e.version,
            e.parent_instance_id, 'Pending', e.trace_context
        FROM unnest(p_new_orchestration_events) AS e
        WHERE e.event_type = 'ExecutionStarted'
          AND NOT EXISTS (
              SELECT 1 FROM dt.instances i
              WHERE i.task_hub = v_task_hub
                AND i.instance_id = e.instance_id
          )
        ORDER BY e.instance_id;

        -- Insert orchestration event payloads
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text, reason)
        SELECT v_task_hub, e.instance_id, e.payload_id, e.payload_text, e.reason
        FROM unnest(p_new_orchestration_events) AS e
        WHERE e.payload_id IS NOT NULL;

        -- Insert new orchestration events
        INSERT INTO dt.new_events (
            task_hub, instance_id, execution_id, event_type, name,
            runtime_status, visible_time, task_id, trace_context, payload_id
        )
        SELECT
            v_task_hub, e.instance_id, e.execution_id, e.event_type, e.name,
            e.runtime_status, e.visible_time, e.task_id, e.trace_context, e.payload_id
        FROM unnest(p_new_orchestration_events) AS e;
    END IF;

    -- Delete processed events
    IF p_deleted_events IS NOT NULL THEN
        DELETE FROM dt.new_events e
        USING unnest(p_deleted_events) AS d
        WHERE e.task_hub = v_task_hub
          AND e.instance_id = d.instance_id
          AND e.sequence_number = d.sequence_number;
    END IF;

    -- Insert history events (CRITICAL: PK violation detection for split-brain)
    IF p_new_history_events IS NOT NULL THEN
        INSERT INTO dt.history (
            task_hub, instance_id, execution_id, sequence_number, event_type,
            task_id, "timestamp", is_played, name, runtime_status, visible_time,
            trace_context, data_payload_id
        )
        SELECT
            v_task_hub, h.instance_id, h.execution_id, h.sequence_number, h.event_type,
            h.task_id, h."timestamp", h.is_played, h.name, h.runtime_status, h.visible_time,
            h.trace_context, h.payload_id
        FROM unnest(p_new_history_events) AS h;
        -- NOTE: PK violation will throw exception automatically (split-brain detection)
    END IF;

    -- Insert new task events
    IF p_new_task_events IS NOT NULL THEN
        -- Insert task payloads
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text)
        SELECT v_task_hub, t.instance_id, t.payload_id, t.payload_text
        FROM unnest(p_new_task_events) AS t
        WHERE t.payload_id IS NOT NULL;

        -- Insert new tasks
        INSERT INTO dt.new_tasks (
            task_hub, instance_id, execution_id, name, task_id, visible_time,
            locked_by, lock_expiration, payload_id, version, trace_context
        )
        SELECT
            v_task_hub, t.instance_id, t.execution_id, t.name, t.task_id, t.visible_time,
            t.locked_by, t.lock_expiration, t.payload_id, t.version, t.trace_context
        FROM unnest(p_new_task_events) AS t;
    END IF;

    -- Transaction commits automatically on function exit
END;
$$;

COMMENT ON FUNCTION dt.checkpoint_orchestration IS 'Atomic orchestration state transition (most complex operation)';

-- =============================================================================
-- SEND MESSAGE (RAISE EVENT)
-- =============================================================================

-- Function: dt.add_orchestration_event
-- Purpose: Add new event to an existing orchestration instance
-- Parameters:
--   p_instance_id: Target instance ID
--   p_event_type: Event type (EventSent, EventRaised, etc.)
--   p_name: Optional event name
--   p_payload_text: Optional payload
--   p_trace_context: Optional distributed tracing context
-- Returns: void
CREATE OR REPLACE FUNCTION dt.add_orchestration_event(
    p_instance_id VARCHAR(100),
    p_event_type VARCHAR(40),
    p_name VARCHAR(300) DEFAULT NULL,
    p_payload_text JSONB DEFAULT NULL,
    p_trace_context VARCHAR(800) DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_execution_id VARCHAR(50);
    v_payload_id UUID;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Get execution ID from existing instance
    SELECT i.execution_id INTO v_execution_id
    FROM dt.instances i
    WHERE i.task_hub = v_task_hub
      AND i.instance_id = p_instance_id;

    IF v_execution_id IS NULL THEN
        RAISE EXCEPTION 'Instance % not found', p_instance_id;
    END IF;

    -- Insert payload if provided
    IF p_payload_text IS NOT NULL THEN
        v_payload_id := gen_random_uuid();
        INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text)
        VALUES (v_task_hub, p_instance_id, v_payload_id, p_payload_text);
    END IF;

    -- Insert event
    INSERT INTO dt.new_events (
        task_hub, instance_id, execution_id, event_type, name, payload_id, trace_context
    ) VALUES (
        v_task_hub, p_instance_id, v_execution_id, p_event_type, p_name, v_payload_id, p_trace_context
    );
END;
$$;

COMMENT ON FUNCTION dt.add_orchestration_event IS 'Add new event to existing orchestration instance';

-- Function: dt.terminate_instance
-- Purpose: Force terminate an orchestration instance
-- Parameters:
--   p_instance_id: Instance to terminate
--   p_reason: Termination reason
-- Returns: void
CREATE OR REPLACE FUNCTION dt.terminate_instance(
    p_instance_id VARCHAR(100),
    p_reason TEXT DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_payload_id UUID;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Create termination payload
    v_payload_id := gen_random_uuid();
    INSERT INTO dt.payloads (task_hub, instance_id, payload_id, text, reason)
    VALUES (v_task_hub, p_instance_id, v_payload_id, to_jsonb(p_reason), p_reason);

    -- Insert termination event
    INSERT INTO dt.new_events (
        task_hub, instance_id, execution_id, event_type, payload_id
    )
    SELECT 
        v_task_hub, 
        p_instance_id, 
        i.execution_id, 
        'ExecutionTerminated',
        v_payload_id
    FROM dt.instances i
    WHERE i.task_hub = v_task_hub
      AND i.instance_id = p_instance_id;

    -- Update instance status
    UPDATE dt.instances
    SET runtime_status = 'Terminated'
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id;
END;
$$;

COMMENT ON FUNCTION dt.terminate_instance IS 'Force terminate an orchestration instance';

-- Function: dt.get_instance_history
-- Purpose: Get full history for an instance
-- Parameters:
--   p_instance_id: Instance ID
--   p_execution_id: Optional execution ID filter
-- Returns: JSON array of history events
CREATE OR REPLACE FUNCTION dt.get_instance_history(
    p_instance_id VARCHAR(100),
    p_execution_id VARCHAR(50) DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_result JSONB;
BEGIN
    v_task_hub := dt.current_task_hub();

    v_result := (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'instanceId', h.instance_id,
                'executionId', h.execution_id,
                'sequenceNumber', h.sequence_number,
                'eventType', h.event_type,
                'name', h.name,
                'runtimeStatus', h.runtime_status,
                'taskId', h.task_id,
                'timestamp', h."timestamp",
                'isPlayed', h.is_played,
                'visibleTime', h.visible_time,
                'reason', p.reason,
                'payloadText', p.text,
                'payloadId', h.data_payload_id,
                'traceContext', h.trace_context
            ) ORDER BY h.sequence_number
        ), '[]'::JSONB)
        FROM dt.history h
        LEFT JOIN dt.payloads p ON
            p.task_hub = v_task_hub AND
            p.instance_id = h.instance_id AND
            p.payload_id = h.data_payload_id
        WHERE h.task_hub = v_task_hub
          AND h.instance_id = p_instance_id
          AND (p_execution_id IS NULL OR h.execution_id = p_execution_id)
    );

    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION dt.get_instance_history IS 'Get full history for an orchestration instance';

-- Function: dt.purge_instance_state_by_time
-- Purpose: Purge orchestration instances older than threshold
-- Parameters:
--   p_threshold_time: Delete instances created before this time
--   p_filter_type: 0=All, 1=Completed, 2=Failed, 3=Terminated
-- Returns: Number of instances deleted
CREATE OR REPLACE FUNCTION dt.purge_instance_state_by_time(
    p_threshold_time TIMESTAMP WITH TIME ZONE,
    p_filter_type SMALLINT DEFAULT 0
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_instance_ids VARCHAR(100)[];
    v_instances_deleted INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Collect matching instance IDs first to enable cascade deletes
    SELECT array_agg(instance_id) INTO v_instance_ids
    FROM dt.instances
    WHERE task_hub = v_task_hub
      AND created_time < p_threshold_time
      AND runtime_status = CASE
          WHEN p_filter_type = 1 THEN 'Completed'
          WHEN p_filter_type = 2 THEN 'Failed'
          WHEN p_filter_type = 3 THEN 'Terminated'
          ELSE runtime_status
      END;

    -- Nothing to purge
    IF v_instance_ids IS NULL THEN
        RETURN 0;
    END IF;

    -- Delete in dependency order to avoid FK violations
    DELETE FROM dt.payloads
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(v_instance_ids);

    DELETE FROM dt.history
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(v_instance_ids);

    DELETE FROM dt.new_events
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(v_instance_ids);

    DELETE FROM dt.new_tasks
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(v_instance_ids);

    DELETE FROM dt.instances
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(v_instance_ids);

    v_instances_deleted := array_length(v_instance_ids, 1);
    RETURN v_instances_deleted;
END;
$$;

COMMENT ON FUNCTION dt.purge_instance_state_by_time IS 'Purge orchestration instances by time';

-- Function: dt.purge_instance_state_by_id
-- Purpose: Purge specific orchestration instance by ID
-- Parameters:
--   p_instance_ids: Array of instance IDs to purge
-- Returns: Number of instances deleted
CREATE OR REPLACE FUNCTION dt.purge_instance_state_by_id(
    p_instance_ids VARCHAR(100)[]
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_instances_deleted INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Delete payloads first
    DELETE FROM dt.payloads
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(p_instance_ids);

    -- Delete history
    DELETE FROM dt.history
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(p_instance_ids);

    -- Delete new events
    DELETE FROM dt.new_events
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(p_instance_ids);

    -- Delete new tasks
    DELETE FROM dt.new_tasks
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(p_instance_ids);

    -- Delete instances
    DELETE FROM dt.instances
    WHERE task_hub = v_task_hub
      AND instance_id = ANY(p_instance_ids);

    GET DIAGNOSTICS v_instances_deleted = ROW_COUNT;
    RETURN v_instances_deleted;
END;
$$;

COMMENT ON FUNCTION dt.purge_instance_state_by_id IS 'Purge specific orchestration instances by ID';

-- Function: dt.query_many_orchestrations
-- Purpose: Query multiple orchestration instances with filters
-- Parameters:
--   p_page_size: Max results per page
--   p_page_number: Page number (0-based)
--   p_fetch_input: Include input
--   p_fetch_output: Include output
--   p_created_time_from: Filter by created time (from)
--   p_created_time_to: Filter by created time (to)
--   p_instance_id_prefix: Filter by instance ID prefix
--   p_exclude_sub_orchestrations: Exclude sub-orchestrations
--   p_runtime_status_filter: Filter by runtime status (comma-separated)
-- Returns: Set of orchestration states
CREATE OR REPLACE FUNCTION dt.query_many_orchestrations(
    p_page_size SMALLINT,
    p_page_number INTEGER,
    p_fetch_input BOOLEAN DEFAULT TRUE,
    p_fetch_output BOOLEAN DEFAULT TRUE,
    p_created_time_from TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01',
    p_created_time_to TIMESTAMP WITH TIME ZONE DEFAULT '2099-12-31',
    p_instance_id_prefix VARCHAR(100) DEFAULT NULL,
    p_exclude_sub_orchestrations BOOLEAN DEFAULT FALSE,
    p_runtime_status_filter VARCHAR(200) DEFAULT NULL
)
RETURNS TABLE (
    instance_id VARCHAR(100),
    execution_id VARCHAR(50),
    name VARCHAR(300),
    version VARCHAR(100),
    created_time TIMESTAMP WITH TIME ZONE,
    last_updated_time TIMESTAMP WITH TIME ZONE,
    completed_time TIMESTAMP WITH TIME ZONE,
    runtime_status VARCHAR(20),
    input_text JSONB,
    output_text JSONB,
    custom_status_text JSONB,
    parent_instance_id VARCHAR(100),
    trace_context VARCHAR(800)
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_offset INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();
    v_offset := p_page_number * p_page_size;

    RETURN QUERY
    SELECT
        i.instance_id,
        i.execution_id,
        i.name,
        i.version,
        i.created_time,
        i.last_updated_time,
        i.completed_time,
        i.runtime_status,
        CASE WHEN p_fetch_input THEN p_input.text ELSE NULL END AS input_text,
        CASE WHEN p_fetch_output THEN p_output.text ELSE NULL END AS output_text,
        p_custom.text AS custom_status_text,
        i.parent_instance_id,
        i.trace_context
    FROM dt.instances i
    LEFT JOIN dt.payloads p_input ON
        p_input.task_hub = v_task_hub AND
        p_input.instance_id = i.instance_id AND
        p_input.payload_id = i.input_payload_id
    LEFT JOIN dt.payloads p_output ON
        p_output.task_hub = v_task_hub AND
        p_output.instance_id = i.instance_id AND
        p_output.payload_id = i.output_payload_id
    LEFT JOIN dt.payloads p_custom ON
        p_custom.task_hub = v_task_hub AND
        p_custom.instance_id = i.instance_id AND
        p_custom.payload_id = i.custom_status_payload_id
    WHERE i.task_hub = v_task_hub
      AND i.created_time >= p_created_time_from
      AND i.created_time <= p_created_time_to
      AND (p_instance_id_prefix IS NULL OR i.instance_id LIKE p_instance_id_prefix || '%')
      AND (p_exclude_sub_orchestrations = FALSE OR i.parent_instance_id IS NULL)
      AND (p_runtime_status_filter IS NULL OR i.runtime_status = ANY(string_to_array(p_runtime_status_filter, ',')))
    ORDER BY i.created_time DESC
    LIMIT p_page_size
    OFFSET v_offset;
END;
$$;

COMMENT ON FUNCTION dt.query_many_orchestrations IS 'Query multiple orchestration instances with filters';

-- Function: dt.rewind_instance
-- Purpose: Rewind a failed orchestration to retry
-- Parameters:
--   p_instance_id: Instance to rewind
--   p_reason: Reason for rewind
-- Returns: void
CREATE OR REPLACE FUNCTION dt.rewind_instance(
    p_instance_id VARCHAR(100),
    p_reason TEXT DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_new_execution_id VARCHAR(50);
BEGIN
    v_task_hub := dt.current_task_hub();
    v_new_execution_id := gen_random_uuid()::TEXT;

    -- Update instance with new execution ID and set to running
    UPDATE dt.instances
    SET execution_id = v_new_execution_id,
        runtime_status = 'Running',
        last_updated_time = NOW(),
        completed_time = NULL,
        output_payload_id = NULL
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id
      AND runtime_status = 'Failed';

    -- Delete history for failed execution
    DELETE FROM dt.history
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id;

    -- Delete new events that were created after the failure
    DELETE FROM dt.new_events
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id;

    -- Delete pending tasks
    DELETE FROM dt.new_tasks
    WHERE task_hub = v_task_hub
      AND instance_id = p_instance_id;
END;
$$;

COMMENT ON FUNCTION dt.rewind_instance IS 'Rewind a failed orchestration to retry';

-- Function: dt.get_scale_recommendation
-- Purpose: Get recommended replica count based on backlog
-- Parameters:
--   p_max_concurrent_orchestrations: Max concurrent orchestrations
--   p_max_concurrent_activities: Max concurrent activities
-- Returns: Recommended replica count
CREATE OR REPLACE FUNCTION dt.get_scale_recommendation(
    p_max_concurrent_orchestrations INTEGER,
    p_max_concurrent_activities INTEGER
)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_pending_events INTEGER;
    v_pending_tasks INTEGER;
BEGIN
    v_task_hub := dt.current_task_hub();

    -- Count pending events
    SELECT COUNT(*) INTO v_pending_events
    FROM dt.new_events
    WHERE task_hub = v_task_hub;

    -- Count pending tasks
    SELECT COUNT(*) INTO v_pending_tasks
    FROM dt.new_tasks
    WHERE task_hub = v_task_hub;

    -- Simple recommendation: 1 worker per 10 pending items, min 1, max based on concurrency settings
    RETURN GREATEST(1, LEAST(
        p_max_concurrent_orchestrations,
        (v_pending_events / 10 + 1)::INTEGER,
        (v_pending_tasks / 10 + 1)::INTEGER
    ));
END;
$$;

COMMENT ON FUNCTION dt.get_scale_recommendation IS 'Get recommended replica count based on backlog';

-- Logic script complete
