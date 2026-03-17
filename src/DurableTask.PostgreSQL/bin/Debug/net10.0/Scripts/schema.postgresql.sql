-- Copyright (c) Janus SGA Contributors.
-- Licensed under the MIT License.
--
-- DurableTask.PostgreSQL Schema
-- PostgreSQL port of DurableTask.SqlServer
-- Based on: https://github.com/microsoft/durabletask-mssql
--
-- Target: PostgreSQL 17+
-- Version: 0.1.0-poc

-- =============================================================================
-- SCHEMA CREATION
-- =============================================================================

-- Create dt schema if not exists
CREATE SCHEMA IF NOT EXISTS dt;

-- =============================================================================
-- TABLES
-- =============================================================================

-- Table: dt.versions
-- Purpose: Track schema migrations using semantic versioning
CREATE TABLE IF NOT EXISTS dt.versions (
    semantic_version VARCHAR(100) NOT NULL PRIMARY KEY,
    upgrade_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Insert initial version
INSERT INTO dt.versions (semantic_version)
VALUES ('0.1.0-poc')
ON CONFLICT (semantic_version) DO NOTHING;

-- Table: dt.payloads
-- Purpose: Store large payloads externally to avoid bloating main tables
-- Note: Using JSONB instead of TEXT for structured data validation
CREATE TABLE IF NOT EXISTS dt.payloads (
    task_hub VARCHAR(50) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    payload_id UUID NOT NULL,
    text JSONB NULL,  -- Changed to JSONB for better querying
    reason TEXT NULL,

    CONSTRAINT pk_payloads PRIMARY KEY (task_hub, instance_id, payload_id)
);

-- Index for payload lookup by instance
CREATE INDEX IF NOT EXISTS idx_payloads_instance
    ON dt.payloads(task_hub, instance_id);

-- Table: dt.instances
-- Purpose: Core orchestration instance metadata and state
CREATE TABLE IF NOT EXISTS dt.instances (
    task_hub VARCHAR(50) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NOT NULL DEFAULT gen_random_uuid()::TEXT,
    name VARCHAR(300) NOT NULL,  -- Orchestration type name
    version VARCHAR(100) NULL,
    created_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated_time TIMESTAMP WITH TIME ZONE NULL,
    completed_time TIMESTAMP WITH TIME ZONE NULL,
    runtime_status VARCHAR(20) NOT NULL,
    locked_by VARCHAR(100) NULL,
    lock_expiration TIMESTAMP WITH TIME ZONE NULL,
    input_payload_id UUID NULL,
    output_payload_id UUID NULL,
    custom_status_payload_id UUID NULL,
    parent_instance_id VARCHAR(100) NULL,
    trace_context VARCHAR(800) NULL,  -- v1.2.0 distributed tracing support

    CONSTRAINT pk_instances PRIMARY KEY (task_hub, instance_id),
    CONSTRAINT chk_runtime_status CHECK (runtime_status IN
        ('Pending', 'Running', 'Completed', 'Failed', 'Terminated', 'Suspended'))
);

-- Index: Used by LockNext and Purge operations
-- Partial index to only index active/lockable instances
CREATE INDEX IF NOT EXISTS idx_instances_runtime_status
    ON dt.instances(task_hub, runtime_status)
    INCLUDE (lock_expiration, created_time, completed_time)
    WHERE runtime_status IN ('Pending', 'Running');

-- Index: Used for multi-instance queries and time-based filtering
CREATE INDEX IF NOT EXISTS idx_instances_created_time
    ON dt.instances(task_hub, created_time DESC)
    INCLUDE (runtime_status, completed_time, instance_id);

-- Index: Parent-child relationships for sub-orchestrations
CREATE INDEX IF NOT EXISTS idx_instances_parent
    ON dt.instances(task_hub, parent_instance_id)
    WHERE parent_instance_id IS NOT NULL;

-- Table: dt.new_events
-- Purpose: FIFO queue of pending orchestration events
CREATE TABLE IF NOT EXISTS dt.new_events (
    sequence_number BIGSERIAL NOT NULL,  -- IDENTITY equivalent
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    visible_time TIMESTAMP WITH TIME ZONE NULL,  -- For durable timers
    dequeue_count INTEGER NOT NULL DEFAULT 0,
    task_hub VARCHAR(50) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NULL,
    event_type VARCHAR(40) NOT NULL,
    runtime_status VARCHAR(30) NULL,
    name VARCHAR(300) NULL,
    task_id INTEGER NULL,
    payload_id UUID NULL,
    trace_context VARCHAR(800) NULL,

    CONSTRAINT pk_new_events PRIMARY KEY (task_hub, instance_id, sequence_number)
);

-- Index: For LockNextOrchestration to find instances with pending events
CREATE INDEX IF NOT EXISTS idx_new_events_visible
    ON dt.new_events(task_hub, instance_id, visible_time)
    WHERE visible_time IS NOT NULL;

-- Table: dt.history
-- Purpose: Event sourcing log (append-only)
CREATE TABLE IF NOT EXISTS dt.history (
    task_hub VARCHAR(50) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NOT NULL,
    sequence_number BIGINT NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    task_id INTEGER NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    is_played BOOLEAN NOT NULL DEFAULT FALSE,
    name VARCHAR(300) NULL,
    runtime_status VARCHAR(20) NULL,
    visible_time TIMESTAMP WITH TIME ZONE NULL,
    data_payload_id UUID NULL,
    trace_context VARCHAR(800) NULL,

    CONSTRAINT pk_history PRIMARY KEY (task_hub, instance_id, execution_id, sequence_number)
);

-- Index: For efficient history retrieval ordered by sequence
CREATE INDEX IF NOT EXISTS idx_history_instance
    ON dt.history(task_hub, instance_id, execution_id, sequence_number);

-- Table: dt.new_tasks
-- Purpose: FIFO queue of pending activity tasks
CREATE TABLE IF NOT EXISTS dt.new_tasks (
    task_hub VARCHAR(50) NOT NULL,
    sequence_number BIGSERIAL NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    execution_id VARCHAR(50) NULL,
    name VARCHAR(300) NULL,
    task_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    visible_time TIMESTAMP WITH TIME ZONE NULL,
    dequeue_count INTEGER NOT NULL DEFAULT 0,
    locked_by VARCHAR(100) NULL,
    lock_expiration TIMESTAMP WITH TIME ZONE NULL,
    payload_id UUID NULL,
    version VARCHAR(100) NULL,
    trace_context VARCHAR(800) NULL,

    CONSTRAINT pk_new_tasks PRIMARY KEY (task_hub, sequence_number)
);

-- Index: For vScaleHints and instance-based queries
CREATE INDEX IF NOT EXISTS idx_new_tasks_instance
    ON dt.new_tasks(task_hub, instance_id)
    INCLUDE (sequence_number, timestamp, lock_expiration, visible_time);

-- Index: For LockNextTask to efficiently find next available task
-- NOTE: Partial index with NOW() removed in POC (NOW() is not IMMUTABLE)
-- Full index created instead for simplicity
CREATE INDEX IF NOT EXISTS idx_new_tasks_lockable
    ON dt.new_tasks(task_hub, sequence_number, lock_expiration);

-- Table: dt.global_settings
-- Purpose: Configuration settings (e.g., TaskHubMode)
CREATE TABLE IF NOT EXISTS dt.global_settings (
    name VARCHAR(300) NOT NULL PRIMARY KEY,
    value TEXT NULL,  -- Using TEXT instead of sql_variant
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_modified_by VARCHAR(128) NOT NULL DEFAULT CURRENT_USER
);

-- Default task hub mode (1 = User ID based)
INSERT INTO dt.global_settings (name, value)
VALUES ('TaskHubMode', '1')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Function: dt.current_task_hub()
-- Purpose: Get current task hub name based on TaskHubMode setting
-- Returns: Task hub identifier (max 50 chars)
CREATE OR REPLACE FUNCTION dt.current_task_hub()
RETURNS VARCHAR(50)
LANGUAGE plpgsql
STABLE  -- Result doesn't change within transaction (cannot be IMMUTABLE as it calls CURRENT_USER)
SECURITY DEFINER  -- Run with owner privileges to access global_settings
AS $$
DECLARE
    v_task_hub_mode TEXT;
    v_task_hub VARCHAR(150);
BEGIN
    -- Get TaskHubMode setting
    SELECT value INTO v_task_hub_mode
    FROM dt.global_settings
    WHERE name = 'TaskHubMode'
    LIMIT 1;

    -- Mode 0: Task hub from application name
    IF v_task_hub_mode = '0' THEN
        v_task_hub := current_setting('application_name', true);
    -- Mode 1: Task hub from current user
    ELSIF v_task_hub_mode = '1' THEN
        v_task_hub := CURRENT_USER;
    END IF;

    -- Default if not set
    IF v_task_hub IS NULL OR v_task_hub = '' THEN
        v_task_hub := 'default';
    END IF;

    -- Hash long names (keep first 16 chars + MD5 hash)
    IF LENGTH(v_task_hub) > 50 THEN
        v_task_hub := SUBSTRING(v_task_hub, 1, 16) || '__' ||
                      MD5(v_task_hub);
    END IF;

    RETURN v_task_hub;
END;
$$;

-- Function: dt.get_scale_metric()
-- Purpose: Calculate autoscaling metric (active orchestrations + tasks)
-- Returns: Count of work items requiring processing
CREATE OR REPLACE FUNCTION dt.get_scale_metric()
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_task_hub VARCHAR(50);
    v_now TIMESTAMP WITH TIME ZONE;
    v_live_instances BIGINT;
    v_live_tasks BIGINT;
BEGIN
    v_task_hub := dt.current_task_hub();
    v_now := NOW();

    -- Count distinct instances with pending events + tasks
    SELECT
        COUNT(DISTINCT e.instance_id),
        COUNT(t.instance_id)
    INTO v_live_instances, v_live_tasks
    FROM dt.instances i
    LEFT JOIN dt.new_events e ON
        e.task_hub = v_task_hub AND
        e.instance_id = i.instance_id AND
        (e.visible_time IS NULL OR e.visible_time < v_now)
    LEFT JOIN dt.new_tasks t ON
        t.task_hub = v_task_hub AND
        t.instance_id = i.instance_id
    WHERE i.task_hub = v_task_hub
      AND i.runtime_status IN ('Pending', 'Running');

    RETURN COALESCE(v_live_instances, 0) + COALESCE(v_live_tasks, 0);
END;
$$;


-- =============================================================================

-- NOTE: Views commented out in POC due to STABLE function in WHERE clause
-- View: dt.v_instances would require IMMUTABLE function for predicate
-- Uncomment when needed or remove WHERE clause and filter at application level

-- =============================================================================
-- COMMENTS (PostgreSQL Documentation)
-- =============================================================================

COMMENT ON SCHEMA dt IS 'DurableTask.PostgreSQL - Durable workflow orchestration provider';
COMMENT ON TABLE dt.instances IS 'Orchestration instance metadata and current runtime state';
COMMENT ON TABLE dt.history IS 'Event sourcing log for orchestration execution (append-only)';
COMMENT ON TABLE dt.new_events IS 'FIFO queue of pending orchestration events';
COMMENT ON TABLE dt.new_tasks IS 'FIFO queue of pending activity tasks';
COMMENT ON TABLE dt.payloads IS 'External storage for large payloads (input/output/custom status)';
COMMENT ON TABLE dt.versions IS 'Schema version tracking for migrations';
COMMENT ON TABLE dt.global_settings IS 'Configuration settings (TaskHubMode, etc)';

COMMENT ON FUNCTION dt.current_task_hub() IS 'Returns current task hub name based on TaskHubMode setting';
COMMENT ON FUNCTION dt.get_scale_metric() IS 'Returns count of active orchestrations + tasks for autoscaling';

-- Schema creation complete
