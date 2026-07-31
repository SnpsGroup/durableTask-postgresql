-- Forward migration: 1.0.0 -> 1.1.0
-- Purpose: establishes the forward-migration mechanism. Adds an optional description
-- column to dt.instances for future use. Idempotent via ADD COLUMN IF NOT EXISTS.
ALTER TABLE dt.instances ADD COLUMN IF NOT EXISTS description VARCHAR(500);
