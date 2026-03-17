"""Raw SQL for every metadata-table operation.

Tables
------
commits          – one row per logical commit (multi-step)
commit_steps     – ordered SQL steps inside a commit
anti_commands    – inverse SQL for each step (stored externally)
snapshots        – S3 snapshot metadata
snapshot_config  – single-row table holding the snapshot frequency (1–5)
"""

# ── Schema creation ───────────────────────────────────────────────────────────

INIT_METADATA_TABLES = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS commits (
    commit_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    commit_number  BIGSERIAL   UNIQUE NOT NULL,
    hash           VARCHAR(64) NOT NULL,
    message        TEXT,
    created_at     TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS commit_steps (
    step_id      BIGSERIAL PRIMARY KEY,
    commit_id    UUID      NOT NULL REFERENCES commits(commit_id) ON DELETE CASCADE,
    step_order   INT       NOT NULL,
    sql_command  TEXT      NOT NULL,
    step_type    VARCHAR(16) NOT NULL DEFAULT 'DML'
);

CREATE TABLE IF NOT EXISTS anti_commands (
    id         BIGSERIAL PRIMARY KEY,
    commit_id  UUID      NOT NULL REFERENCES commits(commit_id) ON DELETE CASCADE,
    step_id    BIGINT    NOT NULL REFERENCES commit_steps(step_id) ON DELETE CASCADE,
    anti_sql   TEXT      NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id            BIGSERIAL PRIMARY KEY,
    commit_number BIGINT    NOT NULL,
    s3_key        TEXT      NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS snapshot_config (
    id        INT PRIMARY KEY DEFAULT 1,
    frequency INT NOT NULL DEFAULT 5,
    CHECK (frequency >= 1 AND frequency <= 5)
);

-- Seed the config row if it doesn't exist
INSERT INTO snapshot_config (id, frequency)
VALUES (1, 5)
ON CONFLICT (id) DO NOTHING;
"""

# ── Commits ───────────────────────────────────────────────────────────────────

INSERT_COMMIT = """
INSERT INTO commits (hash, message)
VALUES (%s, %s)
RETURNING commit_id, commit_number, created_at;
"""

SELECT_ALL_COMMITS = """
SELECT commit_id, commit_number, hash, message, created_at
FROM commits
ORDER BY commit_number;
"""

SELECT_COMMIT_BY_ID = """
SELECT commit_id, commit_number, hash, message, created_at
FROM commits
WHERE commit_id = %s;
"""

SELECT_COMMIT_BY_NUMBER = """
SELECT commit_id, commit_number, hash, message, created_at
FROM commits
WHERE commit_number = %s;
"""

SELECT_LATEST_COMMIT_NUMBER = """
SELECT COALESCE(MAX(commit_number), 0) AS latest FROM commits;
"""

# ── Commit steps ──────────────────────────────────────────────────────────────

INSERT_COMMIT_STEP = """
INSERT INTO commit_steps (commit_id, step_order, sql_command, step_type)
VALUES (%s, %s, %s, %s)
RETURNING step_id;
"""

SELECT_STEPS_BY_COMMIT = """
SELECT step_id, commit_id, step_order, sql_command, step_type
FROM commit_steps
WHERE commit_id = %s
ORDER BY step_order;
"""

# ── Anti-commands ─────────────────────────────────────────────────────────────

INSERT_ANTI_COMMAND = """
INSERT INTO anti_commands (commit_id, step_id, anti_sql)
VALUES (%s, %s, %s)
RETURNING id;
"""

SELECT_ANTI_COMMANDS_BY_COMMIT = """
SELECT id, commit_id, step_id, anti_sql
FROM anti_commands
WHERE commit_id = %s
ORDER BY step_id;
"""

SELECT_ANTI_COMMANDS_FOR_ROLLBACK = """
SELECT ac.id, ac.commit_id, ac.step_id, ac.anti_sql
FROM anti_commands ac
JOIN commits c ON c.commit_id = ac.commit_id
WHERE c.commit_number > %s
ORDER BY c.commit_number DESC, ac.step_id DESC;
"""

# ── Snapshots ─────────────────────────────────────────────────────────────────

INSERT_SNAPSHOT = """
INSERT INTO snapshots (commit_number, s3_key)
VALUES (%s, %s)
RETURNING id, created_at;
"""

SELECT_ALL_SNAPSHOTS = """
SELECT id, commit_number, s3_key, created_at
FROM snapshots
ORDER BY commit_number DESC;
"""

SELECT_NEAREST_SNAPSHOT_BEFORE = """
SELECT id, commit_number, s3_key, created_at
FROM snapshots
WHERE commit_number <= %s
ORDER BY commit_number DESC
LIMIT 1;
"""

# ── Snapshot config ───────────────────────────────────────────────────────────

SELECT_SNAPSHOT_FREQUENCY = """
SELECT frequency FROM snapshot_config WHERE id = 1;
"""

UPDATE_SNAPSHOT_FREQUENCY = """
UPDATE snapshot_config SET frequency = %s WHERE id = 1;
"""
