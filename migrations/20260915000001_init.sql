CREATE TABLE workflow_runs (
    id              UUID PRIMARY KEY,
    workflow_type   TEXT NOT NULL,
    -- NULL until a worker first picks up the run, at which point it's
    -- pinned to whichever version was latest at that moment (see
    -- Workflow::version). Resumes after a redeploy keep resolving to the
    -- same pinned version, even if a newer one is now registered.
    workflow_version INT,
    status          TEXT NOT NULL DEFAULT 'pending',
    input           JSONB NOT NULL,
    output          JSONB,
    error           JSONB,
    run_after       TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT workflow_runs_status_check
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
);

CREATE INDEX workflow_runs_pending_idx
    ON workflow_runs (run_after)
    WHERE status = 'pending';

CREATE INDEX workflow_runs_locked_idx
    ON workflow_runs (locked_until)
    WHERE status = 'running';

CREATE TABLE workflow_steps (
    run_id          UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_key        TEXT NOT NULL,
    kind            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'waiting',
    output          JSONB,
    error           JSONB,
    attempt         INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 1,
    backoff_base_ms BIGINT NOT NULL DEFAULT 0,
    backoff_factor  REAL NOT NULL DEFAULT 2.0,
    resume_at       TIMESTAMPTZ,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,

    PRIMARY KEY (run_id, step_key),
    CONSTRAINT workflow_steps_kind_check CHECK (kind IN ('step', 'timer')),
    CONSTRAINT workflow_steps_status_check
        CHECK (status IN ('waiting', 'retrying', 'completed', 'failed'))
);
