CREATE TYPE workflow_type as ENUM (
    'TICKET_BOOKING'
);

CREATE TYPE workflow_state as ENUM (
    'INITIALIZED',
    'PENDING',
    'BOOKING',
    'PAYMENT',
    'COMPLETED',
    'FAILED',
    'COMPENSATING'
    );

CREATE TYPE worker_type as ENUM(
    'BOOKING',
    'PAYMENT'
    );

CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    workflow_type workflow_type NOT NULL,
    workflow_state workflow_state NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP,
    state_timeout TIMESTAMP
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    worker_type worker_type NOT NULL,
    payload JSONB NOT NULL,
    timeout_seconds INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE
);

CREATE INDEX idx_outbox_created_at ON outbox(created_at);