CREATE TYPE workflow_type as ENUM (
    'TICKET_BOOKING'
);

CREATE TYPE orchestrator_workflow_state as ENUM (
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
    workflow_state orchestrator_workflow_state NOT NULL,
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
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processing BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE
);

CREATE INDEX idx_outbox_updated_at ON outbox(updated_at);

CREATE TABLE tickets (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    seat_ids TEXT[] NOT NULL,
    amount INT NOT NULL,
    payment_id VARCHAR(255),
    customer_name VARCHAR(255),
    customer_email VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'CONFIRMED',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE,
    CONSTRAINT uq_tickets_workflow_id UNIQUE (workflow_id)
);

CREATE INDEX idx_tickets_workflow_id ON tickets(workflow_id);