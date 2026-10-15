CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE payment_workflow_state as ENUM (
    'PENDING',
    'COMPLETED',
    'FAILED',
    'COMPENSATING'
);

CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    workflow_state payment_workflow_state NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP
);

CREATE TYPE worker_type as ENUM(
    'PAYMENT'
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    worker_type worker_type NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE
);

CREATE TABLE payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL,
    amount INT NOT NULL,
    payment_method VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE
);

CREATE INDEX idx_outbox_created_at ON outbox(created_at);
CREATE INDEX idx_payments_workflow_id ON payments(workflow_id);
