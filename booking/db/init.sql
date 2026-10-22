CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE booking_workflow_state as ENUM (
    'PENDING',
    'COMPLETED',
    'FAILED',
    'COMPENSATING'
    );

CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    workflow_state booking_workflow_state NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP
);

CREATE TYPE worker_type as ENUM(
    'BOOKING'
    );

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    worker_type worker_type NOT NULL,
    payload JSONB NOT NULL,
    timeout_seconds INT NOT NULL DEFAULT 15,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processing BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (workflow_id) references workflows(id) ON DELETE CASCADE
);

CREATE TABLE seat (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    train_id VARCHAR(255) NOT NULL,
    seat_number VARCHAR(255) NOT NULL,
    cost INT NOT NULL DEFAULT 500,
    is_booked BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT uq_train_seat UNIQUE (train_id, seat_number)
);

CREATE INDEX idx_outbox_updated_at ON outbox(updated_at);
CREATE INDEX idx_seat_available ON seat(train_id, seat_number) WHERE is_booked = false;

-- Initial Seed of 100 Seats

INSERT INTO seat (train_id, seat_number)
SELECT
    'TRAIN-1',
    'SEAT-' || LPAD(s::text, 5, '0')
FROM generate_series(1, 10000) AS s;