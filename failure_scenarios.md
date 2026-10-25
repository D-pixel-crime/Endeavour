### WHAT ARE THE BEST TIMEOUTS TO KEEP ?
- Cascading rule: DB Timeout < Worker Processing < SQS Visibility < Orchestrator State Timeout.
- Database query timeout: 3–5 seconds.
- Worker processing timeout: 10–15 seconds.
- SQS visibility timeout: 30 seconds (with background heartbeat extension).
- CDC outbox retry window: 15–30 seconds (implemented: 30 seconds, instead of 2 minutes).
- Orchestrator state timeout: 35–45 seconds (implemented: `taskTimeout + 20s`, accounting for Worker + CDC + SQS queue time + buffer).

---

# Start to Finish
Step-by-step failure scenarios study.

## Generating a UUID:
- Suppose the UUID generation fails.
  - **Resolved**: Replaced `uuid.New()` with `uuid.NewRandom()` in `orchestrator/handlers/get_handlers/generateWorkflowID.go`, returning an explicit HTTP 500 error if entropy fails rather than risking runtime panics.

## Creating a Workflow
- Suppose client sends duplicate UUID.
  - **Resolved**: Handled via `SELECT id, workflow_state FROM workflows WHERE id = $1 FOR UPDATE` returning HTTP 208 (`StatusAlreadyReported`) with current state and workflow ID.
- What if client retries rapidly or an attacker floods duplicate check requests, exhausting the database connection pool (`Orchestrator_DB_Pool`)?
  - **Resolved**: Multi-tier defense-in-depth:
    1. **Token Bucket Rate Limiter Middleware** (`orchestrator/middleware/ratelimit.go`): Enforces per-client IP token bucket rate limits (`RATE_LIMIT_POST_RPS`, `RATE_LIMIT_POST_BURST`) with proxy header support (`X-Forwarded-For`, `X-Real-IP`), rejecting floods with `HTTP 429 Too Many Requests` and `Retry-After: 1`.
    2. **In-Flight Concurrency Limiter** (`orchestrator/middleware/concurrency.go`): Bounded semaphore (`MAX_CONCURRENT_WORKFLOW_CREATIONS`) guarantees that no more than $N$ concurrent transactions can execute against PostgreSQL simultaneously, instantly shedding excess load.
    3. **Fast Idempotency Cache & In-Flight Singleflight Lock** (`orchestrator/middleware/idempotency_cache.go`):
       - Recently created or confirmed workflows are cached in-memory (`IdempotencyCache`). Duplicate requests within the TTL return HTTP 208 immediately with zero DB queries or connection overhead.
       - In-flight mutex locks (`TryLock`) prevent concurrent requests with the identical `workflow_id` from racing on DB `FOR UPDATE` row locks or deadlocking.
- Any of the db transaction fails.
  - **Resolved**: Atomic transaction with `defer tx.Rollback(ctx)` rolls back cleanly without persisting partial state.
- What if client sends request, DB commits, but client disconnects before receiving HTTP 202?
  - **Resolved**: Client retrying with the same `workflow_id` receives HTTP 208 with the already committed state and does not create duplicate outbox tasks.
- What if client request payload is missing required fields (e.g. `num_seats`) or has invalid JSON?
  - **Resolved**: Added strict validation in `createWorkflow.go`: `num_seats` must be between 1 and 10; `email`, `full_name`, and `payment_method` must be non-empty. `num_seats` is properly forwarded to the Booking outbox payload.


## CDC_Service (Since they are always working on same pattern)
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
  - **Resolved**: Bounded per-batch execution using `context.WithTimeout(ctx, 25*time.Second)` inside `processBatch`, preventing hung queries or SQS requests from permanently starving the concurrency semaphore.
- Polling a database is expensive, can we get sweet interval for ticker? Can we implement other push/pull based mechanism like transactional log-reading?
  - **Architecture Note**: Can use PostgreSQL `LISTEN/NOTIFY` (instant wakeup trigger) with a fallback ticker, or PostgreSQL Logical Replication (WAL log tailing via Debezium/pgoutput).
- Suppose we have queried and scanned the rows, then service fails. The rows are marked as `processing = true`, but since the service failed, they wont be updated again.
  - **Resolved**: Stale recovery window reduced from 2 minutes to 30 seconds (`INTERVAL '30 seconds'`). Abandoned rows are quickly reclaimed by the next poller cycle.
- What if while sending the messages, the service crashed? We wont know whether the messages reached or not?
  - **Resolved**: SQS FIFO deduplication (`MessageDeduplicationId = outbox_id`) deduplicates within 5 minutes. Downstream workers are idempotent (`ON CONFLICT`).
- What if we know the send has failed, but the service crashes before changing the affected outbox rows?
  - **Resolved**: Same mechanism: 30-second stale interval reclaims the row.
- What if the service doesnt crash, but the final update to outbox fails?
  - **Resolved**: Idempotent re-send: SQS FIFO deduplicates, or downstream workers skip already processed states.
- What if a message repeatedly fails (poison pill/bad routing)? No max retries or DLQ, so it retries indefinitely.
  - **Resolved**: Added `retry_count INT DEFAULT 0` and `status VARCHAR(20) DEFAULT 'PENDING'` to outbox. When `retry_count >= 5`, outbox row is updated to `status = 'FAILED'`, halting infinite retry loops.
- On send failure, setting `processing = true, updated_at = NOW()` locks the message out from retry for 2 full minutes.
  - **Resolved**: On send failure, poller updates outbox with `processing = false, retry_count = retry_count + 1, updated_at = NOW()`, immediately releasing the lock for prompt retry instead of a 2-minute lockout.
- What if multiple CDC instances run? Clock skew or a slow instance (> 2 min) can cause dual-dispatch to SQS.
  - **Resolved**: `FOR UPDATE SKIP LOCKED` prevents concurrent row claims. SQS FIFO deduplication protects against duplicate dispatch.
- What if rapid INSERT, UPDATE, DELETE causes PostgreSQL table and index bloat on `outbox`?
  - **Ops Note**: Tuned via PostgreSQL autovacuum storage parameters:
    `ALTER TABLE outbox SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);`

## Consumer.ReceiveMessages (same everywhere)
- We read the messages, but run into some of the pre-processing errors.
  - **Resolved**: Malformed JSON or missing workflow ID messages are logged as poison pills and acknowledged to prevent FIFO head-of-line blocking.
- What if a poison message fails in a FIFO queue? Head-of-Line (HoL) blocking stalls all subsequent messages in that `MessageGroupId` (workflow).
  - **Resolved**: Unrecoverable/malformed payload errors are acknowledged or routed away from the queue so they do not block subsequent messages in the FIFO group.
- What if a message is retried after 5 minutes? AWS SQS FIFO deduplication window is only 5 minutes; it will be treated as a fresh message.
  - **Resolved**: Application-level idempotency via `SELECT workflow_state ... FOR UPDATE` and unique constraints (`uq_payments_workflow_id`, `uq_tickets_workflow_id`).
- What if heartbeat visibility extension fails (throttled/network)? Message becomes visible to another worker, causing concurrent duplicate execution.
  - **Resolved**: DB row locks (`FOR UPDATE`) ensure only one worker progresses; subsequent worker detects `COMPLETED` state and skips execution.
- What if one message in a batch of 10 fails or crashes? Since delete happens at the end of the batch, the other already-processed messages won't be deleted and will be re-delivered (batch fate-sharing).
  - **Resolved**: Wrapped each message processing invocation in an isolated `func() { defer func() { recover() }(); ... }` block, preserving all successfully processed message IDs for deletion even if a sibling message panics.

## Booking
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
  - **Resolved**: Protected by semaphore-bounded goroutine pool with `recover()` blocks and process context timeouts.
- What if we receive a request for already failed booking workflow ?
  - **Resolved**: Checks `if existingState == FAILED || existingState == COMPENSATING` and immediately skips/acknowledges without re-processing.
- What if we receive a request for pending or already completed booking workflow?
  - **Resolved**: Skips duplicate processing if `COMPLETED`; row lock serializes pending concurrent attempts.
- What if anything in the same transaction fails? Or suppose the service crashes in between those lines of code?
  - **Resolved**: Seat reservation, workflow state update, and outbox insertion are executed in a single atomic database transaction (`tx`).
- What if all the DB transactions pass, but:
    - Service crashes before deleting from SQS?
    - Service crashes during delete, we wont know the result?
    - Deletion fails?
  - **Resolved**: On redelivery, worker reads `existingState == COMPLETED`, skips processing, and deletes the message from SQS.
- Suppose the worker times out before completion?
  - **Resolved**: `processCtx` cancellation rolls back the database transaction cleanly.
- What if a workflow previously FAILED (out of seats), but on redelivery seats are now free? Code flips state from FAILED to COMPLETED.
  - **Resolved**: Guarded with explicit check: if `existingState == BOOKING_WORKFLOW_FAILED`, worker skips and refuses state regression.
- What if `seat` table has no `workflow_id` foreign key? We cannot verify seat ownership during compensation/rollback.
  - **Resolved**: Added `booked_by_workflow_id UUID` column and index to `seat` table.

## Orchestrator Reading from Booking Egress
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
  - **Resolved**: Isolated panic recovery and bounded batch processing.
- What if we pull the messages and then the service crashes?
  - **Resolved**: SQS visibility timeout will expire, redelivering the message safely.
- What if we pull a message which failed? Orchestrator currently deletes it without transitioning state or triggering compensation.
  - **Resolved**: Orchestrator transitions workflow to `FAILED` and records failure payload.
- What if we pull a message whose workflow has gone past booking now?
  - **Resolved**: Ignored if not in `BOOKING` state (state regression prevention).
- What if we receive a successful message, but that state has already timed-out?
- What if delayed COMPLETED message arrives after Orchestrator already timed out and cancelled? The booked seats are permanently leaked in Booking DB (ghost allocation).
  - **Resolved**: If a `COMPLETED` message arrives but workflow is in `FAILED` or non-booking state, Orchestrator immediately emits a compensation outbox event to `BOOKING` worker (`action: "COMPENSATE"`, `seat_ids: egressPayload.SeatIDs`) to release the ghost seat allocation.
- What if DB transactions go through, but:
    - Server crashes during send, we dont have any info anymore.
    - Push to sqs fails.
  - **Resolved**: Outbox pattern guarantees message is persisted and retried by CDC poller.

## Payment
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
  - **Resolved**: Handled via bounded pool and panic isolation.
- What if we receive a request for already failed payment workflow ?
  - **Resolved**: Checks `if existingState == FAILED || existingState == COMPENSATING` and immediately skips/acknowledges without re-processing.
- What if we receive a request for pending or already completed payment workflow?
  - **Resolved**: Skips duplicate processing if `COMPLETED`; row lock serializes pending attempts.
- What if anything in the same transaction fails? Or suppose the service crashes in between those lines of code?
  - **Resolved**: Payment record, workflow state update, and outbox insertion are executed in a single atomic database transaction.
- What if all the DB transactions pass, but:
    - Service crashes before deleting from SQS?
    - Service crashes during delete, we wont know the result?
    - Deletion fails?
  - **Resolved**: On redelivery, worker reads `existingState == COMPLETED`, skips processing, and deletes the message from SQS.
- Suppose the worker times out before completion?
  - **Resolved**: `processCtx` cancellation rolls back transaction cleanly.
- What if payment worker generates a new random `payment_id` on redelivery? Payment gateway will double charge if idempotency key isn't fixed to `workflow_id`.
  - **Resolved**: `payment_id` is derived deterministically from the workflow UUID using `uuid.NewSHA1(uuid.NameSpaceOID, []byte("payment:"+workflowUUID.String()))`, ensuring identical payment gateway idempotency across all retries.

## Orchestrator Reading from Payment Egress
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
  - **Resolved**: Handled via batch panic recovery and bounded poller concurrency.
- What if we pull the messages and then the service crashes?
  - **Resolved**: SQS visibility timeout will expire, redelivering the message safely.
- What if we pull a message which failed? Orchestrator deletes it and never triggers compensation to release booked seats.
  - **Resolved**: Orchestrator initiates backward compensation (`COMPENSATING_PAYMENT` -> `COMPENSATING_BOOKING` -> `FAILED`).
- What if we pull a message whose workflow has gone past payment now?
  - **Resolved**: Guarded by `wf.WorkflowState != ORCHESTRATOR_WORKFLOW_PAYMENT`.
- What if we receive a successful message, but that state has already timed-out?
  - **Resolved**: Guarded by workflow state checks.
- What if duplicate payment egress message arrives? Ticket table record is blindly overwritten.
  - **Resolved**: Workflow state check skips ticket creation if already past `PAYMENT` state, and `tickets` table uses `ON CONFLICT (workflow_id)` idempotency.
- What if DB transactions go through, but:
    - Server crashes during send, we dont have any info anymore.
    - Push to sqs fails.
  - **Resolved**: Handled by transactional outbox pattern.

## Compensating Transactions / Saga Rollbacks
- What if Payment fails: who triggers the compensation to cancel the booking?
    - **Implemented**: If Payment fails (after 2 internal retries), it publishes `status = 'FAILED'` to outbox. CDC forwards it to Orchestrator Payment Egress. Orchestrator initiates backward compensation starting from Payment (`COMPENSATING_PAYMENT`), where Payment worker checks `payments` table (refunds if money was captured) and returns `status = 'COMPENSATED'`. Orchestrator then transitions to `COMPENSATING_BOOKING`, sends compensation to Booking to release seats, and once Booking confirms `COMPENSATED`, Orchestrator marks workflow `FAILED`.
- What if a compensation message fails to push to SQS or the worker crashes during compensation?
    - Outbox pattern and CDC ensure compensation messages are reliably delivered. SQS FIFO message redelivery with idempotent compensation handlers guarantees at-least-once execution.
- What if compensation action (releasing seats) arrives out of order before the original booking committed?
    - If workflow state is already `FAILED` or `COMPENSATING_BOOKING`, delayed `COMPLETED` booking egress messages are rejected and not advanced to payment.
- What if compensation unbooks a seat that was already assigned to another customer?
    - **Implemented**: `seat` table includes `booked_by_workflow_id UUID`. Compensation executes `UPDATE seat SET is_booked = false, booked_by_workflow_id = NULL WHERE booked_by_workflow_id = $1`, guaranteeing seats owned by other workflows can never be unbooked.
- What if payment succeeded, but ticket creation in Orchestrator failed? How is refund executed?
    - Payment worker supports `action: "COMPENSATE"`, checking `payments` table for captured money and setting status to `'REFUNDED'`. Orchestrator can trigger `COMPENSATING_PAYMENT` followed by `COMPENSATING_BOOKING`.

## Timeouts related scenarios:
1. Suppose the orchestrator times out on booking or payment.
	- Above two can be due to various reasons:
		- The worker could not complete the task
		- The worker completed the task, but could not push, etc.
        - The cdc_service fails to read properly from the outbox.
- What if there is no background sweeper/reaper daemon checking `state_timeout < NOW()`? Workflows never time out automatically in DB.
  - **Implemented**: Created dedicated standalone `sweeper_service` microservice (`sweeper_service/` package). It polls `workflows WHERE state_timeout IS NOT NULL AND state_timeout < NOW() FOR UPDATE SKIP LOCKED` on an independent interval:
    - Automatically marks abandoned `BOOKING` workflows as `FAILED`.
    - Automatically initiates backward compensation for abandoned `PAYMENT` workflows by updating state to `COMPENSATING_PAYMENT` and writing a compensation task to `outbox`.
    - Handles multi-stage compensation timeouts (escalating from `COMPENSATING_PAYMENT` to `COMPENSATING_BOOKING` to guarantee booked seats are released).
    - Runs as a dedicated single-replica service, eliminating multi-instance lock contention, uncoordinated goroutine chaos, and unnecessary DB network calls from scaled Orchestrator API pods.
- What if clock skew exists between Postgres server (`NOW()`) and Go app servers (`time.Now()`)?
  - **Resolved**: Query checks `WHERE state_timeout < NOW()` directly inside PostgreSQL engine, making Postgres server time the single source of truth and immune to app server clock skew.
- What if worker completes *after* orchestrator has already timed out and marked workflow FAILED?
  - **Resolved**: Ghost allocation prevention in Booking Egress handler detects that workflow is in `FAILED` state and immediately emits a compensation outbox event to release the seats in Booking DB.
        
2. What if the worker itself times out on the workflow ?
  - **Resolved**: Context timeout aborts the worker's DB transaction and stops the visibility heartbeat, allowing SQS retry or DLQ capture.

## Overall:
- What are the scenarios where `workflow_id` can fail as an idempotency parameter?
    - Entity-level conflicts (e.g. seat-level race vs workflow-level lock).
    - State regression on redelivery (old step message overwriting newer step state).
    - External payment gateways requiring explicit, consistent idempotency headers.
- What are the ways to maintain idempotency?
    - Database unique constraints and atomic conditional updates (`WHERE status = EXPECTED`).
    - Deterministic idempotency keys across all retries.
    - Dead-letter queues and poison pill isolation.

## Additional Questions:
- How will we serve different regions or locations ?
    - SQS FIFO is single-region only; requires regional queue partitions and worker pools.
    - Train and seat data can be partitioned by region/station (cell-based architecture).
    - Multi-region DB failover/split-brain risks losing un-replicated outbox events.
