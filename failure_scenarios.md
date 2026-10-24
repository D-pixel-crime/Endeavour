### WHAT ARE THE BEST TIMEOUTS TO KEEP ?
- Cascading rule: DB Timeout < Worker Processing < SQS Visibility < Orchestrator State Timeout.
- Database query timeout: 3–5 seconds.
- Worker processing timeout: 10–15 seconds.
- SQS visibility timeout: 30 seconds (with background heartbeat extension).
- CDC outbox retry window: 15–30 seconds (instead of 2 minutes).
- Orchestrator state timeout: 30–45 seconds (must account for Worker + CDC + SQS queue time + buffer).

---

# Start to Finish
Step-by-step failure scenarios study.

## Generating a UUID:
- Suppose the UUID generation fails.

## Creating a Workflow
- Suppose client sends duplicate UUID.
- Any of the db transaction fails.
- What if client sends request, DB commits, but client disconnects before receiving HTTP 202?
- What if client request payload is missing required fields (e.g. `num_seats`) or has invalid JSON?

## CDC_Service (Since they are always working on same pattern)
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
- Polling a database is expensive, can we get sweet interval for ticker? Can we implement other push/pull based mechanism like transactional log-reading?
- Suppose we have queried and scanned the rows, then service fails. The rows are marked as `processing = true`, but since the service failed, they wont be updated again.
- What if while sending the messages, the service crashed? We wont know whether the messages reached or not?
- What if we know the send has failed, but the service crashes before changing the affected outbox rows?
- What if the service doesnt crash, but the final update to outbox fails?
- What if a message repeatedly fails (poison pill/bad routing)? No max retries or DLQ, so it retries indefinitely.
- On send failure, setting `processing = true, updated_at = NOW()` locks the message out from retry for 2 full minutes.
- What if multiple CDC instances run? Clock skew or a slow instance (> 2 min) can cause dual-dispatch to SQS.
- What if rapid INSERT, UPDATE, DELETE causes PostgreSQL table and index bloat on `outbox`?

## Consumer.ReceiveMessages (same everywhere)
- We read the messages, but run into some of the pre-processing errors.
- What if a poison message fails in a FIFO queue? Head-of-Line (HoL) blocking stalls all subsequent messages in that `MessageGroupId` (workflow).
- What if a message is retried after 5 minutes? AWS SQS FIFO deduplication window is only 5 minutes; it will be treated as a fresh message.
- What if heartbeat visibility extension fails (throttled/network)? Message becomes visible to another worker, causing concurrent duplicate execution.
- What if one message in a batch of 10 fails or crashes? Since delete happens at the end of the batch, the other already-processed messages won't be deleted and will be re-delivered (batch fate-sharing).

## Booking
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
- What if we receive a request for already failed booking workflow ?
- What if we receive a request for pending or already completed booking workflow?
- What if anything in the same transaction fails? Or suppose the service crashes in between those lines of code?
- What if all the DB transactions pass, but:
    - Service crashes before deleting from SQS?
    - Service crashes during delete, we wont know the result?
    - Deletion fails?
- Suppose the worker times out before completion?
- What if a workflow previously FAILED (out of seats), but on redelivery seats are now free? Code flips state from FAILED to COMPLETED.
- What if `seat` table has no `workflow_id` foreign key? We cannot verify seat ownership during compensation/rollback.

## Orchestrator Reading from Booking Egress
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
- What if we pull the messages and then the service crashes?
- What if we pull a message which failed? Orchestrator currently deletes it without transitioning state or triggering compensation.
- What if we pull a message whose workflow has gone past booking now?
- What if we receive a successful message, but that state has already timed-out?
- What if delayed COMPLETED message arrives after Orchestrator already timed out and cancelled? The booked seats are permanently leaked in Booking DB (ghost allocation).
- What if DB transactions go through, but:
    - Server crashes during send, we dont have any info anymore.
    - Push to sqs fails.

## Payment
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
- What if we receive a request for already failed payment workflow ?
- What if we receive a request for pending or already completed payment workflow?
- What if anything in the same transaction fails? Or suppose the service crashes in between those lines of code?
- What if all the DB transactions pass, but:
    - Service crashes before deleting from SQS?
    - Service crashes during delete, we wont know the result?
    - Deletion fails?
- Suppose the worker times out before completion?
- What if payment worker generates a new random `payment_id` on redelivery? Payment gateway will double charge if idempotency key isn't fixed to `workflow_id`.

## Orchestrator Reading from Payment Egress
- We are using concurrent goroutines on pool basis. What if some of goroutines crash or take a long, starving further processing ?
- What if we pull the messages and then the service crashes?
- What if we pull a message which failed? Orchestrator deletes it and never triggers compensation to release booked seats.
- What if we pull a message whose workflow has gone past payment now?
- What if we receive a successful message, but that state has already timed-out?
- What if duplicate payment egress message arrives? Ticket table record is blindly overwritten.
- What if DB transactions go through, but:
    - Server crashes during send, we dont have any info anymore.
    - Push to sqs fails.

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
- What if clock skew exists between Postgres server (`NOW()`) and Go app servers (`time.Now()`)?
- What if worker completes *after* orchestrator has already timed out and marked workflow FAILED?
        
2. What if the worker itself times out on the workflow ?

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

