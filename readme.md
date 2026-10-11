# Distributed? Workflow Orchestrator
A simple orchestrator for executing tasks in a distributed environment. We will take the simple processes of a Ticket Booking System.

## Functional Requirements:
- Accept a task from client.
- Ensure the task is executed and result is sent back to the client.
- Observe the task during its whole workflow duration for (lets just say) state, logs, metrics, events and errors.

## Non-Functional Requirements:
- Each workflow must be unique and idempotent (unless mentioned) for its whole duration.
- Each part/service of a workflow must be carried out in an asynchronous manner (unless explicitly required).
- Each workflow will only move forward after completion of current part/service.
- Each part/service should have a timeout timer for its completion so as not stall the system.
- A workflow can have multiple retries on each part/service.
- Each part/service must be individually scalable, i.e Microservices architecture.
- If any part/service fails in its task of workflow, the Orchestrator engine **must** carry out compensating transactions to undo previous changes.
- Each change should be logged.

## Initial Components and Cycle
![Initial HLD](Initial%20HLD%20of%20Orchestrator.svg)