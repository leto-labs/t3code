# Pending sends

## Purpose

A message the user sent must stay visible until the server has it, even while
the user switches threads or the server is slow.

## Requirements

### Requirement: pending sends survive navigation

Sent and queued messages that the server has not acknowledged MUST remain
visible in their thread across thread switches and view remounts.

#### Scenario: user switches away and back

- **WHEN** the user sends a message, opens another thread, and returns before the server copy arrives
- **THEN** the message is still visible once
- **AND** it is replaced by the server copy without duplication

### Requirement: acknowledgement rule

A pending message MUST retire only when its user-message turn item or its
user-queued run appears, when it leaves an observed server queue, or when the
send fails.

#### Scenario: queued message is cancelled elsewhere

- **WHEN** a queued message disappears from the server queue after being observed
- **THEN** its pending row is removed and its preview resources are released

### Requirement: delivery-safe retry

A send interrupted by a transport failure MUST retry with the same command id
so the server deduplicates it, and MUST NOT restore an already accepted message
into the composer.

#### Scenario: response lost after acceptance

- **WHEN** the server accepted a send but the client lost the response
- **THEN** the thread shows the message once and the composer is not refilled
