# Safeguards session renewal

## Purpose

A provider can flag a whole native session through its safeguards (Claude: "safeguards flagged
this session"), after which it refuses every later turn on that session. T3 renews the session
inside the same thread, so the user keeps the thread's identity, links and history, instead of
moving the work to a new thread by hand.

## Design

Renewal reuses the rejected-history recovery path that Codex `invalid_encrypted_content` uses: the
next turn opens a fresh native session on the same provider thread and delivers a full-thread
summary first. Refused runs are left out of every recovery summary, so the flagged content is not
replayed. A renewal is tried once per refusal chain; the run that received the summary being
refused again means the conversation itself is flagged, and the user continues in a new thread.

## Requirements

### Requirement: a refused session is renewed in the same thread

When the latest delivered run on a thread's native provider session failed with a safeguards
refusal (`model_refusal`), the next turn MUST NOT resume that native session. It MUST start a fresh
native session for the same T3 thread and provider thread and deliver a full-thread summary first.
The thread's title, pin, worktree, scheduled-task bindings and history MUST stay unchanged, and the
thread MUST show a marker that names the renewal. The refused request MUST NOT be resent.

#### Scenario: the next prompt continues on a fresh session

- **WHEN** a Claude turn fails with a safeguards refusal
- **AND** the user sends another prompt in the same thread
- **THEN** the prompt runs on a new native session of the same provider thread
- **AND** the thread shows "Session renewed after a safeguards refusal"

#### Scenario: other provider errors keep the session

- **WHEN** a turn fails with a provider error that is not a refusal
- **THEN** the next turn resumes the same native session

### Requirement: the renewal summary leaves out refused runs

The renewal summary MUST include the thread's earlier conversation and MUST exclude every run that
ended in a refusal: its user message, its tool and assistant items and the refusal text.

#### Scenario: the flagged prompt is not replayed

- **WHEN** a session is renewed after a refused run
- **THEN** the summary contains the earlier turns
- **AND** it contains neither the refused prompt nor the refusal message

### Requirement: one renewal per refusal chain

When the first run on a renewed session is refused again, T3 MUST NOT renew again. Every later
turn start on that thread MUST fail with an error that tells the user to continue in a new thread.

#### Scenario: a second refusal stops renewal

- **WHEN** the run that received a renewal summary is refused
- **THEN** the next prompt fails without opening another native session
- **AND** the error tells the user to continue in a new thread

### Requirement: refusals do not move Auto accounts

Claude Auto MUST NOT treat a refusal as a usage-limit failure or switch accounts because of it. A
renewed session MUST start on the account the pool policy selects, without copying the refused
session's transcript.

#### Scenario: a refusal on an Auto member

- **WHEN** a Claude Auto turn on account A is refused
- **THEN** Auto records no quota failover for A
- **AND** the next prompt opens a fresh session on the account Auto selects
