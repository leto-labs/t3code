# Live voice

## Purpose

Live voice adds a Codex realtime conversation to the same canonical T3 thread.
The browser owns the low-latency media path while T3 remains authoritative for
thread identity, signaling, transcripts, handoffs, and check-ins.

## Requirements

### Requirement: one canonical thread

Starting voice MUST use the selected T3 thread. On a new draft, T3 MUST create
the thread through the same launch path as the first text send, including
workspace, worktree, and setup preparation, without a placeholder text turn.

#### Scenario: voice starts on a draft

- **WHEN** the user starts voice on a draft in New worktree mode
- **THEN** T3 launches the thread with its worktree and setup
- **AND** negotiates the call without sending a message

### Requirement: server-owned sessions

The server MUST keep one call per thread for its own lifetime, independent of
client connections, and MUST keep the provider session and native thread loaded
while a call is live. A stale native thread MUST be recreated before retrying.

#### Scenario: client reconnects

- **WHEN** the T3 connection drops and returns during a call
- **THEN** the client receives a snapshot of live calls and undelivered handoff results
- **AND** the call continues without renegotiating media

### Requirement: exclusive focus with real hold

Several calls MAY stay connected, but at most one per environment has the
microphone and speaker. Focusing or starting another call MUST hold the previous
one. Hold MUST keep the transport connected, mute both directions, and record
remote audio into a bounded buffer that resume replays before going live. When
recording is unavailable, hold MUST still mute and report that nothing will be
replayed. Environments MUST NOT affect each other.

#### Scenario: a held call answers

- **WHEN** a held call's remote side speaks
- **THEN** the client shows a waiting response
- **AND** resume plays the recording, then the live stream

### Requirement: transcripts

Partial phrases MUST be shown live and never persisted. Finished phrases MUST be
persisted as runless thread rows, each with its own id, without starting a turn,
and MUST be carried into the context handoff of a later provider switch.

#### Scenario: a phrase finishes

- **WHEN** the user finishes a spoken phrase during a call
- **THEN** the phrase is saved as its own thread row without starting a turn
- **AND** a later provider switch carries it in the handoff context

### Requirement: handoffs and check-ins

A handoff MUST run as one normal run on the thread using the run settings the
client last configured: provider, model, options, permissions, and interaction
mode. The settings MUST be snapshotted when the handoff starts and MUST change
without restarting media. The run's copy of the spoken request MUST be hidden.
Handoff results and finished native subagents MUST be published once to the
owning client, which forwards them to the call even while it is held.

#### Scenario: a handoff finishes while the call is held

- **WHEN** a handoff run ends while its call is on hold
- **THEN** the result is delivered to the owning client once
- **AND** the client forwards it to the held call

### Requirement: text while live

Sending text during a call MUST hold the call until that turn has run and ended,
then resume it.

#### Scenario: user types during a call

- **WHEN** the user sends a text message while a call is live
- **THEN** the call is held while that turn runs
- **AND** it resumes when the turn ends

### Requirement: recovery

WebSocket or media loss MUST surface a recoverable state. A media connection that
does not recover within a short window MUST end the call so it can be restarted.

#### Scenario: media does not come back

- **WHEN** the media connection drops and does not recover in time
- **THEN** the call ends and shows that it can be restarted
