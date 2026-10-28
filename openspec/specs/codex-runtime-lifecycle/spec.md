# Codex runtime lifecycle

## Purpose

Codex threads can move between app-server processes (account switches, idle
unloads, crashes). T3 must never leave two native writers on one conversation
and must recover from an app-server that exited.

## Requirements

### Requirement: unload releases the native writer

Unloading a native thread MUST unsubscribe and, when Codex reports it was
subscribed, wait for its closed notification before another process resumes
the thread. A detach MUST keep the previous writer tracked until the unload
succeeds.

#### Scenario: thread resumes on another process

- **WHEN** a thread is unloaded from one Codex process and resumed in another
- **THEN** the resume starts only after the first process released the writer

### Requirement: dead app-servers are not reused

When a Codex app-server exits, its event stream MUST fail and its runtime MUST
be reopened instead of reused by later turns, including when replacing its
process failed.

#### Scenario: app-server exits between turns

- **WHEN** the app-server process exits cleanly while idle
- **THEN** the next turn opens a new runtime instead of failing on the dead one
