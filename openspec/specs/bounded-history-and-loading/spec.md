# Bounded history and loading

## Purpose

Thread snapshots limit the historical rows sent over remote connections while
keeping controls actionable.

## Requirements

### Requirement: select records for the history window

The initial thread snapshot MUST select checkpoint scopes by checkpoint IDs
referenced by the history window and by nodes already retained in the snapshot.
Sharing a run with a window item alone does not select a checkpoint scope.

#### Scenario: checkpoint scope owned by older work

- **WHEN** a window item references a checkpoint from an older run
- **THEN** the snapshot includes the checkpoint's scope
- **AND** a scope with no retained node or referenced checkpoint stays out of the snapshot

### Requirement: retain execution and relationship records

Subagent, context transfer, provider turn, and runtime request rows MUST follow
upstream's window selection, which also retains records needed by thread
relationships, execution controls, and the composer. The snapshot MUST also
include provider threads used by live subagents. Plans MUST follow upstream's
behavior and retain all eligible active plans.

#### Scenario: live subagent has a separate provider thread

- **WHEN** a live subagent uses a provider thread outside the visible history
- **THEN** its provider thread is included in the snapshot

### Requirement: pending controls survive the window

Pending user-input questions, approvals, secret forms for live runs, and running
background work MUST be included in the initial bounded snapshot even when they
fall outside the recent history window.

#### Scenario: old controls stay actionable

- **WHEN** a pending question, pending secret form for a live run, or running
  background work is older than the bounded window
- **THEN** the snapshot retains its control without loading older history
- **AND** any pending secret form appears in the visible timeline
