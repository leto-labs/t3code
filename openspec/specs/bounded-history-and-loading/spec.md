# Bounded history and loading

## Purpose

Thread snapshots must stay small over remote connections even when a thread has
large messages or tool output, and that bound must never hide something the
user has to act on.

## Requirements

### Requirement: snapshots bounded by size

The initial thread snapshot MUST be bounded by encoded size as well as by row
count, counting message bodies, so a few very large items cannot make it carry
megabytes.

#### Scenario: thread with very large messages opens remotely

- **WHEN** a thread's recent rows hold more bytes than the snapshot limit
- **THEN** the snapshot includes fewer rows and stays under the limit
- **AND** the response still has paging metadata for the older rows

### Requirement: pending controls survive the window

Pending user-input questions, approvals, secret forms, running background work,
and the active plan of each live run MUST be included in every bounded snapshot
even when they fall outside the recent history window, so an "Input" or
"Working" state is always actionable.

#### Scenario: old question stays answerable

- **WHEN** a thread's pending question is older than the bounded window
- **THEN** the initial snapshot still includes the question
- **AND** the user can answer it without loading older history

### Requirement: large items arrive as previews

An item larger than the per-item wire limit MUST be sent as a preview marked as
omitted detail. The client MUST fetch that one item's full detail only when the
user asks for it, without reloading the thread snapshot.

#### Scenario: large tool output is expanded

- **WHEN** the user expands a tool call whose output was omitted
- **THEN** only that item's detail is fetched
- **AND** the rest of the timeline stays mounted and unchanged

### Requirement: plans load on demand

A plan body omitted from the snapshot MUST load through its own request. The
plan's actions MUST stay available while it loads, and a failed load MUST offer
a retry without resetting the thread.

#### Scenario: plan detail fails to load

- **WHEN** loading a plan's full body fails
- **THEN** the plan card shows the failure with a retry
- **AND** the visible thread stays intact
