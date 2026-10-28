# Background refresh controls

## Purpose

Repository and pull-request refreshes are auxiliary work. Users must be able to
disable them when a remote repository is slow, unreachable, or not relevant to
their workflow.

## Requirements

### Requirement: one setting controls background PR refresh

The server MUST expose an environment setting that disables all automatic
pull-request refresh and watch work for that environment.

#### Scenario: setting disables refresh

- **WHEN** the environment setting is disabled
- **THEN** all automatic pull-request refresh and watch work is disabled
- **AND** the value is reflected in settings

### Requirement: disabled means no background fetch

When the setting is disabled, background reactors MUST not start remote fetches,
pull-request lookups, or automatic pulls solely to refresh sidebar metadata.
Manual pull-request commands and refreshes MUST keep working, and pull requests
already known to a thread MUST stay visible. If settings cannot be read, refresh
behaves as enabled.

#### Scenario: disabled refresh does not fetch

- **WHEN** background refresh is disabled and the sidebar loads
- **THEN** no remote fetch, pull-request lookup, or automatic pull starts
- **AND** local thread and workspace state remains available

### Requirement: setting is visible and reversible

The web settings UI MUST expose the current value and allow it to be changed.
Turning the setting back on MUST resume future refresh work without requiring a
new environment or thread.

#### Scenario: refresh is re-enabled

- **WHEN** the user enables background refresh again
- **THEN** subsequent refresh cycles may run
- **AND** no new environment or thread is required

### Requirement: unrelated work remains usable

Disabling background refresh MUST NOT prevent normal thread turns, local status,
or other environment operations from working.

#### Scenario: normal work continues while disabled

- **WHEN** background refresh is disabled
- **THEN** normal thread turns and local status continue to work
- **AND** the setting can be reversed without restarting the server
