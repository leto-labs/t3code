# Spawn broker

## Purpose

On Linux, forking a child process copies the parent's page tables on the calling
thread. Short-lived commands (git, gh, ps, systemctl) spawned from a large
server froze its event loop for seconds during polling bursts. A small broker
process forks those commands so the server stays responsive.

## Requirements

### Requirement: short-lived commands fork outside the server

On Linux, git, gh, ps, and systemctl commands that the broker can represent
exactly MUST be spawned by the broker, with stdin, stdout, stderr, exit codes,
and kill semantics equivalent to the default spawner. Anything else MUST use the
default spawner.

#### Scenario: polling burst does not stall clients

- **WHEN** the server refreshes VCS status for many repositories at once
- **THEN** client round trips show no multi-second stalls
- **AND** the git commands run as children of the broker

### Requirement: no duplicate execution

A command that may have reached the broker MUST NOT be re-run through the
default spawner. Only requests the broker never read may fall back. Commands
running when the broker dies MUST fail and have their process groups killed.

#### Scenario: broker dies mid-command

- **WHEN** the broker exits while a command is running
- **THEN** that command reports failure and is not run again
- **AND** later commands use the default spawner for a cooldown period

### Requirement: operator kill switch

`T3CODE_SPAWN_BROKER=0` MUST disable the broker without a rebuild.

#### Scenario: broker disabled

- **WHEN** the server starts with `T3CODE_SPAWN_BROKER=0`
- **THEN** every command uses the default spawner
