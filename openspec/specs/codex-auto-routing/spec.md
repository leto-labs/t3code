# Codex Auto routing

## Purpose

Codex Auto is a virtual provider that routes a thread across an explicit pool of
existing Codex subscription accounts without purchasing credits or changing the
thread's requested model and service tier.

## Requirements

### Requirement: explicit account pools

The system MUST let an operator define which configured Codex accounts belong to
an Auto pool. An empty or disabled pool MUST remain unavailable to the composer.

#### Scenario: empty pool is unavailable

- **WHEN** an Auto pool has no enabled accounts
- **THEN** the pool is excluded from the composer
- **AND** no thread is routed through it

### Requirement: allowance-aware selection

Auto MUST select only accounts that support the requested model and service tier
and have usable allowance data. Selection MUST use each account's last reported
allowance and MUST NOT wait on a usage probe while any account has allowance;
usage refreshes in the background. An account with no usage data MUST NOT be
treated as available capacity. A failed probe keeps the last reported allowance.
Auto MUST NOT keep a separate cooldown that outlives the account's reported usage.

#### Scenario: cached allowance routes without a probe

- **WHEN** an account's last reported usage shows allowance, however old
- **THEN** Auto can select it without probing usage first
- **AND** native admission still verifies the account before inference

#### Scenario: allowance returns before the reported reset

- **WHEN** every account's last reported usage is exhausted
- **AND** a fresh usage read shows an account has allowance again (for example a banked reset)
- **THEN** Auto rereads usage before failing and selects that account

#### Scenario: no usage data is excluded

- **WHEN** an account has never reported usage, or its usage is unavailable
- **THEN** Auto excludes that account from selection
- **AND** the user sees an actionable unavailable-account state

### Requirement: reset-aware account order

For a new assignment or a failover, Auto MUST order eligible accounts by the reset
time of their binding long window: the weekly or monthly window that applies to
the requested model, including a model-specific one, with the least remaining.
The earliest reset goes first, because allowance left at a reset is lost. Ties
MUST go to fewer running turns, then more remaining allowance. Accounts without
reset data MUST come after accounts with one, ordered by remaining allowance.

#### Scenario: soonest-expiring allowance is spent first

- **WHEN** account A has 8% of its weekly window left and resets in 32 hours
- **AND** account B has 98% left and resets in 6 days
- **THEN** Auto assigns a new thread to account A

#### Scenario: no reset data falls back to remaining allowance

- **WHEN** no eligible account reports a reset time for its binding window
- **THEN** Auto prefers the account with more remaining allowance

### Requirement: per-account turn cap

An account already running the pool's per-account turn cap (default 3) MUST be
skipped for new assignments while another eligible account is below the cap.
When every account is at the cap, the reset-aware order applies to all of them.

#### Scenario: a busy account spills new threads over

- **WHEN** the earliest-resetting account is running as many turns as the pool's cap
- **AND** another eligible account is below the cap
- **THEN** Auto assigns a new thread to the other account

### Requirement: sticky routing with quota failover

Auto MUST keep a thread on its account while it is usable, since switching loses
the prompt cache. On a confirmed allowance failure, Auto MUST move the native
conversation to another eligible account without resending the original request.
A quota rejection MUST refresh that account's usage and move the request to an
account that has not rejected it. A rate-limit error counts as a quota failure
only when a usage read taken after it shows the allowance is spent.

#### Scenario: confirmed quota failure fails over

- **WHEN** account A returns a confirmed allowance failure and account B supports the request
- **THEN** Auto refreshes account A's usage
- **AND** the same native conversation continues on account B
- **AND** the original request is not sent a second time

#### Scenario: an account rejects despite reported allowance

- **WHEN** account A's usage shows allowance but Codex rejects the request for quota
- **THEN** Auto retries on the other accounts without returning to A for that request
- **AND** later requests may select A again once its usage is read

### Requirement: low-allowance threads move at a turn start

At a turn start, a thread MUST move off its account when that account has less
than 5% of its binding window left and another eligible account has at least 5%.

#### Scenario: a nearly spent account releases its threads

- **WHEN** a thread's account has 4% of its binding weekly window left
- **AND** another eligible account has 40% left
- **THEN** the thread's next turn runs on the other account

### Requirement: pool members are not directly selectable

An account that belongs to an Auto pool MUST NOT be selectable or openable as a
direct provider, so two routes cannot write the same native conversation.

#### Scenario: pool member is hidden from the picker

- **WHEN** a Codex account is a member of an Auto pool
- **THEN** the model picker does not offer it directly
- **AND** opening a session on it directly fails with a clear error

### Requirement: agents reach pool members only through Auto

Agent-facing paths MUST treat a pool member as reachable only through its Auto
instance. `orchestrator_capabilities` MUST NOT list pool members. Delegated tasks,
created and launched threads, thread configuration, and scheduled-task creation
MUST reject a pool member as their target with an error that names the Auto
instance to use, and a driver-only target MUST NOT resolve to a pool member.

#### Scenario: an agent names a pool member

- **WHEN** an agent delegates a task to a Codex account that belongs to an Auto pool
- **THEN** the request fails before any thread is created
- **AND** the error names the Auto instance to select instead

### Requirement: pooled usage reporting for agents

The agent usage report MUST describe an Auto pool as one entry, without member
entries of their own. The entry MUST be available when any eligible member has
allowance for one of the pool's models. Each window kind and model MUST report the
best member's remaining percentage and the soonest reset, with a read-only
per-member breakdown. Provider snapshots MUST keep each member's own limits and
no pooled limits on the Auto instance.

#### Scenario: one exhausted member does not hide pool capacity

- **WHEN** account A has spent its weekly window and account B has 60% left
- **THEN** the Auto entry is available with 60% weekly remaining
- **AND** its weekly window reports account A's reset as the soonest reset
- **AND** the breakdown shows why account A is not used

### Requirement: safe native account switching

Switching accounts MUST unload the thread on the previous account and wait for
Codex to confirm the native writer is released before resuming on the next
account. A committed switch MUST preserve the conversation identity and MUST
not leave two native writers active for one thread.

#### Scenario: committed account switch releases the old writer

- **WHEN** a thread changes from account A to account B
- **THEN** the writer for account A is released before account B is acquired
- **AND** the conversation identity remains unchanged

### Requirement: no implicit credit purchase

Auto MUST report exhaustion and reset information when no eligible account is
available. It MUST NOT redeem or purchase credits as part of recovery.

#### Scenario: all accounts are exhausted

- **WHEN** every eligible account reports exhausted allowance
- **THEN** the thread reports reset information and waits
- **AND** Auto does not redeem or purchase credits
