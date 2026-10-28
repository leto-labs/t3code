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

### Requirement: sticky routing with quota failover

Auto MUST keep an existing thread on its selected account while that account is
usable. When a confirmed allowance failure occurs, Auto MUST move the native
conversation to another eligible account without resending the original request.

A quota rejection MUST refresh that account's usage, and the request MUST move
to an account that has not rejected it yet. A rate-limit error counts as a quota
failure only when a usage read taken after it shows the allowance is spent.

#### Scenario: confirmed quota failure fails over

- **WHEN** account A returns a confirmed allowance failure and account B supports the request
- **THEN** Auto refreshes account A's usage
- **AND** the same native conversation continues on account B
- **AND** the original request is not sent a second time

#### Scenario: an account rejects despite reported allowance

- **WHEN** account A's usage shows allowance but Codex rejects the request for quota
- **THEN** Auto retries on the other accounts without returning to A for that request
- **AND** later requests may select A again once its usage is read

### Requirement: pool members are not directly selectable

An account that belongs to an Auto pool MUST NOT be selectable or openable as a
direct provider, so two routes cannot write the same native conversation.

#### Scenario: pool member is hidden from the picker

- **WHEN** a Codex account is a member of an Auto pool
- **THEN** the model picker does not offer it directly
- **AND** opening a session on it directly fails with a clear error

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
