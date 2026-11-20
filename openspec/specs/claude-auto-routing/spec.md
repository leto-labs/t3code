# Claude Auto routing

## Purpose

Claude Auto is a virtual Claude provider that routes a thread across an explicit pool of the
user's Claude subscription accounts. It uses the same pool policy as Codex Auto
(`codex-auto-routing`); this capability covers what differs for Claude: each account keeps its
conversations in its own config directory, its usage windows include per-model weekly windows,
and Claude Code can hold a limited turn open instead of failing it.

## Design: moving a conversation between accounts

Claude Code stores a session as `<CLAUDE_CONFIG_DIR>/projects/<cwd-slug>/<session>.jsonl`, with
subagent transcripts and tool results in `<session>/` beside it and the task list in
`tasks/<session>/`. Codex Auto shares one session home with separate authentication. Claude cannot
do that safely: a shadow config directory would have to link `.credentials.json`, which Claude
Code replaces by rename when it refreshes a token, so the link breaks and the account's real home
keeps a revoked refresh token. Settings, plugins, MCP configuration and the global config file are
also per account and must stay so.

Claude Auto therefore runs each account in its own, unchanged config directory and copies the
session forward. Before an account resumes a conversation, Auto finds the copy of the session that
was written last among the pool's homes and, when it is not already the target's, copies its
transcript, subagent directory and task list into the target home. The transcript is staged and
renamed into place last. The previous account's process has exited by then, so there is one writer
at a time, and older copies stay where they were.

## Requirements

### Requirement: pool policy shared with Codex Auto

Claude Auto MUST apply the Codex Auto pool requirements to Claude accounts: explicit account pools,
allowance-aware selection from cached usage, reset-aware account order with the per-account turn
cap, sticky routing with quota failover, the 5% low-allowance move at a turn start, members that
are reachable only through Auto (picker, direct sessions and agent paths), pooled usage reporting
for agents, and no implicit credit purchase.

#### Scenario: a member is reached only through Claude Auto

- **WHEN** a Claude account belongs to a Claude Auto pool
- **THEN** the model picker does not offer it and opening a session on it directly fails
- **AND** an agent that names it as a target gets an error that names the Claude Auto instance

### Requirement: Claude windows bind by model

Auto MUST treat a model-scoped weekly window as limiting only its model and the account's weekly
window as limiting every model. The binding window for ordering MUST be the most used of the
weekly windows that limit the requested model. The five-hour session window MUST exclude an
account at 100% but MUST NOT order accounts. Paid extra-usage spend MUST NOT count as allowance.

#### Scenario: Fable and Opus bind differently

- **WHEN** an account has 40% of its weekly window and 10% of its Fable weekly window left
- **THEN** a Fable request binds on the Fable window
- **AND** an Opus request binds on the weekly window and ignores the Fable window

#### Scenario: a spent session window excludes the account

- **WHEN** an account's five-hour window is at 100%
- **THEN** Auto does not select it until that window resets

### Requirement: Claude quota failures are confirmed by a usage read

A Claude usage-limit failure MUST move the conversation only when a usage read taken after the
failure shows an applicable window spent for the requested model.

#### Scenario: a limit error is confirmed before failover

- **WHEN** a turn on account A fails with a usage limit
- **AND** a fresh usage read shows A's weekly window at 100%
- **THEN** the conversation continues on another eligible account without resending the request

### Requirement: held turns fail over

When Claude Code holds a turn open because a usage window is spent, Auto MUST stop that turn and
continue the conversation on another eligible account under the same confirmation rule. When no
account is eligible, the turn MUST end as a usage-limit failure with reset information.

#### Scenario: a paused turn moves to the next account

- **WHEN** account A reports that its turn is paused until a usage limit resets
- **THEN** Auto stops the turn on A and continues it on account B

### Requirement: one writer while a conversation moves

Before another account resumes a conversation, Auto MUST stop the previous account's Claude process
and wait for it to exit. It MUST then copy the most recently written session copy from the pool's
homes into the next account's home, without resending the user's request.

#### Scenario: a committed switch copies the conversation

- **WHEN** a thread moves from account A to account B
- **THEN** A's process exits before B's starts
- **AND** B resumes the same session from a copy of A's transcript

### Requirement: member threads continue through the pool

While an account is a pool member, its conversations MUST be continuable through its Claude Auto
instance, so switching a thread from the member to the pool resumes the native session. After the
account leaves the pool, threads moving from the pool to it MUST start with a context handoff.

#### Scenario: a thread moves from a member to its pool

- **WHEN** a thread ran directly on account Sam before Sam joined a Claude Auto pool
- **AND** the thread is switched to that pool
- **THEN** its next turn resumes Sam's session on the account Auto selects

### Requirement: members bill their own subscription

A member MUST be a direct Claude subscription instance. Auto MUST reject a member that is another
pool or that overrides API keys, the API endpoint or a cloud backend in its environment.

#### Scenario: an API-key member is rejected

- **WHEN** a pool member sets `ANTHROPIC_API_KEY` in its environment
- **THEN** Claude Auto reports an error instead of routing turns

### Requirement: side helpers use a chosen provider

Claude Auto MUST NOT generate thread titles, commit messages or other text. Those helpers use the
provider chosen for text generation.

#### Scenario: text generation is not routed

- **WHEN** text generation is asked of Claude Auto
- **THEN** it fails and asks for a physical provider
