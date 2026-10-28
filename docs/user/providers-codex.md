# Codex

Use your ChatGPT plan or an existing Codex CLI login to run Codex in T3 Code.

## Connect with ChatGPT

Connect during onboarding or in **Settings → Providers** (select your target environment first for remote machines). T3 Code manages the Codex runtime and signs you in via OpenAI.

- Manage shared usage and credits through **Manage usage** in T3 Code.
- **Remote sign-in**: If sign-in doesn't return automatically, paste the full `localhost` redirect URL into the sign-in panel.
- **Speed options**: Fast and Ultrafast speeds are not available through ChatGPT sharing—[use an existing Codex login](#use-an-existing-codex-login) instead.
- Disconnecting stops running threads while preserving history.

## Use an existing Codex login

Run `codex login` on the environment's machine, then add a Codex instance in **Settings → Providers** via **Configure manually**. See [provider setup](./install.md#providers) for installation details.

If provider status checks time out on a slower machine, set `T3CODE_CODEX_PROBE_TIMEOUT_MS=60000` in the server environment and restart. See [Provider check timeouts](./install.md#provider-check-timeouts).

## Use multiple accounts

Add another ChatGPT account in **Settings → Providers**, or configure multiple CLI logins with a **shadow home** so work and personal accounts can continue the same threads:

```bash
mkdir -p ~/.codex_personal
CODEX_HOME=~/.codex_personal codex login
```

| Instance       | CODEX_HOME path | Shadow home path    |
| -------------- | --------------- | ------------------- |
| Codex Work     | `~/.codex`      | _(leave empty)_     |
| Codex Personal | `~/.codex`      | `~/.codex_personal` |

- Both instances must share the same **CODEX_HOME path**. Keep the shadow directory fresh—don't copy your full Codex home into it.
- The shadow account needs file-based credential storage (`auth.json`); see [OpenAI's credential storage guide](https://learn.chatgpt.com/docs/auth#credential-storage).
- Use a separate **CODEX_HOME path** (no shadow home) if you want completely isolated sessions.
- Writer locks must be shared with the session history: a private `thread-writer-locks` directory in a shadow home is a conflict. Do not delete lock directories while a Codex harness is running.

To switch accounts mid-thread, pick another compatible Codex instance sharing the same **CODEX_HOME path** from the model picker.

## Codex Auto

Codex Auto chooses among an explicit set of existing Codex subscription accounts.
Select it from the normal model picker on web, desktop, or mobile. New delegated
tasks choose accounts independently. Existing conversations keep their account,
including after a server restart, until its allowance is exhausted or drops below
5% of its weekly window while another account has more.

New threads go to the account whose weekly window resets first, because allowance
left at a reset is lost. An account already running three turns is skipped for new
threads while another account has room. To change that limit, set
`autoPoolMaxRunningTurns` in the pool's `config` in `settings.json`.

In **Settings → Providers → Add provider**, choose **Codex Auto**, then select
which existing Codex accounts it may use. An empty pool stays unavailable in the
composer. Edit its account list, model visibility, and favorites from the same
provider card. You can leave higher allowance accounts out to reserve them for
UltraFast.

All members must use the default `~/.codex` session home; each account can have
its own shadow authentication home. A selected member becomes **pool-only**:
it stays enabled for Auto but disappears from the direct composer and direct
agent/MCP provider access. Disable or delete the pool to stop using it, or
choose a non-member Codex account for a thread. Older mobile clients can select
a configured pool from their normal model picker; manage pool membership on web.

The picker offers only models and options supported by every configured member.
For example, a pool containing an account without UltraFast never offers
UltraFast, even when that account has run out of allowance. Auto routes only by
subscription usage windows; an account with an exhausted window is skipped even
when it has paid credits. If every account's usage refresh fails at once, Auto
uses the last reported windows, and Codex validates the account's allowance
before inference.

On a confirmed quota failure, Auto resumes the same native conversation on
another eligible account without resending the original request. It keeps goal
budgets and task ownership. Pending live questions or unfinished tool work hold
the conversation for recovery instead of switching processes. If every account
is exhausted, the thread reports available reset information and waits; Auto
never buys or redeems credits. Enable **Auto-resume limited threads** to resume
at the earliest reported reset when every eligible account is exhausted.

Agents can call the read-only `provider_usage_limits` tool to inspect per-account
windows, remaining percentages, reset times, freshness, and unavailable status.
Use `refresh: true` to request updated snapshots. A percentage describes that
account's allowance; it is not an absolute token budget. Codex Auto appears as one
entry: available while any member has allowance, with each window showing the best
member's remaining percentage and the soonest reset, plus a per-account breakdown.
Agents cannot target a member account directly; they use Codex Auto instead.

## Keep working toward a goal

Send `/goal` followed by an objective, such as `/goal Implement and validate the
remaining integration tasks`. Codex can continue across turns and delegate work
until it completes the goal or needs your input. This works with a regular Codex
account and with Codex Auto. Send `/goal` alone to check its status, `/goal pause`
to pause, or `/goal resume` to continue. Stop also pauses the goal.

With **Auto-resume limited threads** enabled, a goal stopped by exhausted
allowance resumes at the reported reset. It does not reactivate a goal you paused.

## Temporary model capacity

When OpenAI reports that the selected model is temporarily at capacity, T3 Code
waits and retries the same conversation, model, and account up to five times.
Delays increase from 30 seconds to at most two minutes; after the fifth retry the
turn reports the capacity error. Stop cancels the pending retry; messages sent
during the wait are included when work continues. Codex Auto does not switch
accounts for this error, because changing accounts does not resolve model capacity.

## Features and options

- **Image generation**: Supported when your Codex version, model, and account allow it. If disabled in Codex config, add `-c features.image_generation=true` to the instance's **Launch arguments** (applies to new sessions).
- **Async questions**: Codex can ask a question while continuing to work; answer in the question panel (steers the active turn or starts a new one) or dismiss it without sending.
- **App access approvals**: Respond to external app access requests directly in chat (one-time, session, or permanent depending on the tool).
- **MCP OAuth sign-in**: Click **Sign in** on the thread notice, approve in your browser, and paste the final redirect URL into **Finish sign-in** (requires provider management permission).
- **Daybreak**: Models with cyber access programs show a **Cyber Access** option (`Standard` or `Daybreak`, initialized from `daybreak = true` in `config.toml`).
- **Usage limits**: Limit stops display the exhausted window, reset time, and (on workspace plans) whether credits or spend limits need adjustment. Choose **Resume at reset**, or enable **Auto-resume limited threads** to schedule recovery by default.
- **Feedback to OpenAI**: Send `/feedback <description>` in an existing Codex thread to upload conversation and Codex logs to OpenAI.

## OpenAI Chrome extension

Using your signed-in Chrome profile via [OpenAI's Chrome extension](https://learn.chatgpt.com/docs/chrome-extension) requires a browser connection for the Codex thread. For remote environments, forward the remote page's port or use a reachable URL—T3 Code's built-in preview browser uses its own separate session.
