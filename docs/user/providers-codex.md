# Codex

Use your ChatGPT plan or an existing Codex CLI login to code in T3 Code.

## Connect with ChatGPT

Connect during onboarding or in **Settings → Providers**. For a remote machine,
select that environment first. T3 Code handles Codex installation; sign in on
OpenAI and allow sharing of your ChatGPT plan.

Manage shared usage and credits in ChatGPT through **Manage usage** in T3 Code.
If a request uses a feature that ChatGPT sharing does not support, use another
provider for that request.

Fast and Ultrafast speeds are unavailable with this connection. To use the speed
options your account supports, [use an existing Codex login](#use-an-existing-codex-login).

When reconnecting, choose the same account in T3 Code and on OpenAI's sign-in
page. Disconnecting stops running threads but keeps their history and lets you
reconnect later.

If remote sign-in cannot return automatically, paste the full URL from the final
localhost page into the sign-in panel, even if that page could not load.

## Use an existing Codex login

T3 Code can use your installed Codex and its existing login. Run `codex login`
on the environment's machine to sign in. [Provider setup](./install.md#providers)
covers installation and custom configuration.

Running `codex login` does not change a managed ChatGPT connection. In
**Settings > Providers**, add a Codex instance, choose **Configure manually**,
and complete the wizard. Then select that instance from the thread's model
picker. See [Use multiple accounts](#use-multiple-accounts).

## Allow more time for provider checks

If an authenticated Codex installation reports “Timed out while checking Codex
app-server provider status,” set `T3CODE_CODEX_PROBE_TIMEOUT_MS=60000` in the
T3 server's environment and restart it. See [Provider check timeouts](./install.md#provider-check-timeouts)
for configuration and other providers.

## Generate images

Codex can generate images directly in a T3 Code thread when your Codex version,
model, and account support native image generation.

If Codex reports that its image tool is unavailable, image generation may be
disabled in its configuration. In **Settings → Providers**, edit your existing
Codex provider and add this to **Launch arguments**, keeping any other arguments:

```text
-c features.image_generation=true
```

This enables the feature for that T3 Code provider without changing your shared
Codex configuration. You do not need another provider instance or a separate image
API key. The flag does not add image support to an unsupported model or account.

Launch arguments apply to Codex sessions started after you save them. Start a new
thread with the same Codex provider and ask it to generate an image. The completed
image appears in the thread.

## Use multiple accounts

Add another ChatGPT account in **Settings → Providers**, then select the account
from the thread's model picker. Compatible accounts can continue the same thread.
Connecting accounts through T3 Code leaves your CLI login unchanged.

### Multiple CLI logins

A shared Codex home with a shadow home lets work and personal accounts continue
the same threads. The accounts share Codex sessions and configuration while keeping
their own login and available models.

Keep your first account in `~/.codex`. On the environment's machine, sign the
second account into a fresh directory:

```bash
mkdir -p ~/.codex_personal
CODEX_HOME=~/.codex_personal codex login
```

Then add a second Codex instance in **Settings > Providers**:

| Instance       | CODEX_HOME path | Shadow home path    |
| -------------- | --------------- | ------------------- |
| Codex Work     | `~/.codex`      | Leave empty         |
| Codex Personal | `~/.codex`      | `~/.codex_personal` |

Both instances must use the same **CODEX_HOME path**. T3 Code prepares the shared
state in the shadow directory; do not populate it by copying your whole Codex
home.

The shadow account needs its own `auth.json` file. If Codex uses an OS credential
store, configure file storage for this setup. See
[OpenAI's credential storage guide](https://learn.chatgpt.com/docs/auth#credential-storage).

Use a completely separate **CODEX_HOME path**, with no shadow home, when you want
separate Codex sessions and configuration. That instance cannot continue threads
from the other home.

## Switch accounts in an existing thread

Choose the other account from the thread's model picker. T3 Code offers compatible
Codex instances that share the thread's **CODEX_HOME path**. Changing accounts does
not move the conversation into a separate Codex home.

If the account is missing from the picker, compare the home paths in provider
settings. If two instances show the same unexpected account or models, check their
reported accounts, refresh provider status, and confirm the second instance has
its own shadow path and login. A shadow-home conflict usually means the directory
contains a copied Codex setup. Use a fresh shadow directory and sign in again.
A private `thread-writer-locks` directory is also a conflict: writer locks must be
shared with the session history. Do not delete lock directories while a Codex
harness is running.

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

## Answer questions while Codex works

Codex can ask a question and keep working. Answer it in the thread's question
panel. The answer becomes a new message: it reaches the active turn, or starts
another turn if Codex has finished. Unanswered questions survive reconnects.
If you do not want to answer, dismiss the question from its panel. Dismissing
closes it without sending anything to Codex. This requires a Codex version that
supports async questions.

## Approve app access

Codex tools can request access to another app. Respond to the named app's request
in the thread on web, desktop, or mobile. Some tools offer access for one request,
the current session, or permanently. See [Permission modes](./permission-modes.md)
for command and file approvals.

## Sign in to an MCP server

When an MCP server you added to Codex needs you to sign in, the thread shows a notice with
**Sign in**; on mobile, tap the notice. Approve access on the page it opens. Your browser then goes
to a page that may not load, especially when Codex runs on another machine: copy that page's
address, paste it into T3 Code, and choose **Finish sign-in**. Codex reconnects the server in that
thread and keeps the sign-in for other threads on the same Codex instance; an instance with its own
home, such as a second account, may need to sign in separately. Signing in needs permission to
manage providers.

## Use the OpenAI Chrome extension

Connecting ChatGPT in **Settings → Providers** signs in the Codex provider.
Using your signed-in Chrome profile through the extension requires a browser
connection for the current Codex thread.

If a T3 Code thread reports `The browser access host is unavailable`, its browser
host is unavailable; this does not identify an extension connection failure.
For a task that requested Chrome, follow [OpenAI's setup guide](https://learn.chatgpt.com/docs/chrome-extension),
then select Chrome in a Codex chat in the ChatGPT desktop app.
If the task runs on a remote environment, `localhost` in the desktop browser
refers to your computer. Forward the remote page's port or use a URL reachable
from your computer before repeating the task. Connecting to T3 Code through a
relay or tunnel does not by itself expose that page. T3 Code's built-in browser
preview uses its own browser session.

## Use Daybreak

Models with cyber access programs show a Cyber Access option in the model
options. It starts on Standard, or on Daybreak when `config.toml` in your Codex
home sets `daybreak = true` or the model only runs under Daybreak. Your choice carries
over to new threads. Custom models do not offer it. If Codex rejects a turn
because your login lacks the program, switch Cyber Access back to Standard or
sign in to Codex with an account that has it.

## Codex says I hit a usage limit

When Codex stops on a usage limit, the thread names the window that ran out and
when it resets, when Codex reports them. Choose **Resume at reset**, or enable
**Auto-resume limited threads** to schedule recovery by default. On a workspace plan the
message also says whether your workspace owner needs to add credits or raise the
spend limit to continue sooner.

## Send feedback to OpenAI

In an existing Codex thread, send `/feedback` with an optional description, for
example `/feedback The agent stopped before finishing the tests`. This uploads
the conversation and Codex logs to OpenAI. The returned thread ID can be shared
with OpenAI support.
