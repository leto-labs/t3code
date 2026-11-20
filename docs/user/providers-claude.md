# Claude

T3 Code drives your Claude Code installation, login, skills, and plugins. See [provider setup](./install.md#providers) for initial installation.

## Multiple accounts and presets

Isolate separate Claude accounts or router presets with distinct `CLAUDE_CONFIG_DIR` directories:

```bash
mkdir -p ~/.claude_personal
CLAUDE_CONFIG_DIR=~/.claude_personal claude auth login
```

Add a second Claude instance in **Settings → Providers** (or **Add Claude account** on mobile) pointing **CLAUDE_CONFIG_DIR path** to `~/.claude_personal` (leave empty for the default directory). Always use `CLAUDE_CONFIG_DIR` rather than `HOME` so system keychain paths stay intact.

- Existing threads can only switch between Claude instances sharing the same config directory. To move threads between accounts automatically, use [Claude Auto](#claude-auto).
- Put API keys and endpoint overrides in **Environment variables** (not **Launch arguments**).
- Paths in `--settings` and `--append-system-prompt-file` expand `~` to the server's home directory; other relative paths resolve from the thread's workspace.

## Compact long conversations

- **Auto-compact threshold**: Set **Auto-compact after** (`100000`–`1000000` tokens) in the Claude instance settings to trigger summarization earlier without changing the model's context window.
- **Manual compaction**: Send `/compact` in chat.
- **Idle compaction (>200k tokens)**: Configure **Settings → Providers → Advanced → Compact idle threads** (overridable per instance):
  - **When you return** (default): Shows a **Compact** chip next to Send once Claude's prompt cache expires (~1 hour on subscriptions, 5 min on API/Bedrock/Vertex). Click it to switch to **Keep** if you want full history.
  - **Before the cache expires (beta)**: Automatically sends `/compact` shortly before the cache expires while history is still cached (falls back to **When you return** if missed).
  - **Never**: Disables idle compaction prompts.

## Steering, usage limits, and tasks

- **Steering**: Steered messages wait until Claude finishes its active tool calls, then enter the same turn (or queue for the next turn if Claude exits or drops them).
- **Usage limits**: Mid-turn limit hits display the window and reset time; Claude holds the turn until reset, or you can stop and resume later.
- **Task lists**: Add `CLAUDE_CODE_ENABLE_TODO_TOOLS=1` in the instance's **Environment → Variables** to show Claude's task list progress in web and desktop threads.
- **Auto mode**: Uses Claude Code's native auto mode (disabled if `disableAutoMode` is set in Claude's managed, user, or project settings). See [Permission modes](./permission-modes.md).

## Claude Auto

Claude Auto spreads threads across your Claude subscription accounts. Set up each account
as its own Claude instance first, as described in
[Multiple accounts and presets](#multiple-accounts-and-presets). Then, in
**Settings > Providers > Add provider**, choose **Claude Auto** and select the accounts it
may use. Select **Claude Auto** in the model picker on web, desktop, or mobile.

New threads go to the account whose weekly window resets first, since allowance left at a
reset is lost. A Fable thread is limited by both the weekly window and the Fable weekly
window; other models only by the weekly window. An account whose 5-hour window is used up
is skipped until it resets. A thread stays on its account until the account runs out, or
until it drops below 5% of its weekly window while another account has more. An account
running three turns is skipped for new threads while another has room; set
`autoPoolMaxRunningTurns` in the pool's `config` in `settings.json` to change that.

When an account runs out mid-turn, Auto checks its usage and continues the same
conversation on another account without resending your message. This also applies when
Claude Code would pause the turn until the limit resets. If every account is out, the
thread shows the reset time and waits. Auto routes on subscription usage only; it never
counts extra usage, and it never buys or redeems resets.

Each account keeps its own Claude config directory, login, settings, plugins, and MCP
servers. When a thread moves, T3 Code stops the previous account's Claude process, then
copies the conversation into the next account's directory. A moved thread therefore runs
with the next account's settings and plugins, and starts without that account's prompt
cache.

Selected accounts are reserved for the pool: they disappear from the model picker and from
agent tools, and their usage still shows in the usage dashboard. Threads that ran directly
on an account keep their conversation when you switch them to Claude Auto. After you
remove an account from the pool, a thread moved from Claude Auto to that account starts
with a summary handoff. Claude Auto does not write titles or commit messages; those use
the provider chosen for text generation. Members cannot set API keys, an API endpoint, or a
cloud backend in their environment variables.

Agents see Claude Auto as one entry in `provider_usage_limits`, with each account's windows
in a breakdown, and use Claude Auto instead of a member account.

## Skills and MCP sign-in

- **Skills**: Loaded from `<CLAUDE_CONFIG_DIR>/skills`, the project's `.claude/skills` (config dir wins on name collisions), and enabled Claude plugins (e.g. `plugin:review`). Invoke via `$`. Send `disable-model-invocation` skills one per message.
- **MCP OAuth**: When a Claude MCP server needs sign-in, click **Sign in** on the thread notice, approve in your browser, and paste the final redirect URL into T3 Code (**Finish sign-in**). Connectors added on `claude.ai` authenticate there instead.

## Claude in Chrome

To use the [Claude in Chrome extension](https://code.claude.com/docs/en/chrome) in T3 Code threads, add `--chrome` to **Launch arguments** in the Claude instance settings (applies to new sessions). Requires a direct Anthropic `/login` plan—not supported with API keys, `setup-token`, Bedrock/Vertex, or WSL.

## OpenRouter and local routers

Create a separate Claude instance with its own config directory (e.g. `~/.claude_openrouter`, after running `/logout` if it had a cached Anthropic login) and set **Environment variables**:

| Variable               | Value                                    |
| ---------------------- | ---------------------------------------- |
| `ANTHROPIC_BASE_URL`   | `https://openrouter.ai/api`              |
| `ANTHROPIC_AUTH_TOKEN` | Your OpenRouter API key (mark Sensitive) |
| `ANTHROPIC_API_KEY`    | Explicitly empty                         |

Add unlisted OpenRouter models via **Add custom model** in the instance settings. See the [OpenRouter Claude Code guide](https://openrouter.ai/docs/cookbook/coding-agents/claude-code-integration) or [Claude Code Router](https://github.com/musistudio/claude-code-router) for local router setup.
