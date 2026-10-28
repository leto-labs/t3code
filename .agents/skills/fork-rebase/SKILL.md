---
name: fork-rebase
description: Move this fork's commit stack onto a new upstream T3 Code release, fix what the move breaks, and push the new fork branches. Use for the scheduled upstream release check, or when asked to rebase the fork onto a new upstream tag.
---

# Fork rebase

This is the unattended form of
[Moving to a new upstream release](../../../docs/operations/release-rebase.md#moving-to-a-new-upstream-release).
Read that page first. It holds the stack rules and the conflict rules that this skill applies.

A run pushes new `fork/<tag>` branches only. It never pushes a tag, force-pushes a branch,
changes the default branch, publishes a release, or updates an installed server. Those stay with
the maintainer.

## 1. Find the release

```sh
git fetch -q upstream --tags && git fetch -q origin --prune
base=$(git for-each-ref --format='%(refname:lstrip=4)' 'refs/remotes/origin/fork/' |
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
latest=$(git tag -l 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
```

- If `latest` is not newer than `base`, there is nothing to do. Say so in one line. A scheduled
  run then archives its own thread.
- Use only a tag that upstream published as a stable GitHub Release:
  `curl -s https://api.github.com/repos/pingdotgg/t3code/releases/tags/$latest` must show
  `"prerelease": false`. If the release is not there yet, stop and say so in one line.
- If `git worktree list` already shows a checkout of `fork/$latest`, an earlier run stopped in
  it. Continue from that state, or repeat its open question. Do not start again.

## 2. Rebase

Make the new checkout next to the existing fork worktrees, named like them.

```sh
d=$(date -u +%Y%m%d)
for b in $(git for-each-ref --format='%(refname:lstrip=3)' "refs/remotes/origin/fork/$base*"); do
  git tag "archive/${b//\//-}-$d" "origin/$b"
done
git worktree add -b "fork/$latest" <worktree> "origin/fork/$base"
cd <worktree> && git rebase --onto "$latest" "$base" "fork/$latest"
```

Resolve each conflict by the rules in the operations page. For each one, read what upstream
changed (`git log --oneline $base..$latest -- <file>` and `git diff $base $latest -- <file>`)
before you choose. Keep a short note per commit of what you adapted or dropped, for the report.

After the stack, install (`vp i`) and fix what still breaks. Commit each fix as
`fixup! <title of the owning commit>` and fold it in with
`GIT_SEQUENCE_EDITOR=: git rebase -i --autosquash $latest`.

Then move each branch built on the stack (for example `fork/$base-live-voice`):

```sh
git branch "fork/$latest-live-voice" "origin/fork/$base-live-voice"
git rebase --onto "fork/$latest" "origin/fork/$base" "fork/$latest-live-voice"
```

## 3. Verify

All of these must pass on the stack. Run the first two on each branch built on it too.

```sh
git rebase --exec 'vp run typecheck' "$latest"
vp run check:changed "$latest"
openspec validate --specs --strict
node apps/server/scripts/responsiveness-e2e.ts --variants broker --repos 40
```

If Playwright cannot download its browser, set `T3CODE_PLAYWRIGHT_EXECUTABLE` to a system Chrome.
A typecheck task that exits with 137 was killed for memory. Run it again before you call it a
failure.

## 4. Push

Scan what the fork adds for private data before the push: `git diff $latest fork/$latest` must
not contain home directory paths, host names, account names, email addresses other than the
commit author's, or credentials.

```sh
git push origin "fork/$latest" "fork/$latest-live-voice"
```

The push starts the `Fork` workflow, which runs the same checks in CI. Give its run link.

## 5. Report

Keep it short:

- The upstream release and the number of upstream commits since `base`.
- Per stack commit: applied clean, adapted (how), or reduced (what upstream now covers).
- Each test expectation you changed, and why the new value is right.
- The verification results and the CI run link.
- What the maintainer does next: tag `v<version>-t3custom.1` on `fork/$latest`, make
  `fork/$latest` the default branch, then update the installed servers.

## Stop and ask

Stop, push nothing, and leave the worktree as it is when:

- upstream changed the behavior a stack commit exists for, and keeping, adapting, or dropping it
  is a product decision;
- a test fails and you cannot explain why;
- a fix needs a change outside the feature of the commit that owns it.

Put the question in the report, with the commit, the files, and the options you see.
