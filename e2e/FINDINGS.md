# FINDINGS — manual happy-path walk, 2026-08-13

Method: for each feature bucket, drove the current binary (`/tmp/sandman-current`,
worktree build) through the typical happy-path usage against the live daemon
on 192.168.1.147:4242 (container runner, `/var/lib/sandman` state). Every claim
below was observed live; evidence quoted verbatim. Statuses re-checked against
the worktree source after the maintainer's fix batch (commits `3e42a26`…`13fbabe`).

## Buckets walked (10 of 20)

| # | Bucket | Verdict |
|---|---|---|
| 09 | cron | worked; 1 bug (F7) |
| 13 | triggers | clean |
| 08 | update | worked; 1 bug (F8) |
| 15 | secrets | worked; 1 usability gap (F10) |
| 14 | transactions | worked; 1 bug (F9) |
| 17 | logs | clean |
| 19 | resources/customization | worked; 1 silent-ignore trap (F11) |
| 16 | stats | worked; 1 CLI gap (G1) |
| 10 | git inputs | worked; 1 state-machine finding (F13) |
| 20 | fleet | worked (after updating the stale 133 install) |

## Fixed (verified in source, committed)

- **F1 — local (unplaced) service pipelines dead on the container runner.**
  `runServiceJob` started the service container with no `-p` publish while the
  proxy dialed `127.0.0.1:<internalPort>` → `curl: connection reset` (code 56)
  with the job showing `running`. The remote path published (`-p
  <internal>:<internal>`, worker.go:505); the suite never caught it because the
  harness daemon runs `-runner process`. Fix: service.go:182 now publishes
  `127.0.0.1:<internal>:<internal>` for local services.
- **F2 — conformance suite killed live services with SIGKILL (exit 137).**
  Harness `TestMain` swept `docker ps -aq --filter name=sandman-` → `docker
  rm -f` every match, including production service containers on the same
  dockerd → "service process exited with code 137". Fix: harness_test.go:81
  now scopes the sweep to `name=sandman-conformance-`.
- **F6 — `pipeline stop` panicked on any spout and wedged the daemon.**
  `stopPipeline` accessed `rec.Pipeline.Input.Repo` (nil for spouts) →
  `http: panic serving … runtime error: invalid memory address` inside the
  handler, after `pipelineRecMu.Lock()` → mutex leaked → every
  pipeline-mutex op hung until restart. Fix: pipeline.go:888 guards
  `rec.Pipeline.Input != nil`.
- **F7 — `pipeline delete` error advertised a nonexistent flag.**
  `pipeline "cronp" has downstream consumers; force required (400)` but
  `--force` did not exist (`unknown flag: --force`) — a dead-end; the user
  had to delete downstreams first. Fix: cli.go:824 registers
  `--force` ("delete a pipeline even if it has downstream consumers").
- **F8 — partial reprocess accumulated output instead of replacing.**
  After an update, a job's OUT dir held skipped datums' copy-forwarded
  output plus the new datum's output; `mergeOutputs` appended the second to
  the first → `echo -n v3 > ver.txt` produced head `v1v3`, then `v1v3v3` on
  the next partial reprocess (verified at the blob level: stored SHA =
  literal `v1v3`). Fix: `datumState.TransformHash` — carried content is
  re-run-equivalent (FS-5 concatenation) only under the same transform;
  under a changed transform a fresh datum's output supersedes it.
- **F10 — two `SecretMount`s on one `mountPath` failed the job.**
  `docker: Error response from daemon: Duplicate mount point:
  /sandman/secrets` (exit 125) — sandman is strictly one key per mount, so
  exposing two keys the whole-secret-at-one-path way was
  impossible. Fix (13fbabe): same-path secret mounts merge into one
  bind mount.

## Fixed in round 2 (maintainer batch, commits `3c5651a`…`79fb8b8`; all
live-verified 2026-08-14 on a fresh build of HEAD `79fb8b8`)

- **F9 — `pipeline update --tx` dead code.** Fix: flag registered
  (internal/cli/cli.go:865, folded into the f4c48d1 batch). Verified:
  `pipeline update --tx <id> -f spec.json` stages and applies.
- **F11 — silent ignore of unknown spec fields.** Fix: strict spec decode
  (`DisallowUnknownFields`, api.go:1098 + internal/cli/cli.go:1060).
  Verified: top-level `resourceLimits` → `spec: json: unknown field
  "resourceLimits"`.
- **F13 — git private-push permanent silencing.** Fix (5f7d0ff):
  auto-recover clone-failed pipelines on successful push. Verified:
  private push → pipeline failure; next normal push → job runs, revision
  lands.
- **F14 — CLI open-commit phantom branch.** Fix (79fb8b8): commit-id refs
  address the commit. Verified: `put repo@<commitid>` writes into the open
  commit, master resolves to it, no phantom branch, no empty finish.
- **G1 — `job inspect` no aggregate stats.** Fix (80cf5ff): prints
  processed/recovered/failed/skipped. Verified on a failed job:
  `processed 0 / recovered 0 / failed 1 / skipped 0`.

## Open (not yet fixed in source at the time of writing)

- **F12 — `"user": "nobody"` fails.** `This account is not available` —
  busybox `su` refuses pre-existing accounts with nologin shells; the
  `user` field works only for fresh usernames the runner creates
  (`adduser -D`). Untested surface: the suite only uses fresh names.
  Re-verified 2026-08-14: still reproduces on the current build.

## CLI surface gaps (not bugs)

- `datum list` takes a job ID, not `pipeline@job`; `tag delete` exists
  (added in the refactor; the turn-1 probe predated it). `commit inspect`
  accepts a commit ID, `repo@branch`, and `repo@<commit-id>` (F14).

## Divergences (by design or accepted; document, don't fix)

- Hyphenated repo names are creatable but unconsumable by pipelines
  (`input name "x-y" is not a valid environment variable name`, 400) —
  hyphens work end to end in the upstream tool.
- `file put` to a nonexistent repo auto-creates it — the upstream tool
  errors.
- Triggers are pipeline-input options (`trigger.sizeBytes`) not
  branch-level triggers; there is no branch-trigger CLI surface.
- Git inputs are a push receiver (D-16): `POST /api/v1/git/push` with the
  pushed tree; URLs are validated but never cloned. No CLI verb for git
  push (curl only).
- Cron/git derived repos (`<pipeline>-<cron>`, URL-derived) are deleted
  with their pipeline (5f7d0ff) unless another pipeline still references
  the mapped repo (a shared git side or a plain input over it); `repo
  delete` cleans the remaining inert-data class.
- Job failure output commits are finished empty and become the branch head,
  so a failed job hides prior good output from the head view (recoverable
  via history).

## Operational notes from the walk

- The 133 worker (`ubuntu-worker-02`) was running a stale binary AND a unit
  passing `-token ${SANDBOX_TOKEN}`, a flag removed in the refactor —
  worker exited 2 on restart. Overridden: current build installed,
  unit/env cleaned (`-token` dropped), worker active. The repo's
  `deploy/` files were already current.
- SB-089's full-suite flush-timeout hang (batch 54-59 flake class) did not
  reproduce in any isolated manual run.
- Leftover artifacts flagged during the walk: `sandman-miladyos-42-ecf45fc63d96-service`
  container on 133 (old svc pipeline, still up), stale `conformance-*`
  nodes in the fleet view (dead CI daemons, mdns/sync sources).
