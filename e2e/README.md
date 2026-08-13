# e2e — semi-manual happy-path regression

One batch per feature bucket, run by hand at every new sandman binary
version to confirm the happy path still works end to end. Complements the
automated layers: `conformance/` (deterministic Go tests, matrix-sharded
in CI) and `cli/smoke_test.go` (linear Go smoke flow). The batches here are
shell scripts a human can run against any daemon (`SANDBOX_ADDR`) with any
binary (`BIN`), to catch exactly the class of bug the automated suites
miss — the `pipeline delete --force` dead-end, the spout-stop mutex wedge,
the update output accumulation. See `FINDINGS.md` for what the walk caught.

## Planned layout

```
e2e/
  README.md        this file
  FINDINGS.md      the manual-walk findings log (2026-08-13)
  run.sh           runner: BIN=… ADDR=… ./run.sh [pattern] → per-batch PASS/FAIL
  lib.sh           shared: unique-name prefix, assert_contains, cli(), cleanup trap
  01-repos.sh
  02-files.sh
  03-commits-branches.sh
  04-tags.sh
  05-pipelines.sh
  06-jobs-datums.sh
  07-chain-flush.sh
  08-update.sh
  09-cron.sh
  10-git-input.sh
  11-service.sh
  12-spout.sh
  13-triggers.sh
  14-transactions.sh
  15-secrets.sh
  16-stats.sh
  17-logs.sh
  18-check-reset.sh
  19-resources-customization.sh
  20-fleet.sh          (skipped when no worker advertised)
```

## Batch-file contract

- Each batch is one independent scenario: own resource names (prefix
  `hp$$-`), `set -euo pipefail`, trap-based cleanup, self-contained
  assertions (grep exact expected output; nonzero exit = FAIL).
- Runner prints the binary `version` first, then per-batch
  `PASS/FAIL <name>` and a `N/M passed` summary; exits nonzero on any
  failure.
- Runner is daemon-agnostic: `ADDR` defaults to `127.0.0.1:4242`, `BIN`
  defaults to the repo-root `./sandman` build.
- The bucket list (numbered 01-20) is the same one the manual walk used;
  each batch's scenario is the happy path documented in `FINDINGS.md`.
