# Repository Guidelines

## Project Structure & Module Organization

- `cmd/` entrypoints for CNI plugin, controller, daemon, and helpers; binaries land in `dist/images/` via Make targets.
- `pkg/` shared Go libraries; `fastpath/` and `versions/` hold data-plane helpers and release metadata.
- `charts/` Helm chart, `yamls/` and top-level `*-sa.yaml` manifest examples; `docs/` product docs.
- `hack/` CI/dev scripts; `makefiles/` split build/test logic; `test/` contains `unittest`, `e2e`, `performance`, and fixtures.

## Build, Test, and Development Commands

- `make build-go` – tidy modules and compile Go binaries for linux/amd64 into `dist/images/`.
- `make lint` – run `golangci-lint` plus Go “modernize”; auto-fixes when not in CI.
- `make ut` – run unit tests: Ginkgo suites in `test/unittest` and `go test` with coverage for `pkg`.

## General Coding Guidelines

- Every time after editing code. MUST run `make lint` to detect and fix potential lint issues.
- When modifying code, try to clean up any related code logic that is no longer needed.
- Follow `CODE_STYLE.md`: camelCase identifiers, keep functions short (~100 lines), return/log errors instead of discarding, and prefer `if err := ...; err != nil` patterns.
- For CRD dependencies, follow the lifecycle rules in `CODE_STYLE.md`: never report a referrer
  Ready before the dependency and data plane are ready. A dependency becoming ready or invalid
  must enqueue its referrers; a referrer dropping the reference or being deleted must enqueue the
  dependency when its status or finalizer depends on the reference count. Event handlers only
  enqueue keys: each reconciler rereads current state and orders data-plane cleanup, credential
  changes, Status updates, and finalizer release. Trigger readiness wake-ups only after the
  dependency's authoritative state reaches the informer cache; retain rate-limited retries as a
  fallback; skip terminating and already-converged referrers unless they require cleanup. Treat an
  authoritative usable-to-unusable transition as invalidation, but do not downgrade an established
  binding merely because a dependency Spec update is pending while its old Status/data plane remain
  valid.

## Adding a New Feature

- Plan first: clarify any uncertainties and confirm the approach before making changes.
- Add unit tests to cover the new feature.
- When adding end-to-end (e2e) tests for the new feature, use `f.SkipVersionPriorTo` to ensure they run only on supported branches.

## Fixing a Bug

- Analyze the issue first and identify the root cause. Confirm the analysis before making edits.
- Check if the same bug pattern exists elsewhere in the codebase.
