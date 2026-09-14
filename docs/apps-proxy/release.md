# Apps Proxy Release Process

A release of the Apps Proxy is a **git tag**. There is no manual build step and no GitHub
Release object — pushing a tag that matches one of the patterns below starts
[.github/workflows/release-service-apps-proxy.yml](../../.github/workflows/release-service-apps-proxy.yml),
which builds the image, pushes it to all three cloud registries and (for non-`dev` tags)
triggers the deployment in [keboola/kbc-stacks](https://github.com/keboola/kbc-stacks).

## Tag Patterns

| Tag | Image built & pushed | Deployment triggered |
|---|---|---|
| `production-apps-proxy-v<X.Y.Z>` | yes | yes — two waves, see [Waved Rollout](#waved-rollout) |
| `canary-<name>-apps-proxy-v<...>` | yes | yes — canary stacks only |
| `dev-apps-proxy-v<...>` | yes | **no** — image is pushed, nothing is deployed |

The tag name is used verbatim as the image tag (`IMAGE_TAG: ${{ github.ref_name }}`), so the
tag you push is exactly what ends up in `tag.yaml` in `kbc-stacks`. The prefix is what makes
the platform treat the image as dev / canary / production — see
[GitOps Handbook](https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3717660690/GitOps+Handbook).

Version numbers follow SemVer against the previous `production-apps-proxy-v*` tag. Use
`git tag -l 'production-apps-proxy-*' --sort=-creatordate | head -1` to find the current one.

## What the Workflow Does

1. **Lint** — reuses `test-lint.yml`.
2. **Unit tests** — reuses `test-unit.yml` with
   `package-exception-regex: "./internal/pkg/service/stream|./internal/pkg/service/cli"`.
   Stream and CLI packages are skipped, but everything else (including
   `internal/pkg/service/common/...`) still runs and **can block the release**.
3. **Build & push** — builds `keboola/apps-proxy:<tag>` from
   [provisioning/apps-proxy/docker/Dockerfile](../../provisioning/apps-proxy/docker/Dockerfile)
   and pushes it to AWS ECR, Azure ACR and GCP GAR via the
   [push-image-aws-azure-gcp](../../.github/actions/push-image-aws-azure-gcp) action.
4. **Trigger image tag update** — skipped for `dev-*` tags. Otherwise it dispatches
   `update-image-tag.yaml` in `kbc-stacks` with `helm-chart: apps-proxy`, which opens the
   `tag.yaml` update PRs. ArgoCD picks up each merged PR within 2–3 minutes.

## Waved Rollout

A `production-*` tag does **not** deploy everywhere at once. `update-image-tag.yaml` applies
the `standard` two-wave dev→prod strategy, which since ST-4131 is the universal default for
every app — you do not opt in, and the legacy `automerge` / `multi-stage` inputs are ignored.

| Wave | Stacks | Merged by |
|---|---|---|
| `release:wave:0` | dev + testing (`dev-keboola-aws-eu-west-1`, `dev-keboola-gcp-us-central1`, `kbc-testing-azure-east-us-2`) | automatically |
| `release:wave:1` | all production stacks (`kbc-eu-central-1`, `kbc-us-east-1`, `com-keboola-*`, `cloud-keboola-*`) | the promoter |

Both waves are opened pre-approved and merge without you. Wave 0 goes to dev immediately;
HIU never merges a production target itself, so wave 1 is merged by the promoter shortly
afterwards (about a minute in practice). Merge it by hand only if the promoter does not.

The practical consequence is that the dev→prod gap is small — treat it as a safety net, not
as time to verify. If a change genuinely needs a soak on dev, use `promoter-gradual` or
`promoter-manual-per-stack` instead of relying on the wave delay.

Find both PRs with the "All wave PRs of this release" link in the PR body, or:

```shell
gh search prs --repo keboola/kbc-stacks "apps-proxy@production-apps-proxy-v<X.Y.Z>"
```

## Releasing

### 1. Check that `main` is green

The release workflow re-runs lint and unit tests, so a red `main` means a failed release.

```shell
gh run list -R keboola/keboola-as-code -w "Push CI" -b main -L 1
```

Some tests in this repo are timing-sensitive (etcd watch tests in particular) and
occasionally flake on the `linux-services` job. Confirm a failure is genuine before acting
on it — compare against the same commit's run on the feature branch, and re-run the failed
job if it looks flaky:

```shell
gh run rerun <run-id> -R keboola/keboola-as-code --failed
```

Never release from a broken `main` — revert to the last working commit instead.

### 2. Check the config of the release

If the release adds or changes a required configuration key, make sure the corresponding
values are in `kbc-stacks` **before** tagging. The proxy config is rendered by
`apps/apps-proxy/templates/secret-config.yaml`, and stack values live in
`<stackId>/apps-proxy/{values.yaml,secrets.yaml}`. Keys that are gated behind a Helm `if`
fail silently — the feature simply stays off on stacks that are missing the value.

### 3. Tag and push

```shell
git checkout main && git pull
git tag production-apps-proxy-v1.18.0
git push origin production-apps-proxy-v1.18.0
```

### 4. Watch the release

```shell
gh run list -R keboola/keboola-as-code -w "Release: Apps Proxy" -L 1
```

Then check that the deployment landed:

- the [update-image-tag](https://github.com/keboola/kbc-stacks/actions/workflows/update-image-tag.yaml)
  run in `kbc-stacks` (find it by the `apps-proxy` chart name),
- **the wave 1 PR** — it will not merge itself, see [Waved Rollout](#waved-rollout),
- `<stackId>/apps-proxy/tag.yaml` actually contains the new tag,
- ArgoCD at https://argo.keboola.tech — filter by `app=apps-proxy` under `Labels`,
  use `Refresh apps` to speed up the sync.

After the release, click through a data app login and verify the new functionality really
works on at least one stack.

## Canary First

Recommended for risky changes. Canary stacks deploy from the `canary-$name` branch of the
infrastructure repo, not from `main`.

```shell
git tag canary-orion-apps-proxy-v-AJDA-1234.1
git push origin canary-orion-apps-proxy-v-AJDA-1234.1
```

The trailing `.1`, `.2`, … lets you iterate — each push needs a fresh tag. Verify on the
canary stack, then push the `production-*` tag from the same commit.

See [Canary Stacks Handbook](https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3932585992).

## Finer-Grained Rollout

The two waves are already a dev→prod gate. If a change needs more care than that — one
production stack at a time — use the `promoter-manual-per-stack` label, which opens a PR per
stack with a human merging every one. `promoter-critical` and `promoter-gradual` sit between
that and `standard`; the label on the PR that builds the image is the sole arbiter, so it has
to be set before the image is built.

## Rollback

There is no revert workflow for the proxy (unlike the CLI, see
[cli-release-revert.md](../cli-release-revert.md)). Roll back through `kbc-stacks`:

1. Find the commit that bumped `tag.yaml` (git blame on any
   `<stackId>/apps-proxy/tag.yaml`, or search closed PRs for the old tag).
2. `git revert -m 1 <commit-sha>` on a new branch, open a PR, get an SRE approval, merge.

Alternatively re-run `update-image-tag` with the previous `production-apps-proxy-v*` tag —
the images are still in the registries.

## Release Rules

- Do not release on Friday afternoon.
- Test via a `dev-*` or `canary-*` tag first; have the PM verify before a production release.
- Never cancel a running release pipeline.
- Watch out for stack differences — verify on more than one stack when the change touches
  cloud-specific behaviour.

See [Release Management](https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3848863787).
