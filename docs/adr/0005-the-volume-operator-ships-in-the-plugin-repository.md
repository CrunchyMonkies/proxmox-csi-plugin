# 5. The volume operator ships in the plugin repository

- **Status:** accepted
- **Date:** 2026-10-05
- **Supersedes:** the "One repository, two modes" rejected alternative of
  [ADR 1](0001-the-management-apiserver-is-the-volume-api.md). Nothing else in ADR 1 changes.
- **Related:** [`docs/volume-control-plane.md`](../volume-control-plane.md),
  [`docs/volume-operator.md`](../volume-operator.md)

## Context

ADR 1 put the volume operator in its own repository, `proxmox-csi-service`, so that "the only thing
that ever holds a Proxmox credential" could be checked by reading one repository. It accepted the
cost: a change spanning `pkg/utils/volume` and a reconciler became two pull requests and a tag.

That cost turned out higher than expected:

- The operator imported the plugin through a pinned fork tag. The pin drifted two releases behind
  the plugin (`v0.20.0-1.5.0` against `1.7.0`), so the operator was built against a `proxmoxpool`
  and a volumeID parser that the tenant driver no longer ran.
- The API types had to live in a nested Go module (`api/`) only to break the import cycle between
  the two repositories, which meant two lint runs, two test runs and controller-gen run from a
  subdirectory with absolute paths.
- The volumeID byte-identity golden table that ADR 1 required to be "checked into both repos" is a
  release blocker that only exists because there are two repos.

## Decision

**The operator moves into the plugin repository as an optional component, off by default.**

- The binary is `cmd/volume-operator`, published as the `proxmox-csi-operator` image.
- The API types are `pkg/apis/csi/v1alpha1`, in the plugin's own module. The nested module is gone.
- The reconcilers and Proxmox readers are `pkg/operator/...`.
- The chart is the plugin chart, with `operator.enabled` (default `false`). A management cluster
  installs it with `controller.enabled=false` and `node.enabled=false`.

The property ADR 1 wanted, "a reader can check the credential path in one place", is kept by
making it a directory boundary that CI enforces:

- Only `cmd/volume-operator` and `pkg/operator/...` may import controller-runtime or the operator
  packages. `make operator-isolation` fails the build if `cmd/controller`, `cmd/node` or
  `cmd/pvecsictl` link either in.
- The operator's cluster-scoped RBAC is still generated from the `+kubebuilder:rbac` markers in
  `pkg/operator/...` (`make manifests`), and `make generate-check` fails on drift.

## Consequences

**Gained**

- One module and one version. The operator and the tenant driver always agree on volumeID
  construction because they share the same code at the same commit.
- A change spanning the driver and a reconciler is one pull request.
- Tenant-side work (the `pkg/csi/remote` backend) imports the API types locally.

**Paid**

- The plugin module now carries controller-runtime and the apiextensions test dependencies, even
  for people who never enable the operator. The binaries they run do not link them; `go.mod` does.
- Auditing the credential path means reading two directories of a larger repository, not a whole
  small one. The isolation check is what makes that tractable, so removing it would reopen this
  decision.
- The `proxmox-csi-service` repository is archived. Its history stays readable there; new work
  happens here.
