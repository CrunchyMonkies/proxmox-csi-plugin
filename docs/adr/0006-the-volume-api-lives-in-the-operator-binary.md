# 6. The volume API lives in the operator binary

- **Status:** accepted
- **Date:** 2026-10-05
- **Amends:** [ADR 3](0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) — "served by
  `proxmox-csi-service`" becomes "served by the operator binary"
- **Related:** [ADR 5](0005-the-volume-operator-ships-in-the-plugin-repository.md),
  [`docs/volume-control-plane.md`](../volume-control-plane.md),
  [`docs/volume-operator.md`](../volume-operator.md)

## Context

ADR 3 placed the gRPC volume API in its own binary, `proxmox-csi-service`, which was also
where the operator lived before ADR 5 moved the operator into the plugin repository. That left
the API in a repository that ADR 5 archived, with no home.

The question is not whether the API moves — it has nowhere else to go — but what it becomes: a
second binary in this repository, or a listener inside the existing operator.

A second binary means a second Deployment, a second image, a second release artifact, and a
second process that holds the cross-namespace ServiceAccount whose authority ADR 3 described as
"the new crown jewel". It doubles the thing an operator must monitor and restart, and it
doubles the thing a reviewer must audit for a confused-deputy bug in namespace resolution.

The operator already runs on every management cluster that has tenants, already holds leader
election, already watches the five CRD kinds, and already has the Proxmox credential. Every
argument for keeping the API separate was an argument about the `proxmox-csi-service` repository,
and that repository no longer exists.

## Decision

**The volume API is a gRPC listener inside `cmd/volume-operator`, enabled by
`-volume-api-address` (default off).**

- The listener runs in every replica. It does not require leader election: it writes custom
  resources and returns, and it is the leader-elected reconcilers that touch Proxmox. Serving in
  every replica is what lets `GetCapacity` survive a leader failover, and it is the reason the
  Deployment can scale to two replicas for availability without a second writer.
- TLS terminates at the Envoy Gateway `GRPCRoute`. The pod speaks h2c on a named port
  (`appProtocol: kubernetes.io/h2c`), and the gateway terminates the tenant's TLS and forwards
  cleartext HTTP/2 inside the cluster. This matches the pattern `auth.mgt` already uses and
  avoids a second certificate lifecycle in the operator.
- The proto lives at `api/volume/v1` in this repository, generated into `pkg/apis/volume/v1`
  by `buf`. `make proto` regenerates it, and `make generate-check` fails on drift, the same
  rule the CRD types follow.
- The chart renders a `Service` with `appProtocol: kubernetes.io/h2c` and a `GRPCRoute` on
  the shared Envoy Gateway when `operator.volumeAPI.enabled` is set.

### What of ADR 3 changes

One sentence of ADR 3's Decision — "served by `proxmox-csi-service` over TLS" — becomes
"served by `cmd/volume-operator` over h2c behind the gateway's TLS". Everything else in ADR 3
stands: the method surface, the token-derived namespace, the per-tenant rate limit, the audit
trail, the ServiceAccount authority, the `ValidatingAdmissionPolicy` as a second opinion, and
the cross-repo contract on the proto (now within one repository, which makes the contract easier
to keep).

The Consequences section of ADR 3 still applies in full. The ServiceAccount is still the crown
jewel; the audit trail is still two logs that join; the service is still tier-0. Collocating it
with the operator changes none of that — it changes only the number of processes to deploy and
the number of images to build.

## Consequences

**Gained**

- One binary, one image, one Deployment, one `ServiceAccount`, one set of RBAC markers, one
  release artifact. Everything ADR 5 gained by merging two repositories, this gains by merging
  two binaries.
- The Proxmox credential is read once, by one process. The operator's `DiskReader` and the
  API's write path share the same client pool, which is also the path the Proxmox API metrics
  are collected from.
- No TLS certificate management inside the operator. The gateway handles renewal and the
  operator never sees a private key.
- `GetCapacity` and `WatchCapacity` read from the informer cache the storage reconciler already
  populates. A separate binary would need its own watch or a second gRPC hop.

**Paid**

- A crash in the API listener takes down the reconcilers, and vice versa. The blast radius of a
  panic is one process rather than half a process. Accepted because both halves are already
  equally critical: a reconciler crash stops provisioning just as surely as an API crash does.
- The operator binary grows by the gRPC server, the OIDC token verifier, and the generated
  stubs. The image size increase is small relative to controller-runtime, and the dependency
  (`google.golang.org/grpc`) is already in `go.mod` through the CSI spec.
- `make operator-isolation` must continue to keep gRPC server code out of the CSI binaries.
  The API packages live under `pkg/operator/api`, which the isolation check already covers.
