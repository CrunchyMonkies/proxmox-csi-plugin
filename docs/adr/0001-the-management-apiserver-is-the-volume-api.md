# 1. The management apiserver is the volume API

- **Status:** accepted; superseded in part by
  [ADR 3](0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) — the tenant interface
  only. Everything else here still governs; ADR 3 lists what survives
- **Date:** 2026-08-15
- **Supersedes:** nothing
- **Related:** [ADR 2](0002-federate-tenant-service-account-tokens-directly.md),
  [`docs/volume-control-plane.md`](../volume-control-plane.md)

## Context

Several Kubernetes clusters run as VMs on one Proxmox cluster. Every one of them runs
`proxmox-csi-plugin` with its own Proxmox API credential, and every one of those credentials is
scoped to `/`:

- the CSI controller token holds `VM.Audit VM.Config.Disk Datastore.Allocate
  Datastore.AllocateSpace Datastore.Audit` cluster-wide;
- the migrator authenticates as `root@pam` with a password, because PVE's disk-copy endpoint
  refuses anything less.

So any tenant cluster's CSI controller can attach any other cluster's disk to any VM it can name,
and a cluster-admin on any tenant can read a credential that is root on the hypervisor.

Nothing records which cluster owns which disk. The only thing distinguishing one cluster's volumes
from another's is the `features.controllerVmID` placeholder embedded in the disk name
(`vm-9999-pvc-<uuid>`) — a naming convention, not a boundary, and one that two of the clusters
currently share because both use the default `9999`.

The goal is to make one cluster the sole holder of Proxmox credentials and have the others request
volumes through an interface that is authenticated, authorized, audited and quota-enforced.

## Decision

**Tenant clusters request volumes by creating custom resources in the management cluster. The
management cluster's apiserver is the interface; there is no other service and no bespoke wire
protocol.**

A tenant's CSI controller runs a `remote` backend that translates each controller RPC into a
custom resource in a namespace it alone can write. A single operator in the management cluster
reconciles those resources against Proxmox, holding the only credential in the estate.

Concretely:

- Group `csi.crunchymonkies.com/v1alpha1`, five kinds: `TenantCluster`, `ProxmoxVolume`,
  `ProxmoxVolumeAttachment`, `ProxmoxVolumeSnapshot`, `ProxmoxStorage`.
- One namespace per tenant, `tenant-<cluster>`, with one `RoleBinding`. Namespace scoping *is* the
  isolation mechanism.
- The apiserver audit log is the volume-operation audit trail.
- `ValidatingAdmissionPolicy` (CEL) gives synchronous per-object rejection; the operator re-checks
  everything and is the final authority.

Two properties of the existing code make this cheap, and both are load-bearing:

1. **The node plugin needs no changes.** It makes no Proxmox calls — only the Kubernetes API for
   topology labels and local block devices. Every failure mode below is therefore control-plane
   degradation, never data-plane loss.
2. **The volumeID format is a stable contract** — `<region>/<zone>/<storage>/<disk>`. If the remote
   backend returns byte-identical volumeIDs, existing PersistentVolumes keep working and the node
   plugin never learns anything changed. This is what makes adopting the ~86 existing PVs possible
   without recreating one of them.

## Alternatives considered

### `local` PersistentVolumes in the management cluster as the ledger

Rejected on every axis that matters:

- A PV's lifecycle is owned by `kube-controller-manager`. Its binding state machine, reclaim policy
  and `kubernetes.io/pv-protection` finalizer would fight a controller that wants the object to
  mean "a Proxmox disk owned by tenant X".
- `PersistentVolumeSpec` is largely immutable after binding. Attachment state, target VMID and
  reassignment progress are exactly the high-churn fields this system must write.
- A `local` volume source carries a real node affinity and a real path on a real node. In the
  management cluster that is semantically false, and the scheduler would honour it if anything ever
  bound to it.
- You cannot add fields. Owner, VMID and quota accounting all land in annotations: no schema
  validation, no defaulting, no status subresource, no printer columns, no field selectors.

### A purpose-built HTTPS service in front of Proxmox

Rejected because it means writing and securing the parts the apiserver already provides and has
already had audited: authentication, authorization, admission, audit logging, watch semantics,
optimistic concurrency, and a CLI. The CRD approach spends its complexity budget on the domain
logic instead.

### One repository, two modes

Rejected in favour of two repositories split along the trust boundary. `proxmox-csi-service` is the
only thing that ever holds a Proxmox credential, which is a property a reader can check by looking
at one repo. The cost is real and is accepted: a change spanning `pkg/utils/volume` and a
reconciler is now two pull requests and a tag. See "Consequences".

### Running both backends during migration

Rejected outright. Two writers against the same disks with no shared lock is precisely the
split-brain this project exists to remove. The cut-over is a short single-writer gap in a
maintenance window instead.

## Consequences

**Gained**

- Per-tenant isolation enforced by RBAC rather than by a naming convention.
- An ownership ledger, and with it quotas, drift detection and an answer to "who owns this disk".
- The audit trail names a Kubernetes identity and namespace, not a PVE token.
- `VMLocks` in the plugin is a *process-local* mutex today, so N controllers against one Proxmox
  cluster can genuinely race on VM configs. Consolidating writes behind one leader-elected operator
  makes that lock globally correct for the first time.
- The cross-tenant clone hole closes: `spec.source` is a name reference inside the tenant's own
  namespace, never an attacker-chosen volume handle.
- `CreateSnapshot` stops being a 3600-second blocking gRPC call.

**Paid**

- **The management cluster becomes a platform dependency.** No new provisioning anywhere while it
  is down. See the degradation matrix in `docs/volume-control-plane.md`.
- **A bootstrap ordering constraint.** Nothing on the operator's own path to readiness may be
  PVC-backed by this driver. This is a checked gate in the cut-over runbook, not a convention.
- **Version skew between two modules.** The operator and the tenant driver must agree on volumeID
  construction. The byte-identity golden table is checked into both repos and a diff in it is a
  release blocker, not a test to update.
- Asynchronous provisioning: RPCs return retryable codes and the sidecars retry. Deterministic
  resource names are what make that safe.
