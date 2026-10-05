# The volume control plane

How tenant clusters get Proxmox volumes without holding a Proxmox credential.

This document is the design of record: the resource model, the rule that governs every reconciler,
the threat model, and what happens when the management cluster is unavailable. The decisions behind
it are in [ADR 1](adr/0001-the-management-apiserver-is-the-volume-api.md),
[ADR 2](adr/0002-federate-tenant-service-account-tokens-directly.md),
[ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) and
[ADR 4](adr/0004-tenants-authenticate-with-keycloak-client-credentials.md). ADR 3 and ADR 4 replace
the tenant-facing half of ADR 1 and ADR 2: tenants no longer touch the management apiserver, and
this document describes the design as it stands after them.

## Shape

```
┌─ bne1-cluster1 (tenant) ─────────┐          ┌─ syd1-mgt1 (management) ──────────────┐
│                                  │          │                                       │
│  csi-controller                  │          │  keycloak ◄── client_credentials ──┐  │
│   └ remote backend ──────────────┼──┐       │   └ realm JWKS ──┐                 │  │
│      (gRPC client)               │  │       │                  ▼                 │  │
│                                  │  │ OAuth2│  proxmox-volume-api ───────────────┘  │
│  csi-node (DaemonSet)            │  └──────►│   ├ azp → TenantCluster → namespace   │
│   └ UNCHANGED — no Proxmox calls │  bearer  │   ├ per-tenant rate limit             │
└──────────────────────────────────┘          │   └ request audit                     │
                                              │            │ ProxmoxVolume CR         │
   no route to the apiserver,                 │            ▼                          │
   no kube identity, no RBAC                  │  kube-apiserver (internal only)       │
                                              │   ├ ValidatingAdmissionPolicy (CEL)   │
                                              │   ├ ResourceQuota, resourceVersion CAS│
                                              │   └ audit log                         │
                                              │            │ watch                    │
                                              │            ▼                          │
                                              │  proxmox-volume-operator              │
                                              │   └ the ONLY Proxmox credential ──────┼──► PVE
                                              │  pvecsictl migrator                   │
                                              └───────────────────────────────────────┘
```

Three facts to keep in mind while reading the rest:

- **The node plugin is untouched and unaware.** It never called Proxmox. Every failure below is
  control-plane degradation; none of it can unmount a running pod's volume.
- **Volume handles are byte-identical to the ones the local backend produces.** That is what lets an
  existing cluster switch over without recreating a single PersistentVolume.
- **The CSI driver name does not change.** `csi.proxmox.sinextra.dev` is written into `spec.csi.driver`
  on every existing PV and into every `VolumeAttachment` and `StorageClass`, none of them mutable.
  Only the CRD group is new.

## The resource model

Group `csi.crunchymonkies.com/v1alpha1`.

| Kind | Scope | Short names | Written by | Role |
|---|---|---|---|---|
| `TenantCluster` | cluster | `pxtenant` | operator/admin | registration and policy: identity, owned VMIDs, allowed storages, parameter policy, quotas, mode |
| `ProxmoxVolume` | namespaced | `pxvol` | the API, for a tenant | one Proxmox disk. Named after the PV (`pvc-<uuid>`), which is what makes `CreateVolume` idempotent under CSI retries |
| `ProxmoxVolumeAttachment` | namespaced | `pxattach` | the API, for a tenant | one attachment. Separate lifecycle, separate sidecar, separate admission gate |
| `ProxmoxVolumeSnapshot` | namespaced | `pxsnap` | the API, for a tenant | one snapshot |
| `ProxmoxStorage` | cluster | `pxstore` | operator | the storage catalog. Serves `GetCapacity` from a watch cache, streamed to tenants by the API |

No tenant writes any of these directly. Since
[ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) the write is performed
by `proxmox-volume-api` on the tenant's behalf, into the namespace the token resolved to. The kinds,
their fields and their semantics are unchanged by that — the API's methods map one-to-one onto these
objects, and everything below the API reads as it always did.

Attachment is a separate kind on purpose. Attach/detach is high-churn and driven by a different
sidecar than provisioning, so keeping it apart means attach churn never conflicts with a concurrent
expand or modify on the volume object — and it lets admission policy gate *attach*, the operation
that actually crosses a tenant boundary, on its own terms. Every field of its spec is immutable: an
attachment is created and deleted, never edited.

`ProxmoxVolumeAttachment` carries `spec.volumeID` redundantly, and the operator refuses it unless
that value equals the referenced volume's `status.volumeID` **and** the volume is in the same
namespace. Two independent checks, so pointing a resource you own at a handle you do not gets you
nothing.

### `TenantCluster.spec.mode`

Every reconciler consults this before doing anything:

| Mode | Operator behavior |
|---|---|
| `Observe` (default) | never mutates Proxmox. Adopts, resolves, reports. The cut-over state |
| `Enforce` | normal operation |
| `Suspended` | rejects new work; leaves existing volumes alone |
| `Decommission` | finalizers may delete real disks |

A new tenant lands in `Observe` and stays there until its ledger has been eyeballed. Deleting a disk
requires an explicit, separate transition to `Decommission`.

## The rule: the ledger is authoritative

Every reconciler follows from this one sentence, so it is worth stating precisely.

**The `ProxmoxVolume` resource, not anything in Proxmox, is the record of who owns a disk.** Disk
names, PVE pools and tags are corroborating evidence and enforcement points; they are never the
source of truth.

Three consequences:

1. **`controllerVmID` is advisory.** Two clusters currently share the default placeholder `9999`, so
   their at-rest disks are indistinguishable by name. The tempting fix — assign distinct IDs and
   rename ~86 disks — is wrong: a rename is a real Proxmox operation that must happen while detached,
   and it is 86 chances to lose a volume on a code path that already has a data-loss incident in its
   history. Instead: adopted volumes keep whatever VMID they carry, recorded in `status.ownerVMID` and
   explicitly permitted by `spec.legacyControllerVMIDs`; new volumes get their own tenant's
   placeholder; the old names age out. Only the drift detector consults naming at all.
2. **Usage accounting is derived, never accumulated.** On startup and every resync the operator
   rebuilds quota usage by listing `ProxmoxVolume` objects. A crash mid-reservation therefore
   self-heals: an orphaned reservation for a volume that does not exist is dropped, and a volume that
   exists is always counted.
3. **Drift is reported, never auto-corrected.** The drift detector runs both directions on a resync
   and sets `Degraded`. It does not mutate and it does not delete. A disk in Proxmox with no
   resource, or a resource whose disk has vanished, is a human's problem — the alternative is a
   reconciler that deletes real data because it read a stale list.

The ledger is also the thing that must be *committed before the side effect*: the operator does not
call Proxmox until the status update recording this volume's bytes has succeeded. That update is a
compare-and-swap on `resourceVersion`, so two racing reservations cannot both land on the same base
version — one gets `409 Conflict`, re-reads and re-evaluates.

None of this section changed when the tenant interface did. The ledger is still custom resources in
the management apiserver, the reservation is still a `resourceVersion` compare-and-swap, and the
component doing both is still the operator. The API in front writes the tenant's *intent*; it does
not reserve, does not account and does not call Proxmox.

## Authorization: four layers

They compose as defense in depth, not as alternatives. Each is listed with what it catches that the
others do not.

1. **The ledger (authoritative), reached only through the API.** Namespace scoping is still the
   isolation mechanism; what changed is who enforces it. A request carries no namespace, no
   identity and no owner — the API derives all three from the token, so there is no field for a
   tenant to forge. A cluster-wide unique index over `status.volumeID` refuses a second claim on
   the same handle (`Degraded/DuplicateClaim`) — which is also how you discover whether two
   clusters already share a disk today.

   Be clear-eyed about what this layer costs. Before
   [ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) the apiserver's own
   RBAC stood between one tenant and another's ledger, and a bug in our code could not move it.
   Now one ServiceAccount can write into every tenant namespace and a confused deputy in the API's
   resolution is a cross-tenant compromise. **That is precisely why layers 2 and 3 are not
   belt-and-braces: they are the only layers that survive this one being wrong.**
2. **PVE pool and VM tags.** Each tenant gets a pool; the operator refuses an attach whose target
   VMID is not in `status.resolvedVMIDs`. **This layer is mandatory, not belt-and-braces:** the tenant
   resolves nodeID → VMID from its *own* Node objects, so a compromised tenant controller can request
   an attach to any VMID it likes. Membership is refreshed on a timer and `vmIDsObservedAt` is
   tracked, so a stale resolution refuses the attach rather than trusting an old answer.
3. **Naming convention.** Each tenant gets a unique placeholder VMID. The operator refuses to act on
   a disk whose embedded VMID is neither the tenant's placeholder nor one of its owned VMIDs. Weakest
   layer, and the only one that survives the CRD layer being bypassed entirely.
4. **Quotas** — below.

**Enforcement points, in the order a request meets them.**

1. **The API, synchronously, before anything is written.** Token validation and the `azp` →
   `TenantCluster` → namespace resolution
   ([ADR 4](adr/0004-tenants-authenticate-with-keycloak-client-credentials.md)), the per-tenant rate
   limit, and the request-shape checks it can make without reading Proxmox.
   A refusal here is a gRPC status the sidecar can act on, which is better UX than a rejected
   write, and it is the only layer that sees the request before it becomes an object.
2. **`ValidatingAdmissionPolicy` (CEL), on the write.** Unchanged: `spec.claimRef` consistency,
   `spec.storage ∈ allowedStorages`, parameter-policy caps and
   `attachment.spec.vmid ∈ resolvedVMIDs`. Its job changed rather than its content. It no longer
   defends against a hostile tenant — no tenant can write a CR — it defends against a bug in the
   API, running the same rules in a process the API cannot talk its way past. Keep the two in sync
   deliberately: identical rules in two places is the point, not duplication to be refactored away.
3. **The operator, before touching Proxmox.** **It re-checks everything and is the final
   authority** — admission alone cannot be trusted for a resource whose truth lives outside the
   cluster.

Two hazards no single layer catches, both covered by a template-time check plus a startup check: two
tenants sharing a namespace, which silently gives each the other's volumes, and a copy-pasted
registration whose identity was never changed, which does the same thing one layer up. The chart
refuses a duplicate namespace or identity in `tenants[]` at template time; the API refuses to start
if a Keycloak client id maps to more than one `TenantCluster`, or if two registrations share a
namespace. The `tenants[].identity` value in the chart and `TenantCluster.spec.subject` must match,
and CI asserts it.

## Quotas: three layers

Neither tenant-level nor namespace-level quota is expressible with native `ResourceQuota` alone — it
can cap *object count* in the management namespace but not aggregate capacity, and it cannot see the
tenant-side PVC namespace at all. So:

1. **Native `ResourceQuota` on `count/proxmoxvolumes.csi.crunchymonkies.com`.** The apiserver's quota
   admission plugin already handles concurrency correctly. This is the only layer that is both
   synchronous and race-free, so use it as a generous fuse against a runaway provisioner loop, not as
   a business rule. Zero code.
2. **`ValidatingAdmissionPolicy` for stateless per-object ceilings** — storage allow-list, VMID
   allow-list, parameter caps, and a per-volume `maxVolumeBytes`. That last one catches the
   single-40TiB-PVC mistake synchronously, which an aggregate quota inherently cannot.
3. **The operator ledger, authoritative for aggregate capacity and per-namespace usage.** Correct
   under concurrency for the three reasons in "the ledger is authoritative" above.

All three still work unchanged with the API in front, because none of them cares who wrote the
object: `ResourceQuota` counts objects in a namespace regardless of the writer, the CEL policies run
on the write, and the ledger is rebuilt by listing. The API adds a fourth thing that is not a quota
and should not be mistaken for one — a per-tenant request rate limit, which bounds how fast a
looping provisioner can ask, not how much it can have.

A rejected volume lands `phase: Rejected`, reason `QuotaExceeded`. `CreateVolume` returns
`codes.ResourceExhausted`, external-provisioner records `ProvisioningFailed` on the PVC and backs
off, and the PVC stays `Pending` with a readable reason. That is how EBS and PD report quota.
**Do not try to make it synchronous.** An admission-time check against cached usage is acceptable
purely as UX sugar, but it is stale by construction and must be commented as advisory.

> **Namespace quota is fairness, not security.** `claimRef.namespace` is *tenant-asserted* — it
> arrives as a field of the API request and a compromised tenant controller can put any string in
> it. What it cannot do is exceed its
> **tenant** budget, which is the real boundary. Do not build a compliance story on the namespace
> number.

> **Prerequisite:** `csi-provisioner` only passes `csi.storage.k8s.io/pvc/namespace` when started
> with `--extra-create-metadata`, which the tenant chart does not set today. Without it,
> `claimRef.namespace` is empty and namespace quotas silently enforce nothing. The operator sets
> `Admitted=False/InvalidSpec` when namespace quotas are configured and `claimRef` is absent — fail
> loud, not silent.

## Threat model

**Assets, in order:** tenant data on Proxmox disks; the Proxmox credential; the integrity of the
ownership ledger; availability of volume provisioning.

**Trust boundaries:** tenant cluster → volume API (OAuth2, per-tenant client); volume API →
management apiserver (one ServiceAccount, all tenant namespaces); management apiserver → operator
(RBAC); operator → Proxmox (the one credential).

The second of those is new and is the weakest link inside the management cluster: one identity that
can write into every tenant namespace, with nothing in Kubernetes behind it to catch a mistake in
which namespace it chose. It is why the PVE pool check and the VMID naming check are load-bearing
rather than defence in depth — see layer 1 of "Authorization" above.

**Assumed trusted:** the management cluster's control plane and etcd; the Keycloak realm that issues
tenant tokens; the Proxmox cluster itself; whoever has root on a tenant's control-plane nodes *for
that tenant only*.

**Primary adversary:** a compromised tenant CSI controller, or a cluster-admin on one tenant cluster,
attempting to reach another tenant's data or the hypervisor.

| Attack | Mitigation | Residual |
|---|---|---|
| Attach another tenant's disk to my VM | the API pins the namespace from the token, not from the request; volume/attachment same-namespace check; `volumeID` equality check | none via the API. Requires the other tenant's client secret |
| Attach my disk to another tenant's VM | operator checks target VMID against live pool membership (layer 2), not against tenant-supplied data | a VMID moved between pools between refresh and attach; bounded by `vmIDsObservedAt` staleness limit |
| Clone or restore from a volume I do not own | `spec.source` is a name reference within my own namespace; raw handles are not accepted | none. This closes a hole that exists today |
| Impersonate another tenant | `azp` selects a registration row; nothing in the token body supplies the namespace, quota or mode | **weaker than before ADR 4:** requires the victim's Keycloak client secret, readable by any cluster-admin on that tenant, where the `jwt[]` design required its service-account signing key |
| Steal a tenant's client secret | one client per tenant, so blast radius is that tenant; short token lifetime; delivery by SealedSecret, never a chart value | a cluster-admin on a tenant can always read it. Detection, rotation and the `TenantCluster` kill switch, not prevention |
| Replay a captured access token | TLS on the API; short `exp` | valid until expiry. Deleting the `TenantCluster` registration refuses it immediately, which is why that is the authoritative revocation and disabling the Keycloak client is not |
| Present a token minted for another relying party in the realm | `aud` must contain the audience the API owns; a missing audience mapper fails closed at startup | none, provided the check exists. This is why its absence is a startup failure and not a warning |
| Confused deputy: get the API to write into another tenant's namespace | namespace is derived from the token and never read from the request; startup refuses duplicate client ids or shared namespaces | a bug in the API. Layers 2 and 3 (PVE pool membership, VMID naming) are what remain |
| Compromise the Keycloak realm | out of the tenant's reach; realm config version controlled and reproducible | total: the realm can mint a token for any tenant. Ranks with management-cluster compromise |
| Exhaust storage for everyone | tenant quota (authoritative) + object-count fuse + per-volume ceiling | over-provision within an approved tenant budget |
| Forge `claimRef.namespace` to dodge a namespace quota | none — it is tenant-asserted | accepted and documented: fairness, not security |
| Steal the Proxmox credential | it exists in exactly one namespace in one cluster, mounted by one deployment | management cluster compromise is total compromise. Explicitly in scope for etcd hardening, out of scope here |
| Bypass the CRD layer with a stolen PVE token | pool-scoped tokens and per-tenant pools (Phase 0) limit blast radius | PVE ACL paths cannot express per-volume ownership inside a shared storage; a tenant with allocate rights on a storage can still destroy another tenant's disk there |
| Destroy the ledger via `helm uninstall` | CRDs live in the chart's `crds/`, which Helm never deletes; finalizers on every object | deliberate `kubectl delete crd` |
| Silent revocation failure | offboarding removes the `TenantCluster`, which the API refuses against on the next request regardless of any token in flight | disabling the Keycloak client alone leaves issued tokens working until `exp`; removing the registration is the step that counts |

**Explicitly out of scope.** Malicious hypervisor or Proxmox operator. A malicious management-cluster
admin. A malicious Keycloak realm administrator. Denial of service against the volume API by an
authenticated tenant beyond what quota and rate limits cover. Data-at-rest encryption on Proxmox
storage.

**Note on residual code.** A tenant binary still *contains* the Proxmox client code; isolation comes
from the absence of a credential and from network policy, not from the absence of code. A build tag
would shrink that, and it is a follow-up rather than a blocker.

## Degradation matrix

What happens when the management cluster is unavailable:

| | |
|---|---|
| Running pods with mounted volumes | **unaffected** — the node plugin never talks to the management cluster |
| Pod restart on the same node | unaffected — the attachment already exists |
| Pod reschedule to another node | **blocked** — needs a detach and an attach, both writes |
| New provisioning, expansion, deletion | **blocked**; PVs stay `Released` |
| `GetCapacity` | served from the informer cache until the staleness limit |
| Snapshot creation | blocked |
| Migration (pod-follow, node evacuation) | policy still evaluates tenant-side; execution blocked |

[ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) and
[ADR 4](adr/0004-tenants-authenticate-with-keycloak-client-credentials.md) add two components that
can fail independently of the apiserver behind them, so the table has two more rows:

| | |
|---|---|
| The volume API is down | every row above that says "blocked" is blocked, for the same reason and with the same data-plane safety. The operator keeps reconciling whatever is already in the ledger, so work already accepted still completes |
| Keycloak is down | tenants holding an unexpired access token keep working for the rest of its lifetime; the rest fail to authenticate and see the same blocked set. **ADR 2 named this exactly: a Keycloak outage is now "storage provisioning is down in every cluster", not "nobody can log in"** |

The access-token lifetime is therefore two knobs in one, pulling opposite ways: it is how long a
tenant rides out a Keycloak outage, and it is the window in which a revoked tenant still works.
Pick it knowing that, and note that deleting the `TenantCluster` registration cuts a tenant off
immediately either way.

This is real coupling and it should be stated plainly rather than discovered: **the failure mode is
degradation of the control plane, never data-plane loss.** Keeping the node plugin unchanged is what
buys that, and it is why no change to it is acceptable without revisiting this table. Adding the API
in front does not weaken it — the node plugin still talks to nothing, so a total outage of Keycloak,
the API, the apiserver and the operator together still cannot unmount a running pod's volume.

The drill is part of verification: scale the operator to zero, confirm running pods keep their
volumes, confirm only new provisioning blocks.

## Bootstrap constraints

The management cluster runs as VMs on the Proxmox cluster it controls, and it is tenant zero — it
runs the CSI plugin itself and should go through the same path as everyone else, otherwise there are
two credential holders on day one and the central invariant is false immediately.

The circularity is mostly illusory: a cluster pointing its own CSI controller at its own volume API
adds no cross-cluster dependency, and a management cluster that is down cannot serve its own volume
requests either way. The one real constraint is ordering:

- **Nothing on the path to readiness of the operator, the volume API or Keycloak may be PVC-backed
  by this driver.** Use the bundled `local-path` provisioner or an external Postgres.

  Keycloak is the sharp one, and
  [ADR 4](adr/0004-tenants-authenticate-with-keycloak-client-credentials.md) is where it is
  argued. ADR 2 wrote this rule for a design that did not strictly need it, because Keycloak was
  not in the machine path. It is now, so the deadlock it described is live: no Keycloak
  → no token → no volume → no database → no Keycloak. Nothing recovers that without manual
  intervention on a cluster that is already down. **This is a checked gate, not a convention.**
- **The management cluster flips last**, after every tenant.
- The gate is checked, not remembered:
  `kubectl get pv -o custom-columns=NAME:.metadata.name,DRIVER:.spec.csi.driver,CLAIM:.spec.claimRef`
  must show nothing from `csi.proxmox.sinextra.dev` in the platform namespaces before cut-over —
  and the namespaces checked must include Keycloak's and the volume API's, not only the operator's.

## Audit

The audit trail is two logs that join, and it has to be read as one:

- **The volume API's request log.** One structured record per request: the Keycloak client, the
  tenant it resolved to, the method, the arguments that matter, the decision and a request id.
  This is the only log that sees a *refused* request, since a refusal never becomes an object.
- **The management apiserver's audit log.** Every create, delete and patch that actually landed.
  Since [ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) these are all
  attributed to the API's ServiceAccount, so on its own it no longer says which tenant asked. The
  API closes that by stamping `csi.crunchymonkies.com/tenant` and the request id onto every object
  it writes, which is what makes the join possible.

Two honest caveats, because this is weaker than what ADR 1 banked:

- **The join has a seam.** A crash between the apiserver write and the API's log line leaves an
  object with no request record. The apiserver's own entry survives, so nothing is invisible, but
  the attribution for that one write has to be recovered from the label rather than read off a
  log line.
- **Pod-level attribution is gone.** This section used to carry the calling pod in
  `extra["csi.crunchymonkies.com/tenant-pod"]`, read from a service-account token's
  `kubernetes.io.pod` claim. A client-credentials token has no such claim. A tenant may send a pod
  hint and the API logs it, but it is tenant-asserted and must be treated as such — the same
  standing as `claimRef.namespace`.

Both are still a strict improvement over what exists today, where PVE records only that some token
did something, with no way back to a Kubernetes identity or namespace at all.

## Related

- [ADR 1 — the management apiserver is the volume API](adr/0001-the-management-apiserver-is-the-volume-api.md)
  — superseded in part by ADR 3; everything except the tenant interface still governs
- [ADR 2 — federate tenant service-account tokens directly](adr/0002-federate-tenant-service-account-tokens-directly.md)
  — superseded by ADR 4, and worth reading for the objections ADR 4 has to answer
- [ADR 3 — tenants reach the volume API through an OAuth2 service](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md)
- [ADR 4 — tenants authenticate with Keycloak client credentials](adr/0004-tenants-authenticate-with-keycloak-client-credentials.md)
- `proxmox-csi-plugin/docs/specifications.md` — the volumeID format and the `controllerVmID`
  convention this design supersedes as an ownership mechanism
