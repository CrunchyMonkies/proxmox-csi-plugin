# 3. Tenants reach the volume API through an OAuth2 service, not the apiserver

- **Status:** accepted
- **Date:** 2026-08-23
- **Supersedes:** [ADR 1](0001-the-management-apiserver-is-the-volume-api.md), in part — see
  "What of ADR 1 survives"
- **Related:** [ADR 4](0004-tenants-authenticate-with-keycloak-client-credentials.md),
  [`docs/volume-control-plane.md`](../volume-control-plane.md)

## Context

ADR 1 decided that **"tenant clusters request volumes by creating custom resources in the
management cluster. The management cluster's apiserver is the interface; there is no other service
and no bespoke wire protocol."** ADR 2 then gave each tenant a Kubernetes identity on that
apiserver, one `jwt[]` authenticator per cluster.

The requirement has changed: **no tenant cluster may hold a management-cluster Kubernetes identity,
and no tenant cluster may have a network path to the management apiserver.** Tenants reach the
volume control plane through an authenticated API endpoint this repository serves, and nothing
else.

The reason is the size of the thing being exposed. An apiserver reachable from every tenant network
is a surface no one can enumerate: hundreds of built-in resource handlers, discovery, the
aggregation layer, admission webhooks, `SelfSubjectAccessReview`, watch on anything RBAC forgot to
exclude. RBAC
narrows what a caller is *allowed* to reach; it does not narrow what the process will *parse* on
behalf of an unauthenticated or partly-authenticated request, and it is the parsing surface that
carries the CVEs. Every tenant credential is another cluster whose compromise puts an authenticated
caller on the estate's single most valuable control plane. A service with a fixed method list has a
surface a reader can count, and it can sit in a DMZ with the apiserver on an interface tenants
cannot route to at all.

ADR 1's counter-argument was not wrong and is not being disowned. It rejected a bespoke service
because that means writing and securing "authentication, authorization, admission, audit logging,
watch semantics, optimistic concurrency, and a CLI" that the apiserver already provides and has
already had audited. That cost is real, it is now being paid, and the honest version of this
decision is a list of which of those the service genuinely owes and which it still delegates:

| Owed by the service | Still the apiserver's |
|---|---|
| authentication (OAuth2 bearer, [ADR 4](0004-tenants-authenticate-with-keycloak-client-credentials.md)) | optimistic concurrency — `resourceVersion` CAS, which is what makes the ledger's reservation race safe |
| authorization: caller → exactly one `TenantCluster` → exactly one namespace | `ValidatingAdmissionPolicy` (CEL), still enforced on every write |
| a request audit trail, and the join back to the apiserver's | the `ResourceQuota` object-count fuse, which counts objects in a namespace regardless of who wrote them |
| rate limiting per tenant | watch semantics, which the reconcilers still consume unchanged |
| the wire contract, its versioning, and skew between two repos | `kubectl` for the humans who operate the management cluster |

Five of the seven survive intact because the store did not change. Only the front door did.

## Decision

**Tenant clusters call a versioned gRPC API served by `proxmox-csi-service` over TLS,
authenticated by an OAuth2 bearer token. The service resolves the caller to a registered
`TenantCluster` and performs the custom-resource write in that tenant's namespace using its own
ServiceAccount. Tenants hold no kubeconfig, no `jwt[]` stanza, no `RoleBinding` and no route to the
management apiserver.**

The apiserver does not leave the design; it stops being the interface and becomes the service's
store. The reconcilers watch the same five kinds in the same group and do not know a request came
over the wire.

Concretely:

- The method surface mirrors what the tenant's `remote` backend needs and nothing more: the
  controller RPCs — create, delete, expand, modify, attach, detach, snapshot, list — plus a
  capacity stream that replaces the tenant's watch on `ProxmoxStorage`. It is a fixed list, and its
  being a fixed list is the point.
- Every request carries no namespace, no identity and no owner. The service derives all three from
  the token. A field a tenant cannot send is a field a tenant cannot forge.
- The service's ServiceAccount is namespace-unrestricted across tenant namespaces, which is a
  concentration of authority that did not exist before: it is now the thing standing between one
  tenant and another's ledger. That is the price of the requirement and it is stated in
  Consequences rather than buried.
- The proto lives in this repository and is generated into the plugin, alongside the volumeID
  byte-identity golden table that already crosses the same boundary.

### What of ADR 1 survives

Everything except the interface. The ledger and its authority, the five kinds and the group name,
the `volumeID` byte-identity contract that lets ~86 existing PersistentVolumes carry over, the
untouched node plugin, the one-credential invariant, the leader-elected single writer that makes
`VMLocks` globally correct, and the two-repo split along the trust boundary all stand exactly as
written. What falls is one sentence of ADR 1's Decision — "there is no other service and no bespoke
wire protocol" — and the "purpose-built HTTPS service in front of Proxmox" alternative it rejected.
ADR 1's other three alternatives (`local` PVs as the ledger, one repository with two modes, running
both backends during migration) are unaffected and remain rejected for the reasons given there.

## Alternatives considered

### An OAuth2-terminating reverse proxy that impersonates onto the apiserver

Keeps the Kubernetes wire protocol, so the tenant's `remote` backend needs no change at all, and
`Impersonate-User: pvx:<cluster>` reuses the RBAC that already exists. Rejected because it does not
satisfy the requirement it is meant to satisfy: what reaches the tenant is still a Kubernetes API,
and the proxy must then decide, path by path and verb by verb, which of it to forward. That is the
same unenumerable surface with a filter in front, and a filter that fails open on a path nobody
thought of is worse than no filter, because the design claims the surface is closed.

### An aggregated APIService

`k8s.io/apiserver` gives the machinery — watch, optimistic concurrency, admission, audit — as a
library, and the kinds would keep their shape. Rejected because it is the apiserver's wire protocol
by construction, so the caller still needs a Kubernetes identity and a route to the aggregation
layer, which is the requirement inverted. It is the right answer to a different question, and worth
re-reading if the requirement ever softens.

### REST and JSON rather than gRPC

Rejected, narrowly. The tenant side of this API is a CSI controller that already speaks gRPC
in-process, so gRPC is the format it does not have to translate twice. `GetCapacity` wants a stream
where the CRD path had a watch, and long-polling that over REST is a thing to build rather than a
thing to use. And protobuf gives a contract that mirrors the CRD types field-for-field with a
generated client on both sides, which matters more than usual here because contract skew across two
repositories is already a known failure mode in this design. The cost is that debugging needs
`grpcurl` rather than `curl`; it is accepted.

### Keeping the CRD path open alongside the API

Rejected on ADR 1's own reasoning about running both backends during migration: two writers against
the same objects with no shared lock is the split-brain this project exists to remove. The `jwt[]`
stanzas and tenant `RoleBinding`s are removed at cut-over, not left in place as a fallback.

## Consequences

**Gained**

- A tenant compromise yields a bearer token for a service with a countable method list, not an
  identity on the management apiserver.
- The management apiserver needs no route from any tenant network, which is a firewall rule rather
  than a policy argument.
- Per-tenant rate limiting becomes possible at all. The apiserver's flow control is not shaped for
  "this one tenant is looping".
- Tenant onboarding stops requiring a change to every management server node's
  `AuthenticationConfiguration` file and a hot reload that can silently fail on one node. See
  [ADR 4](0004-tenants-authenticate-with-keycloak-client-credentials.md).

**Paid**

- **The audit trail has to be rebuilt, and it is weaker.** ADR 1 banked "the audit trail names a
  Kubernetes identity and namespace, not a PVE token". Every write now arrives as the service's
  ServiceAccount, so the apiserver audit log attributes all of them to the service. Two mechanisms
  restore the attribution and neither is free: the service emits a structured audit record per
  request naming the client, the tenant, the method and the outcome; and it stamps
  `csi.crunchymonkies.com/tenant` and a request id onto every object it writes, so the apiserver's
  record joins to the service's. The result is a log we operate rather than one the apiserver
  already ships, and it fails differently — an apiserver audit entry cannot be lost by a crash
  between the write and the log line.
- **Pod-level attribution is gone outright.** The design ADR 1 produced carried the calling pod in
  `extra["csi.crunchymonkies.com/tenant-pod"]`, read from the SA token's `kubernetes.io.pod` claim
  — see the Audit section of
  [`docs/volume-control-plane.md`](../volume-control-plane.md).
  A client-credentials token has no such claim. A tenant may assert a pod hint in the request, but
  it is tenant-asserted and must be logged as such — fairness-grade information, not evidence.
- **The service is tier-0.** It is on the path of every provision, attach, expand, delete and
  snapshot. Its availability joins the degradation matrix in
  [`docs/volume-control-plane.md`](../volume-control-plane.md), and it needs the same
  leader-election and bootstrap care the operator already has.
- **The service's ServiceAccount is the new crown jewel.** One identity that can write into every
  tenant namespace. A confused-deputy bug in namespace resolution is a cross-tenant compromise with
  no second layer inside Kubernetes to catch it — which is exactly why layers 2 and 3 of the
  authorization stack, the PVE pool membership check and the VMID naming check, are not optional
  belt-and-braces. They are the only layers that survive the service being wrong.
- **`ValidatingAdmissionPolicy` changes job rather than retiring.** It no longer defends against a
  hostile tenant, because no tenant can write a CR. It now defends against a bug in the service,
  which is a real adversary with a real history. It stays, and the policies stay identical, so the
  same CEL is a second opinion on every write.
- **A second cross-repo contract.** The proto joins the volumeID golden table as a thing a version
  skew can break, with the same rule: a diff in it is a release blocker, not a test to update.
