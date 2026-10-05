# 4. Tenants authenticate with Keycloak client credentials

- **Status:** accepted
- **Date:** 2026-08-23
- **Supersedes:** [ADR 2](0002-federate-tenant-service-account-tokens-directly.md)
- **Related:** [ADR 3](0003-tenants-reach-the-volume-api-through-an-oauth2-service.md),
  [`docs/volume-control-plane.md`](../volume-control-plane.md)

## Context

[ADR 3](0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) moves the tenant interface
off the management apiserver and onto a service this repository runs. That deletes the mechanism
ADR 2 chose: there is no apiserver in the tenant's path any more, so there is no
`AuthenticationConfiguration` and no `jwt[]` authenticator to federate into. The token question has
to be answered again for a relying party that is our own process.

ADR 2 answered it under a requirement this record changes. It opened with: **"no tenant cluster
holds a long-lived credential of any kind — not a Proxmox token, and not a client secret that
replaces it"**, and it rejected Keycloak in the machine path partly on that ground. That
requirement is relaxed here, deliberately and with the cost stated. It is the reason ADR 2 is
superseded rather than amended: the reasoning in it is sound and still readable, and it was
reasoning about a different requirement.

Keycloak is already being deployed on the management cluster for human and `kubectl`
authentication — ADR 2 noted it "is being deployed regardless". It is now also the machine
authority.

## Decision

**Each tenant cluster gets one confidential Keycloak client. Its CSI controller obtains an access
token with the `client_credentials` grant and presents it as a bearer token to the volume API. The
service validates the token against the realm's JWKS and maps the client to exactly one registered
`TenantCluster`.**

What the service checks on every request, in order, because this list is what replaces the security
property ADR 2 relied on:

1. Signature against the realm JWKS, with the key selected by `kid` and the JWKS cached and
   refreshed on unknown-`kid` only.
2. `iss` equals the configured realm issuer. Exactly one issuer is configured; there is no
   per-tenant issuer and therefore no issuer-selects-the-key step to get wrong.
3. `aud` contains the audience the service owns. Keycloak does not put a resource audience in a
   `client_credentials` token by default — it emits `account` — so each tenant client needs an
   explicit audience mapper. A service that accepts a token minted for a different relying party in
   the same realm is the whole attack, so this check is not optional and its absence must fail
   closed at startup, not per request.
4. `exp`, `nbf` and `iat` within a small clock skew.
5. `azp` — the client id — looked up against the registered tenants. **Not one claim in the token
   selects the namespace, the quota or the mode.** The claim selects a row; the row carries the
   authority. This is ADR 2's "nothing in the token body influences the username" property,
   relocated from the apiserver's stanza table into the service's registration lookup.

An unknown, disabled or ambiguous `azp` is a refusal, and a client id that maps to two
`TenantCluster` objects is a startup failure rather than a request-time coin flip — the same class
of mistake as ADR 2's copy-pasted username literal, caught the same way.

ADR 2's rule to **bind on the `pvx:<cluster>` string, never on an issuer URL and never on a
Keycloak client ID**, pays off here exactly as it was meant to. The string survives as the tenant's
identity in `TenantCluster.spec.subject`, in the audit record and in the label the service stamps
on every object it writes. The client id maps *to* it and is replaceable without touching anything
downstream.

## Answering ADR 2's objections

ADR 2 rejected Keycloak in the machine path on four grounds. Three are answered; one is conceded
and one is now actively working against us.

**1. "The mechanism does not exist in a supported form."** — resolved, by not using that mechanism.
ADR 2 was assessing RFC 8693 token exchange, where a tenant SA token is traded for a Keycloak
token: v1 is preview and deprecated in 26.6, v2 accepts only a Keycloak access token as subject.
`client_credentials` is a first-class, non-preview grant in every Keycloak version. It needs
neither exchange nor the Federated Client Authentication work ADR 2 was waiting on.

**2. "The supported alternative reintroduces the secret."** — conceded in full. A confidential
client means a long-lived client secret sitting in every tenant cluster, which is the property ADR 2
existed to prevent. What is bought for it is ADR 3's requirement: no tenant holds a
management-cluster Kubernetes identity. What is paid is that a cluster-admin on any tenant can read
that tenant's secret. The mitigations are ordinary and all of them are load-bearing:

- One client per tenant, so the blast radius of a leaked secret is one tenant's namespace — the
  same boundary the old `jwt[]` stanza gave, reached by a cheaper attack.
- A short access-token lifetime, because it is also the revocation window; see Consequences.
- Delivery by `SealedSecret` or external-secrets into the tenant, never as a chart value and never
  in the tenant's git history.
- A written rotation procedure, practised during onboarding. Keycloak supports two active secrets
  during rotation; a procedure that requires a simultaneous restart is a procedure nobody runs.
- The secret authenticates a client to Keycloak and nothing else. It is not a Proxmox credential
  and it is not a Kubernetes credential, so the one-credential invariant of
  [ADR 1](0001-the-management-apiserver-is-the-volume-api.md) is untouched.

**3. "It removes none of the prerequisite work."** — inverted; this one is now a gain. ADR 2's
prerequisite was that every tenant publish a static OIDC discovery mirror at
`https://oidc.<cluster>.ouchi.com.au/`, because RKE2 defaults `--service-account-issuer` to a value
that is byte-identical on every cluster and `issuer.url` must be unique per authenticator. ADR 2
recorded the consequence bluntly: *"a second tenant cannot be registered until this changes."*
Nothing reads a tenant's discovery document any more. **That prerequisite is deleted, and with it
the blocker on registering a second tenant.** Tenant onboarding becomes: create a Keycloak client,
create a `TenantCluster`, ship the secret — no file on every management server node, and no hot
reload that can silently half-land.

**4. "Co-locating it creates a hard bootstrap cycle, and this is decisive."** — not answered.
Accepted, and it is the sharpest cost in this record. ADR 2's deadlock is now live: *"no Keycloak →
no token → no volume → no database → no Keycloak."* Two things follow, and both are gates rather
than guidance:

- **Keycloak's storage, and everything on the volume API service's path to readiness, must not be
  PVC-backed by this driver.** ADR 2 already wrote this rule for a design that did not strictly
  need it. It needs it now. Use the bundled `local-path` provisioner or an external Postgres, and
  check it before cut-over rather than remembering it.
- **A Keycloak outage is now "storage provisioning is down in every cluster", not "nobody can log
  in".** ADR 2 called that "a worse failure mode for no gain at this scale". The gain has changed;
  the failure mode has not. It belongs in the degradation matrix and in whatever pages on Keycloak.

The one thing that softens it: an access token is valid until it expires, so a tenant that already
holds one rides out a short Keycloak outage. This is the same knob as the revocation window, pulling
the other way, and picking its value is picking which of the two matters more.

## Consequences

- **Revocation is weaker and slower.** ADR 2 revoked by removing a stanza, and observed that
  Keycloak is worse here because "disabling a client does not invalidate already-issued access
  tokens". That is now our revocation story: disabling or deleting a client stops new tokens, and
  existing ones work until `exp`. The residual window is the access-token lifetime and must be
  chosen as such. Offboarding therefore has a second step — remove the tenant's `TenantCluster`
  registration, which the service rejects against immediately regardless of any token in flight.
  That, not the Keycloak click, is the authoritative revocation.
- **The alert changes.** ADR 2 named
  `apiserver_authentication_config_controller_automatic_reload_success_total` "the most important
  alert in the design". It is now irrelevant to tenants. What replaces it is Keycloak availability
  and the service's own token-rejection rate — a spike in rejections is either a rotation that
  half-landed or someone trying keys.
- **Keycloak's realm configuration becomes part of the system's definition.** Client list, audience
  mappers, token lifetimes and the service-account roles are configuration that must be version
  controlled and reproducible, not clicked. A realm restored from a backup that predates a tenant is
  an outage for that tenant.
- **CI's assertion moves.** ADR 2 had CI assert that `jwt[]` username literals are unique and match
  `^"pvx:[a-z0-9-]+"$`. The equivalent invariant is that each Keycloak client id maps to exactly one
  `TenantCluster` and each `TenantCluster.spec.subject` is unique — checkable against the cluster
  rather than against a config file, and also enforced at service startup.
- **Owed schema and chart work, named here and done in a later pass.**
  `api/csi/v1alpha1/tenantcluster_types.go` carries `spec.subject`, documented as "must match the
  username literal in the apiserver's `jwt[]` stanza", and `spec.issuer`, the tenant's own OIDC
  issuer "recorded for audit". Both need re-meaning: `subject` stays the `pvx:<cluster>` identity
  string but stops being an RBAC subject, and `issuer` is replaced by the client id the service maps
  on. In the chart, `templates/tenant-rbac.yaml` renders a `Role`, a `RoleBinding` to
  `kind: User pvx:<cluster>` and a tenant-catalog `ClusterRole`/`ClusterRoleBinding` bound to the
  group `pvx:tenant-clusters`; with no tenant Kubernetes identity left, all four are removed and the
  namespace, its labels and the `ResourceQuota` stay. The three-places-must-match instruction is
  written down twice more — `templates/_helpers.tpl` and `README.md.gotmpl`, from which
  `charts/proxmox-csi-operator/README.md` is generated — and becomes two places, the chart's
  `tenants[].identity` and `TenantCluster.spec.subject`.
