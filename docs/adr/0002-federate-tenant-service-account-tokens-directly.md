# 2. Federate tenant service-account tokens directly, not through Keycloak

- **Status:** superseded by [ADR 4](0004-tenants-authenticate-with-keycloak-client-credentials.md)
- **Date:** 2026-08-15
- **Related:** [ADR 1](0001-the-management-apiserver-is-the-volume-api.md)

## Context

The requirement is that **no tenant cluster holds a long-lived credential of any kind** — not a
Proxmox token, and not a client secret that replaces it. A tenant should authenticate to the
management cluster with something it already mints and already rotates: its own projected
ServiceAccount token.

Keycloak was the assumed broker, and a new instance is being deployed on the management cluster
regardless, for human and `kubectl` authentication.

## Decision

**The management apiserver validates tenant ServiceAccount tokens directly, via structured
`AuthenticationConfiguration` with one `jwt[]` authenticator per tenant cluster. Keycloak is not in
the machine token path.** Keycloak gets a `jwt[]` stanza of its own, for humans, in the same file.

The secretless property is preserved in full: a tenant presents a projected SA token, audience-
scoped to the management cluster, that its own kubelet rotates.

```yaml
- issuer:
    url: https://oidc.bne1-cluster1.ouchi.com.au
    audiences: [proxmox-cp.syd1.ouchi.com.au]
  claimValidationRules:
    - expression: 'claims.sub == "system:serviceaccount:csi-proxmox:proxmox-csi-controller"'
    - expression: 'has(claims["kubernetes.io"]) && has(claims["kubernetes.io"].pod)'
    - expression: 'double(claims.exp) - double(claims.iat) <= 3900.0'
  claimMappings:
    username: { expression: '"pvx:bne1-cluster1"' }
    groups:   { expression: '["pvx:tenant-clusters"]' }
```

The security property that makes this the stronger option, not merely the simpler one:
`issuer.url` must be unique across authenticators, the stanza is selected by the token's `iss`, and
the signature is then verified against **that stanza's JWKS only**. So the username is a constant
literal — nothing in the token body influences it. Impersonating another tenant requires that
tenant's SA signing key, i.e. root on one of its control-plane nodes. That is the correct boundary,
and the same one a per-cluster mTLS certificate would give without a CA to operate.

(A claim-derived username is not even available: `sub` begins with `system:`, which the JWT
authenticator refuses as a username.)

## Why not Keycloak in the machine path

- **The mechanism does not exist in a supported form.** RFC 8693 external→internal exchange
  (`subject_token_type=…:jwt` + `subject_issuer`) is legacy token-exchange **v1**: preview,
  disabled by default, dependent on the also-deprecated `admin-fine-grained-authz:v1`, and
  explicitly deprecated in Keycloak 26.6. Standard exchange **v2** accepts only a Keycloak access
  token as the subject, so it cannot consume a tenant SA token at all.
- **The supported alternative reintroduces the secret.** The RFC 7523 `jwt-bearer` grant requires a
  confidential client, i.e. a long-lived client secret in every tenant — exactly what the
  requirement rules out. The genuinely secretless supported path is Federated Client Authentication
  (`Signed JWT - Federated` plus the first-class `Kubernetes` identity-provider type), promoted to
  supported in Keycloak **26.6**. A new deployment can have that by construction, so this objection
  is about timing, not capability.
- **It removes none of the prerequisite work.** Keycloak's Kubernetes IdP needs each tenant's OIDC
  discovery document and JWKS reachable *from Keycloak* — the same publishing task, plus a second
  consumer of it and a second place a rollout can half-land.
- **Co-locating it creates a hard bootstrap cycle, and this is decisive.** Keycloak needs a
  database. A Postgres PVC on the management cluster is served by this driver, which as tenant zero
  authenticates to its own apiserver through the `jwt[]` path. Put Keycloak in that path and a cold
  start deadlocks: no Keycloak → no token → no volume → no database → no Keycloak. Nothing recovers
  that without manual intervention on a cluster that is already down.

Even with the storage rule below observed, routing machine tokens through Keycloak turns a Keycloak
outage from "nobody can log in" into "storage provisioning is down in every cluster". That is a
worse failure mode for no gain at this scale.

**Hard rule, and it belongs in the runbook either way:** Keycloak's storage, and anything else on
the operator's bootstrap path, must not be PVC-backed by this driver. Use the bundled `local-path`
provisioner or an external Postgres.

## Keeping the option open

Switching to Keycloak later must be a configuration change and nothing more, so:

- **Bind RBAC to the username string `pvx:<cluster>`** — never to an issuer URL, never to a
  Keycloak client ID.
- Adopting Federated Client Authentication then means replacing N per-tenant stanzas with one
  Keycloak stanza whose username expression is `'"pvx:" + claims.pvx_cluster'`. RBAC, admission
  policies, quotas, the operator and every tenant chart are untouched.

That becomes worth doing at roughly six or more tenants — when maintaining N stanzas across every
server node's config file starts to hurt — or when the same identity is needed at a non-Kubernetes
relying party. Deploy the new Keycloak with the feature enabled and the tenant IdPs configured,
then leave it out of the machine path until one of those thresholds is actually reached.

## Consequences

- **Prerequisite, not optional:** RKE2 defaults `--service-account-issuer` to
  `https://kubernetes.default.svc.cluster.local`, which is byte-identical on every cluster. Since
  `issuer.url` must be unique per authenticator, *a second tenant cannot be registered until this
  changes.* Each tenant publishes a static read-only discovery mirror at
  `https://oidc.<cluster>.ouchi.com.au/` — public keys only, no inbound path to any apiserver.
- **Revocation is removing a stanza.** JWTs are not individually revocable, and this beats Keycloak
  here, where disabling a client does not invalidate already-issued access tokens. But if the
  hot reload silently fails on one server node, revocation silently failed on that node.
  `apiserver_authentication_config_controller_automatic_reload_success_total` is the most important
  alert in the design. Practise offboarding during onboarding.
- **`--authentication-config` is mutually exclusive with the `--oidc-*` flags.** Any existing
  `--oidc-*` configuration on the management cluster must move into the same file as another
  `jwt[]` entry. Configure the new Keycloak as a stanza from the start and never add an `--oidc-*`
  flag for it.
- **N stanzas is O(N) operational surface**, replicated identically to every server node and
  managed by configuration management. A copy-pasted stanza whose username literal was never
  changed is not caught by issuer uniqueness, so CI asserts the literals are unique and each
  matches `^"pvx:[a-z0-9-]+"$`.
