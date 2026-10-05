# Architecture decision records

One file per decision, numbered, never edited after acceptance — a decision that turns out wrong
gets a new record that supersedes it, so the reasoning that led there stays readable.

| # | Decision | Status |
|---|---|---|
| [1](0001-the-management-apiserver-is-the-volume-api.md) | The management apiserver is the volume API | superseded in part by 3 and 5 |
| [2](0002-federate-tenant-service-account-tokens-directly.md) | Federate tenant service-account tokens directly, not through Keycloak | superseded by 4 |
| [3](0003-tenants-reach-the-volume-api-through-an-oauth2-service.md) | Tenants reach the volume API through an OAuth2 service, not the apiserver | accepted |
| [4](0004-tenants-authenticate-with-keycloak-client-credentials.md) | Tenants authenticate with Keycloak client credentials | accepted |
| [5](0005-the-volume-operator-ships-in-the-plugin-repository.md) | The volume operator ships in the plugin repository | accepted |
| [6](0006-the-volume-api-lives-in-the-operator-binary.md) | The volume API lives in the operator binary | accepted; amends 3 |

The design these produce is written up in
[`../volume-control-plane.md`](../volume-control-plane.md); how to run it is in
[`../volume-operator.md`](../volume-operator.md).
