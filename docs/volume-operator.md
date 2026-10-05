# Volume operator

The volume operator is the management-side half of the [volume control plane](volume-control-plane.md).
It runs in one management cluster, holds the Proxmox credential there, and keeps the
`csi.crunchymonkies.com/v1alpha1` ledger that tenant clusters request volumes through.

It is **optional and off by default**. Tenant clusters that run the CSI controller and node plugin
the usual way are unaffected.

> [!NOTE]
> The operator runs the storage catalog publisher, the tenant reconciler, volume adoption, the
> drift detector, and the full volume lifecycle for tenants: create, delete, attach, detach,
> expand, modify (VolumeAttributesClass) and snapshots, clones and restores. Writes require the
> tenant to be in **Enforce** mode; Observe mode is read-only.

## Enable

In the management cluster, install the plugin chart with only the operator:

```yaml
controller:
  enabled: false
node:
  enabled: false

operator:
  enabled: true
  config:
    clusters:
      - url: https://cluster-api-1.example.com:8006/api2/json
        insecure: false
        token_ref:
          name: proxmox-operator-token
        region: Region-1
  tenants:
    - name: cluster-a
      namespace: tenant-cluster-a
```

The chart then:

- installs the five `csi.crunchymonkies.com` CRDs (`TenantCluster`, `ProxmoxVolume`,
  `ProxmoxVolumeAttachment`, `ProxmoxVolumeSnapshot`, `ProxmoxStorage`), annotated
  `helm.sh/resource-policy: keep` so `helm uninstall` does not delete the ledger;
- runs the `proxmox-csi-operator` Deployment with leader election;
- grants the generated ClusterRole on those resources, plus a namespaced Role for leases, events and
  only the `token_ref` Secrets it is given;
- renders a namespace and an object-count ResourceQuota per entry in `operator.tenants`. Tenants
  get no Role or RoleBinding: they hold no identity on the management cluster and reach it only
  through the volume API ([ADR 3](adr/0003-tenants-reach-the-volume-api-through-an-oauth2-service.md)).

The CRDs are rendered only when `operator.enabled` is true, so tenant clusters never get them.

The cloud config is the same format as the CSI plugin's ([config.md](config.md)). `token_ref` Secrets
are read from the operator's namespace.

## Flags

| Flag | Default | |
|---|---|---|
| `-cloud-config` | — | Required. Path to the cloud config |
| `-metrics-address` | `:8080` | `0` disables metrics |
| `-health-probe-address` | `:8081` | Readiness fails when the storage catalog is older than 5m |
| `-leader-elect` | `true` | |
| `-leader-election-namespace` | in-cluster namespace | |
| `-storage-sync-period` | `1m` | Storage catalog refresh |
| `-tenant-resolve-period` | `1m` | Tenant VMID resolution |
| `-volume-sync-period` | `5m` | Volume re-check |
| `-drift-detector` | `true` | |
| `-drift-interval` | `30m` | |
| `-volume-api-address` | `` | TCP address for the gRPC volume API. Empty disables. |
| `-volume-api-rate` | `10` | Per-tenant requests/second |
| `-volume-api-burst` | `20` | Per-tenant burst |

Proxmox API metrics are served at `/metrics/proxmox`; the drift gauges
(`proxmox_csi_drift_*`) are on `/metrics`.

## Code layout

| Path | |
|---|---|
| `cmd/volume-operator` | The binary |
| `pkg/apis/csi/v1alpha1` | CRD types (`make generate` regenerates deepcopy) |
| `pkg/operator/proxmox` | Read-only Proxmox adapters over `pkg/proxmoxpool` |
| `pkg/operator/controller/{storage,tenant,volume,drift}` | Reconcilers and runnables |
| `charts/proxmox-csi-plugin/files/` | Generated CRDs and ClusterRole (`make manifests`) |
| `test/operator` | `AssertNoWrites` and the CRD/CEL validation test |

The CSI binaries must not import any of this. `make operator-isolation` enforces it; see
[ADR 5](adr/0005-the-volume-operator-ships-in-the-plugin-repository.md).

## Volume API

The operator optionally serves a gRPC **volume API** (`-volume-api-address=:9090`) that
tenant clusters use to provision volumes remotely. It runs on **every replica** (not
leader-gated) and authenticates callers via OIDC bearer tokens (JWT). TLS terminates at
Envoy Gateway with h2c to the pod ([ADR 6](adr/0006-volume-api-in-operator-binary.md)).

Authentication: the caller's JWT `iss` and `azp` must resolve to exactly one
TenantCluster with `spec.issuer == iss` and `spec.subject == "pvx:" + azp`, and
that tenant must be Admitted. Only issuers that appear in a TenantCluster are
ever fetched (OIDC discovery + JWKS are cached per issuer).

Every object created via the API gets the `csi.crunchymonkies.com/tenant` label
and a `csi.crunchymonkies.com/request-id` annotation. A structured audit log
line is written for every call.

The API is enabled in the chart with `operator.volumeAPI.enabled: true`. An
optional `operator.volumeAPI.gateway` section renders a Gateway API GRPCRoute.

## Remote mode

A tenant cluster's CSI controller can operate in **remote mode** where it talks
to the volume API instead of Proxmox directly. Enable it with
`controller.remote.enabled: true` in the chart. The controller needs:

- `controller.remote.url`: the volume API address (host:port)
- `controller.remote.tokenURL`: the OAuth2 token endpoint
- `controller.remote.existingSecret`: a Secret with `clientId` and `clientSecret` keys

In remote mode the Proxmox config Secret is not rendered and `--cloud-config` is
not passed; `--remote-volume-api` and `--cloud-config` are mutually exclusive.

The provisioner sidecar gets `--extra-create-metadata` so that the PVC name and
namespace flow through to the operator for namespace-quota accounting.

## Provisioning

The volume reconciler provisions new disks when `spec.adopt` is false and the
tenant is in **Enforce** mode. The refusal order is:

1. Tenant unknown or suspended
2. Mode is Observe -- `TenantObserveOnly`
3. Region mismatch
4. Storage not in `allowedStorages` -- `StorageNotAllowed`
5. ParameterPolicy violations -- `ParameterRejected`
6. Quota exceeded -- `QuotaExceeded`
7. Namespace quota exceeded (requires `claimRef`) -- `NamespaceQuotaExceeded`

Disk naming follows the CSI controller's convention: `vm-<placeholderVmid>-<pvName>`.
The volumeID is built identically to the CSI controller's `region/zone/storage/disk`
format.

Deletion of provisioned (non-adopted) volumes deletes the Proxmox disk. Adopted
volumes keep today's behavior: the record goes, the disk stays (unless the
tenant is in Decommission mode).
