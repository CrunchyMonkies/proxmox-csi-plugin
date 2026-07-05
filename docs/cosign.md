# Verify images

We'll be employing [Cosing's](https://github.com/sigstore/cosign) keyless verifications to ensure that images were built in Github Actions.

## Verify Helm chart

We will verify the keyless signature using the Cosign protocol.

```shell
cosign verify ghcr.io/crunchymonkies/charts/proxmox-csi-plugin:0.1.4 --certificate-identity https://github.com/CrunchyMonkies/proxmox-csi-plugin/.github/workflows/release-charts.yaml@refs/heads/main --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Verify containers

We will verify the keyless signature using the Cosign protocol.

```shell
# Edge version
cosign verify ghcr.io/crunchymonkies/proxmox-csi-controller:edge --certificate-identity https://github.com/CrunchyMonkies/proxmox-csi-plugin/.github/workflows/build-edge.yaml@refs/heads/main --certificate-oidc-issuer https://token.actions.githubusercontent.com

cosign verify ghcr.io/crunchymonkies/proxmox-csi-node:edge --certificate-identity https://github.com/CrunchyMonkies/proxmox-csi-plugin/.github/workflows/build-edge.yaml@refs/heads/main --certificate-oidc-issuer https://token.actions.githubusercontent.com

# Releases
cosign verify ghcr.io/crunchymonkies/proxmox-csi-controller:v0.2.0 --certificate-identity https://github.com/CrunchyMonkies/proxmox-csi-plugin/.github/workflows/release.yaml@refs/tags/v0.2.0 --certificate-oidc-issuer https://token.actions.githubusercontent.com

cosign verify ghcr.io/crunchymonkies/proxmox-csi-node:v0.2.0 --certificate-identity https://github.com/CrunchyMonkies/proxmox-csi-plugin/.github/workflows/release.yaml@refs/tags/v0.2.0 --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
