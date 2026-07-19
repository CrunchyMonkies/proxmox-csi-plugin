#!/bin/bash
# Install (or remove) the token-authorised copy-volume endpoint on a Proxmox VE node.
# Touches NO Proxmox-shipped file: drops a self-contained Perl module tree + two
# systemd drop-ins, then restarts pvedaemon/pveproxy so they load it.
#
#   ./install.sh install     (default)
#   ./install.sh uninstall
#   ./install.sh verify
#
# Run as root on each PVE node.
set -euo pipefail

MODDIR=/usr/lib/pve-csi-copy/perl
SELF=$(cd "$(dirname "$0")" && pwd)
DROPINS=(
  /etc/systemd/system/pvedaemon.service.d/10-pve-csi-copy.conf
  /etc/systemd/system/pveproxy.service.d/10-pve-csi-copy.conf
)

die() { echo "error: $*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || die "must run as root"
command -v pveversion >/dev/null || die "not a Proxmox VE node"

install_files() {
  # Module tree. The loader (PERL5OPT=-MPVECSICopy) dies at startup if PVECSICopy.pm
  # is missing, so the module MUST be present before the drop-ins reference it.
  install -d -m 0755 -o root -g root "$MODDIR" "$MODDIR/PVECSICopy"
  install -m 0644 -o root -g root "$SELF/perl/PVECSICopy.pm"      "$MODDIR/PVECSICopy.pm"
  install -m 0644 -o root -g root "$SELF/perl/PVECSICopy/Impl.pm" "$MODDIR/PVECSICopy/Impl.pm"

  # install -d does NOT re-chmod/chown a pre-existing dir. This path is in a root
  # daemon's @INC, so a non-root-writable entry here = root code-exec in pvedaemon.
  # Force ownership/perms, then refuse to continue if anything is group/world-writable.
  chown -R root:root /usr/lib/pve-csi-copy
  chmod 0755 /usr/lib/pve-csi-copy "$MODDIR" "$MODDIR/PVECSICopy"
  chmod 0644 "$MODDIR/PVECSICopy.pm" "$MODDIR/PVECSICopy/Impl.pm"
  if find /usr/lib/pve-csi-copy \( ! -user root -o -perm /022 \) -print | grep -q .; then
    die "insecure perms/ownership under /usr/lib/pve-csi-copy (must be root-owned, not group/world-writable)"
  fi

  # Loader drop-ins LAST, once the module they name is guaranteed on disk.
  install -D -m 0644 "$SELF/systemd/pvedaemon.service.d/10-pve-csi-copy.conf" "${DROPINS[0]}"
  install -D -m 0644 "$SELF/systemd/pveproxy.service.d/10-pve-csi-copy.conf"  "${DROPINS[1]}"
}

remove_files() {
  # Drop-ins FIRST: once removed, -MPVECSICopy is no longer requested, so a missing
  # module can't wedge daemon startup.
  rm -f "${DROPINS[@]}"
  rm -rf /usr/lib/pve-csi-copy
}

# Canonical verification lives in pkg/pve-csi-copy-verify (also shipped by the .deb
# as /usr/sbin/pve-csi-copy-verify) so install.sh and the package agree.
# shellcheck source=pkg/pve-csi-copy-verify
. "$SELF/pkg/pve-csi-copy-verify"
verify() { pve_csi_copy_verify; }

case "${1:-install}" in
install)
  # If anything below fails, roll back the drop-ins + module and restart the daemons
  # so a failed install never leaves a half-loaded / non-booting daemon behind.
  rollback() {
    echo "install failed — rolling back" >&2
    remove_files
    systemctl daemon-reload || true
    systemctl restart pvedaemon pveproxy || true
  }
  trap rollback ERR

  install_files
  systemctl daemon-reload
  # Env changes require a restart (reload does not re-read Environment=).
  # Brief API interruption (seconds); running guests / CSI mounts are unaffected.
  systemctl restart pvedaemon pveproxy
  verify

  trap - ERR
  echo "installed."
  ;;
uninstall)
  remove_files
  systemctl daemon-reload
  systemctl restart pvedaemon pveproxy
  systemctl is-active --quiet pvedaemon || die "pvedaemon not active after removal"
  echo "removed."
  ;;
verify)
  verify
  ;;
*)
  die "usage: $0 [install|uninstall|verify]"
  ;;
esac
