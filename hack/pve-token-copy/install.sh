#!/bin/bash
# Install (or remove) the token-authorised copy-volume endpoint on a Proxmox VE node.
# Touches NO Proxmox-shipped file: drops a self-contained Perl module tree, a
# daemon-start wrapper, and two systemd drop-ins, then restarts pvedaemon/pveproxy
# so they load it.
#
#   ./install.sh install     (default)
#   ./install.sh uninstall
#   ./install.sh verify
#
# Run as root on each PVE node.
set -euo pipefail

# The module lives in a DEFAULT @INC dir. pvedaemon/pveproxy run `perl -T` (taint
# mode), which ignores PERL5LIB/PERL5OPT, so the drop-ins load it via a
# command-line `-MPVECSICopy` — which only resolves from a default @INC path.
MODDIR=/usr/share/perl5
# The daemon-start wrapper: reads each daemon's real ExecStart at start time and
# injects -MPVECSICopy, so it survives PVE changing the invocation. Fails safe.
WRAPPERDIR=/usr/lib/pve-csi-copy
WRAPPER="$WRAPPERDIR/pve-csi-copy-exec"
SELF=$(cd "$(dirname "$0")" && pwd)
DROPINS=(
  /etc/systemd/system/pvedaemon.service.d/10-pve-csi-copy.conf
  /etc/systemd/system/pveproxy.service.d/10-pve-csi-copy.conf
)

die() { echo "error: $*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || die "must run as root"
command -v pveversion >/dev/null || die "not a Proxmox VE node"

install_files() {
  # Module tree, into the shared default @INC dir. The loader (ExecStart's
  # `perl -T -MPVECSICopy`) dies at startup if PVECSICopy.pm is missing, so the
  # module MUST be present before the drop-ins reference it. Only our own files
  # (PVECSICopy.pm + PVECSICopy/) are touched — never the shared /usr/share/perl5.
  install -d -m 0755 -o root -g root "$MODDIR/PVECSICopy"
  install -m 0644 -o root -g root "$SELF/perl/PVECSICopy.pm"      "$MODDIR/PVECSICopy.pm"
  install -m 0644 -o root -g root "$SELF/perl/PVECSICopy/Impl.pm" "$MODDIR/PVECSICopy/Impl.pm"

  # These files are in a root daemon's @INC, so a non-root-writable entry here =
  # root code-exec in pvedaemon. Force ownership/perms on OUR files/dir, then
  # refuse to continue if any of them is non-root-owned or group/world-writable.
  chown root:root "$MODDIR/PVECSICopy" "$MODDIR/PVECSICopy.pm" "$MODDIR/PVECSICopy/Impl.pm"
  chmod 0755 "$MODDIR/PVECSICopy"
  chmod 0644 "$MODDIR/PVECSICopy.pm" "$MODDIR/PVECSICopy/Impl.pm"
  if find "$MODDIR/PVECSICopy.pm" "$MODDIR/PVECSICopy" \( ! -user root -o -perm /022 \) -print | grep -q .; then
    die "insecure perms/ownership under $MODDIR/PVECSICopy (must be root-owned, not group/world-writable)"
  fi

  # Daemon-start wrapper. systemd's ExecStart runs it as root, so it must be
  # root-owned and not group/world-writable (a writable wrapper = root code-exec in
  # pvedaemon). Install before the drop-ins that reference it.
  install -d -m 0755 -o root -g root "$WRAPPERDIR"
  install -m 0755 -o root -g root "$SELF/pve-csi-copy-exec" "$WRAPPER"
  chown root:root "$WRAPPERDIR" "$WRAPPER"
  chmod 0755 "$WRAPPERDIR" "$WRAPPER"
  if find "$WRAPPER" \( ! -user root -o -perm /022 \) -print | grep -q .; then
    die "insecure perms/ownership on $WRAPPER (must be root-owned, not group/world-writable)"
  fi

  # Loader drop-ins LAST, once the module + wrapper they name are guaranteed on disk.
  install -D -m 0644 "$SELF/systemd/pvedaemon.service.d/10-pve-csi-copy.conf" "${DROPINS[0]}"
  install -D -m 0644 "$SELF/systemd/pveproxy.service.d/10-pve-csi-copy.conf"  "${DROPINS[1]}"
}

remove_files() {
  # Drop-ins FIRST: once removed, -MPVECSICopy is no longer requested, so a missing
  # module can't wedge daemon startup. Remove only our own files — never the
  # shared /usr/share/perl5 dir; rmdir the PVECSICopy subdir only if now empty.
  rm -f "${DROPINS[@]}"
  rm -f "$MODDIR/PVECSICopy.pm" "$MODDIR/PVECSICopy/Impl.pm"
  rmdir "$MODDIR/PVECSICopy" 2>/dev/null || true
  rm -f "$WRAPPER"
  rmdir "$WRAPPERDIR" 2>/dev/null || true
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
  # ExecStart changes require a full restart to take effect (a graceful reload
  # would re-exec the OLD command line). Brief API interruption (seconds);
  # running guests / CSI mounts are unaffected.
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
