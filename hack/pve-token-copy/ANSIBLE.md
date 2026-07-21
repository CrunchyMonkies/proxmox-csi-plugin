# Installing pve-csi-copy with Ansible

The `.deb` (built by the `pve-csi-copy deb` GitHub Action and attached to a
`pve-csi-copy-v*` release) installs onto **each Proxmox VE host** the CSI plugin
targets. It is a host-level package (systemd + Perl on the hypervisor), so it is
installed over SSH to the PVE nodes — not through the Proxmox API and not into any
guest VM.

Drop the tasks below into a role (or a play) that already targets your PVE hosts.

## Variables

```yaml
# group_vars/proxmox.yml  (or wherever your PVE hosts are grouped)
pve_csi_copy_version: "0.2.0"
pve_csi_copy_deb_url: >-
  https://github.com/CrunchyMonkies/proxmox-csi-plugin/releases/download/pve-csi-copy-v{{ pve_csi_copy_version }}/pve-csi-copy_{{ pve_csi_copy_version }}_all.deb
```

## Tasks (install from the GitHub release)

```yaml
- name: Install pve-csi-copy on the PVE hosts
  hosts: proxmox            # <-- your hypervisor host group (root over SSH)
  become: true
  tasks:
    - name: Download pve-csi-copy .deb
      ansible.builtin.get_url:
        url: "{{ pve_csi_copy_deb_url }}"
        dest: "/root/pve-csi-copy_{{ pve_csi_copy_version }}_all.deb"
        mode: "0644"
      register: _pcc_deb

    - name: Install pve-csi-copy
      ansible.builtin.apt:
        deb: "{{ _pcc_deb.dest }}"
      notify: verify pve-csi-copy

  handlers:
    - name: verify pve-csi-copy
      ansible.builtin.command: /usr/sbin/pve-csi-copy-verify
      changed_when: false
```

## Tasks (install from a vendored .deb)

If you'd rather commit the `.deb` into the role's `files/` instead of pulling from a
release:

```yaml
- name: Install pve-csi-copy (vendored)
  ansible.builtin.apt:
    deb: "{{ role_path }}/files/pve-csi-copy_{{ pve_csi_copy_version }}_all.deb"
  notify: verify pve-csi-copy
```

## Notes

- **The package restarts `pvedaemon` and `pveproxy`** in its postinst (a few seconds
  of API downtime; running guests and CSI mounts are unaffected). If you serialise
  PVE work, add `serial: 1` to the play so nodes restart one at a time.
- **`apt: deb:` is idempotent** — re-running with the same version is a no-op; bump
  `pve_csi_copy_version` (and the release) to upgrade.
- **Verification.** `/usr/sbin/pve-csi-copy-verify` exits non-zero if the daemons are
  down or the endpoint did not register — good for a handler, a `check_mode` gate, or
  a post-upgrade smoke test. The package install itself only *warns* on a
  registration miss (it fails safe), so wire this in if you want a hard gate.
- **Uninstall:** `ansible.builtin.apt: name=pve-csi-copy state=absent` — the maintainer
  scripts remove the loader drop-ins before the module and restart the daemons, so no
  stale loader is left behind.
- **After a PVE major upgrade**, re-run the verify task (and a real token copy). The
  module fails safe, but a changed PVE internal would silently drop the endpoint.
