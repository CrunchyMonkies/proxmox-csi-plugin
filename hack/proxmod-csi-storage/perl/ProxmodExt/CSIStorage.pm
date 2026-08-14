package ProxmodExt::CSIStorage;

use strict;
use warnings;

use PVE::RESTHandler;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::RESTHandler);

# Token-authorised replacement for hack/pve-token-copy in proxmox-csi-plugin.
# Endpoints are `protected => 1` (bridged to root inside pvedaemon) and gated by
# explicit ACL checks instead of PVE's root@pam-only default:
#
#   POST /nodes/{node}/proxmod/csi-storage/copy      - cross-storage volume copy
#   POST /nodes/{node}/proxmod/csi-storage/rename    - reassign an unattached
#                                                      volume to another vmid
#
# Mounted entirely under proxmod's own /nodes/{node}/proxmod/csi-storage
# subtree (see Proxmod::API - THE NAMESPACE RULE), so it neither collides
# with nor is shadowed by PVE::API2::Storage::Content's greedy {volume} path.
# That collision risk is exactly what forced hack/pve-token-copy's csi-copy
# method to live at the storage level instead of under content/ - moot here,
# since proxmod never nests inside that subtree.
#
# `copy` is a close port of hack/pve-token-copy/perl/PVECSICopy/Impl.pm:
# same ACL model (Datastore.Audit + check_volume_access on the source,
# Datastore.AllocateSpace on the target), same volume-name allow-list, same
# advisory no-clobber check, same storage_migrate worker. See that file's
# history/README for the reasoning; only the registration mechanism changes
# (proxmod's $api->add_method instead of a raw register_method behind a
# `-MPVECSICopy` daemon command-line hack).
#
# `rename` exists because move_disk cannot express what the CSI driver needs.
#
# An earlier revision shipped a `reassign` endpoint wrapping move_disk's
# `target-vmid` parameter, on the assumption that PVE required root@pam for
# it. That assumption was wrong - PVE::API2::Qemu's move_vm_disk is ACL-gated,
# not root-only - so the endpoint was removed as adding no capability. The
# deeper problem only surfaced later, in a live test against a real cluster:
# move_vm_disk operates on a DISK KEY INSIDE AN EXISTING VM'S CONFIG and moves
# it between two REAL VMs. A CSI volume at rest is unattached, and the vmid
# baked into its name (`controllerVMID`, 9999 by default) is a naming
# placeholder that exists as no VM at all, so every call 500s with
# "could not find VM 9999". Reordering cannot save it: before attach there is
# no disk key to name, after attach the volume already sits on the target.
#
# The only PVE mechanism that renames an UNATTACHED volume is
# PVE::Storage::rename_volume, which has no REST endpoint of its own and is
# reachable from Perl only - hence this method. See
# docs/reassign-volume-on-attach.md for the full account.
#
# rename_volume performs NO in-use check of its own (unlike vdisk_free), so the
# `rename` method below supplies one; see _find_blocking_references.

our $VERSION = '0.3.1';

# Anti-traversal allow-list for volnames, applied to both the source `volume`
# parameter and the target volname parsed out of `target`.
#
# The optional leading `<vmid>/` component is the directory plugin's volname
# shape ('9999/vm-9999-disk-0.raw'); file-backed storages produce it for every
# volume, and the CSI driver's snapshot names go through it too. Without it,
# directory-storage copies are rejected outright.
#
# `[1-9][0-9]{2,8}` is PVE's VMID range (100-999999999) and nothing else, so
# the relaxation stays traversal-safe: '..' cannot appear (the name component
# must start with [A-Za-z0-9], and a second '/' is not matched), and ':' and
# absolute paths remain rejected.
my $RE_VOLNAME = qr{^(?:[1-9][0-9]{2,8}/)?[A-Za-z0-9][A-Za-z0-9._\-]*$};

# `rename`'s target volname is the BARE filename, never the '<vmid>/<name>'
# form: PVE::Storage::Plugin::rename_volume composes the path itself as
# "<basedir>/<target_vmid>/<target_volname>", so a slash here would be a
# traversal into a directory of the caller's choosing.
my $RE_TARGET_VOLNAME = qr{^[A-Za-z0-9][A-Za-z0-9._\-]*$};

sub proxmod_register {
    my ($api) = @_;

    $api->mount(scope => 'node', subclass => __PACKAGE__);

    _register_index($api);
    _register_copy($api);
    _register_rename($api);

    return;
}

sub _register_index {
    my ($api) = @_;

    $api->add_method(
        class => __PACKAGE__,
        name => 'index',
        path => '',
        method => 'GET',
        permissions => { user => 'all' },
        description => 'Index of the proxmox-csi-storage extension.',
        parameters => {
            additionalProperties => 0,
            properties => {
                node => get_standard_option('pve-node'),
            },
        },
        returns => {
            type => 'array',
            items => {
                type => 'object',
                properties => { subdir => { type => 'string' } },
            },
            links => [{ rel => 'child', href => '{subdir}' }],
        },
        code => sub {
            return [{ subdir => 'copy' }, { subdir => 'rename' }];
        },
    );

    return;
}

sub _register_copy {
    my ($api) = @_;

    $api->add_method(
        class => __PACKAGE__,
        name => 'copy',
        path => 'copy',
        method => 'POST',
        protected => 1,
        # Both volume methods answer about the LOCAL machine's storage, so they
        # must run on the node named in the path. PVE only proxies a request to
        # that node when the method asks for it; without this the {node} segment
        # is decorative and the handler executes wherever the request happened to
        # land — for the CSI driver, always the single host its cluster URL points
        # at. With node-local storage every volume on any other node is then
        # invisible, and the method fails with "source volume ... not found" for a
        # volume that plainly exists. PVECSICopy::Impl, which `copy` is a port of,
        # sets this; the port dropped it.
        proxyto => 'node',
        permissions => {
            description => 'Datastore.Audit on the SOURCE storage (a copy is a read); '
                . 'the specific source volume is additionally checked with '
                . 'check_volume_access, and Datastore.AllocateSpace on the TARGET '
                . 'storage is checked in code.',
            check => ['perm', '/storage/{storage}', ['Datastore.Audit']],
        },
        description => 'Copy a volume to another storage. Token-authorised sibling '
            . "of PVE's root-only built-in content 'copy' method.",
        parameters => {
            additionalProperties => 0,
            properties => {
                node => get_standard_option('pve-node'),
                storage => get_standard_option('pve-storage-id'),
                volume => {
                    description => "Source volume name within {storage}. Either a flat "
                        . "block volname or the directory plugin's '<vmid>/<name>' form.",
                    type => 'string',
                    pattern => $RE_VOLNAME,
                    maxLength => 160,
                },
                target => {
                    description => "Target volume id, 'storage:volname'.",
                    type => 'string',
                    maxLength => 160,
                },
                target_node => get_standard_option('pve-node', {
                    description => 'Target node (defaults to the request node).',
                    optional => 1,
                }),
            },
        },
        returns => { type => 'string' },
        code => \&_copy_code,
    );

    return;
}

sub _copy_code {
    my ($param) = @_;

    require PVE::JSONSchema;
    require PVE::RPCEnvironment;
    require PVE::Storage;
    require PVE::SSHInfo;
    require PVE::INotify;

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();
    my $cfg = PVE::Storage::config();

    # --- source (a READ) --------------------------------------------------
    my $src_volid = "$param->{storage}:$param->{volume}";
    my ($src_sid, undef) = PVE::Storage::parse_volume_id($src_volid);
    die "internal: source storage mismatch\n" if $src_sid ne $param->{storage};

    # Datastore.Audit on the storage (declared in `permissions`) is coarse;
    # enforce access to THIS volume so a token can't copy another owner's
    # disk merely because it can create volumes on the same storage.
    my $ownervm;
    eval { (undef, undef, $ownervm) = PVE::Storage::parse_volname($cfg, $src_volid); };
    PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, $ownervm, $src_volid);

    my $src_size = eval { PVE::Storage::volume_size_info($cfg, $src_volid) };
    die "source volume '$src_volid' not found\n" if !$src_size;

    # --- target (a WRITE) --------------------------------------------------
    my ($dst_sid, $dst_volname) = PVE::Storage::parse_volume_id($param->{target});
    die "invalid target volume name\n"
        unless $dst_volname =~ $RE_VOLNAME && length($dst_volname) <= 160;
    my $dst_volid = "$dst_sid:$dst_volname";
    $rpcenv->check($authuser, "/storage/$dst_sid", ['Datastore.AllocateSpace']);

    PVE::Storage::storage_config($cfg, $src_sid);
    PVE::Storage::storage_config($cfg, $dst_sid);

    # Advisory no-clobber (best-effort; see hack/pve-token-copy's Impl.pm for
    # why the real guarantee is the target plugin's exclusive alloc).
    my $dst_exists = eval { PVE::Storage::volume_size_info($cfg, $dst_volid) };
    die "target volume '$dst_volid' already exists, refusing to overwrite\n"
        if $dst_exists;

    my $target_node = $param->{target_node} || PVE::INotify::nodename();

    my $worker = sub {
        my $sshinfo = PVE::SSHInfo::get_ssh_info($target_node);
        PVE::Storage::storage_migrate(
            $cfg, $src_volid, $sshinfo, $dst_sid,
            { target_volname => $dst_volname },
        );
        return;
    };

    return $rpcenv->fork_worker('imgcopy', undef, $authuser, $worker);
}

sub _register_rename {
    my ($api) = @_;

    $api->add_method(
        class => __PACKAGE__,
        name => 'rename',
        path => 'rename',
        method => 'POST',
        protected => 1,
        # Without this the {node} segment is decorative: the method runs on
        # whichever host received the request, not the one named in the path.
        # See _register_copy.
        proxyto => 'node',
        permissions => {
            description => 'Datastore.Audit on the storage; the specific source volume '
                . 'is additionally checked with check_volume_access, '
                . 'Datastore.AllocateSpace on the storage (a rename is a write) and '
                . 'VM.Config.Disk on /vms/{target_vmid} (the rename assigns ownership '
                . 'to that VM) are checked in code. This is the same ACL set move_disk '
                . 'enforces, so no new privilege class is introduced.',
            check => ['perm', '/storage/{storage}', ['Datastore.Audit']],
        },
        description => 'Reassign an unattached volume to another VMID by renaming it '
            . 'at the storage layer. Wraps PVE::Storage::rename_volume, which has no '
            . 'REST endpoint of its own. Synchronous: a rename is an in-directory '
            . 'rename(2), so there is no worker to fork.',
        parameters => {
            additionalProperties => 0,
            properties => {
                node => get_standard_option('pve-node'),
                storage => get_standard_option('pve-storage-id'),
                volume => {
                    description => "Source volume name within {storage}. Either a flat "
                        . "block volname or the directory plugin's '<vmid>/<name>' form.",
                    type => 'string',
                    pattern => $RE_VOLNAME,
                    maxLength => 160,
                },
                target_vmid => get_standard_option('pve-vmid', {
                    description => 'VMID to reassign the volume to. Need not be a VM that '
                        . 'exists - unlike move_disk, this is a storage-level rename.',
                }),
                target_volname => {
                    description => 'Target volume name, without the leading '
                        . "'<target_vmid>/'. Defaults to the next free vm-<target_vmid>-disk-N.",
                    type => 'string',
                    pattern => $RE_TARGET_VOLNAME,
                    maxLength => 160,
                    optional => 1,
                },
            },
        },
        returns => {
            type => 'string',
            description => 'The volume id the volume now has.',
        },
        code => \&_rename_code,
    );

    return;
}

sub _rename_code {
    my ($param) = @_;

    require PVE::RPCEnvironment;
    require PVE::Storage;

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();
    my $cfg = PVE::Storage::config();

    my $storeid = $param->{storage};
    my $target_vmid = $param->{target_vmid};
    my $target_volname = $param->{target_volname};

    my $src_volid = "$storeid:$param->{volume}";
    my ($src_sid, undef) = PVE::Storage::parse_volume_id($src_volid);
    die "internal: source storage mismatch\n" if $src_sid ne $storeid;

    # Datastore.Audit on the storage (declared in `permissions`) is coarse;
    # enforce access to THIS volume so a token cannot reassign another owner's
    # disk merely because it can create volumes on the same storage.
    my $ownervm;
    eval { (undef, undef, $ownervm) = PVE::Storage::parse_volname($cfg, $src_volid); };
    PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, $ownervm, $src_volid);

    # A rename is a write on the storage, and it hands the volume to a VM.
    $rpcenv->check($authuser, "/storage/$storeid", ['Datastore.AllocateSpace']);
    $rpcenv->check($authuser, "/vms/$target_vmid", ['VM.Config.Disk']);

    PVE::Storage::storage_config($cfg, $storeid);

    my $src_size = eval { PVE::Storage::volume_size_info($cfg, $src_volid) };
    die "source volume '$src_volid' not found\n" if !$src_size;

    # --- guard 1: the storage can actually do this ------------------------
    # Refuse up front rather than discover it half way through the plugin.
    die "storage '$storeid' does not support renaming volume '$param->{volume}'\n"
        if !PVE::Storage::volume_has_feature($cfg, 'rename', $src_volid);

    # --- guard 2: nothing is pointing at it -------------------------------
    my $refs = _find_blocking_references($src_volid);
    die "volume '$src_volid' is still referenced by " . join(', ', @$refs)
        . " - refusing to rename\n"
        if @$refs;

    # --- guard 3: advisory no-clobber -------------------------------------
    # PVE::Storage::Plugin::rename_volume makes the real guarantee (it dies on
    # an existing target path); this only turns the common case into a clearer
    # error. Both volname shapes are probed because which one applies is a
    # property of the plugin, not something worth hardcoding here.
    if (defined($target_volname)) {
        for my $candidate ("$storeid:$target_vmid/$target_volname", "$storeid:$target_volname") {
            my $exists = eval { PVE::Storage::volume_size_info($cfg, $candidate) };
            die "target volume '$candidate' already exists, refusing to overwrite\n" if $exists;
        }
    }

    return PVE::Storage::rename_volume($cfg, $src_volid, $target_vmid, $target_volname);
}

# Returns a list of human-readable references that a rename would leave
# dangling. Empty list means the volume is safe to rename.
#
# PVE::Storage::rename_volume performs no in-use check whatsoever - unlike
# vdisk_free, which at least refuses base volumes with linked clones - so
# without this the endpoint would happily rename a disk out from under a
# running VM. This guard is what makes the endpoint safe independently of
# whatever the CSI driver believes about the volume's state.
#
# `unused<n>` references are deliberately TOLERATED. When a volume named for
# the VM it is attached to is detached, PVE does not simply drop it: because
# vm_is_volid_owner() is true, vmconfig_register_unused_drive() parks it in an
# `unused<n>` slot. That entry keeps no device open and blocks nothing, and it
# is precisely the state the CSI driver has to rename out of. The driver
# removes the stale entry afterwards - which is safe only in that order, since
# deleting an `unused<n>` key whose volume still exists makes PVE deallocate
# it, while deleting one whose volume has already been renamed away is a no-op
# (Plugin::free_image warns "does not exist" and returns).
#
# A failure to read any config is fatal: a config we cannot inspect may be the
# one holding the reference, so the only safe answer is to refuse.
sub _find_blocking_references {
    my ($volid) = @_;

    require PVE::Cluster;

    my $found = [];

    my $vmlist = PVE::Cluster::get_vmlist() || {};
    my $ids = $vmlist->{ids} || {};

    for my $vmid (sort { $a <=> $b } keys %$ids) {
        my $node = $ids->{$vmid}->{node};
        my $type = $ids->{$vmid}->{type} // '';

        if ($type eq 'qemu') {
            require PVE::QemuConfig;
            require PVE::QemuServer;

            my $conf = eval { PVE::QemuConfig->load_config($vmid, $node) };
            die "unable to read config of VM $vmid on node '$node' - $@" if $@;

            # foreach_volid covers current drives, pending changes, snapshots
            # and unused slots in one pass, and tells us which is which.
            PVE::QemuServer::foreach_volid($conf, sub {
                my ($vol, $attr) = @_;

                return if $vol ne $volid;

                my $snaps = $attr->{referenced_in_snapshot} // {};

                return
                    if $attr->{is_unused}
                    && !$attr->{is_attached}
                    && !$attr->{referenced_in_pending}
                    && !keys %$snaps;

                my $how =
                    $attr->{is_attached} ? ($attr->{drivename} // 'a disk')
                    : $attr->{referenced_in_pending} ? 'a pending change'
                    : 'a snapshot';

                push @$found, "VM $vmid ($how)";
            });
        } elsif ($type eq 'lxc') {
            require PVE::LXC::Config;

            my $conf = eval { PVE::LXC::Config->load_config($vmid, $node) };
            die "unable to read config of CT $vmid on node '$node' - $@" if $@;

            # Only rootfs/mp<n> are matched, so - as for VMs - a container's
            # unused<n> slots do not block the rename.
            for my $where ([$conf, ''], map { [$conf->{snapshots}->{$_}, " in snapshot '$_'"] }
                sort keys %{ $conf->{snapshots} // {} }) {
                my ($c, $suffix) = @$where;

                for my $key (sort keys %$c) {
                    next if $key !~ m/^(?:rootfs|mp\d+)$/;

                    my $val = $c->{$key};
                    next if !defined($val) || ref($val);

                    my $file = (split(/,/, $val))[0];
                    $file =~ s/^volume=// if defined($file);

                    push @$found, "CT $vmid ($key$suffix)"
                        if defined($file) && $file eq $volid;
                }
            }
        }
    }

    return $found;
}

1;
