package PVECSICopy::Impl;

# Implementation for the token-authorised copy-volume endpoint. Loaded ONLY through
# PVECSICopy.pm's eval, so any die here (including a compile error surfaced by
# `require`) is swallowed and the daemon still starts. Fully-qualified PVE calls
# only — no `import`, so a changed helper signature can't fail at load.
#
# NOTE: this couples to PVE internals (register_method, parse_volname,
# check_volume_access, storage_migrate). Re-validate against the exact PVE version
# on the target node — see README "Caveats". install.sh verify checks it is live.

use strict;
use warnings;

my $REGISTERED = 0;

sub register {
    return if $REGISTERED;

    require PVE::JSONSchema;
    require PVE::RPCEnvironment;
    require PVE::Storage;
    require PVE::SSHInfo;
    require PVE::INotify;
    require PVE::API2::Storage::Content;

    # Idempotent: if a prior load already registered it, do nothing (avoids a
    # register_method collision die on daemon reload).
    if (PVE::API2::Storage::Content->can('map_method_by_name')
        && eval { PVE::API2::Storage::Content->map_method_by_name('copy_volume_token') }) {
        $REGISTERED = 1;
        return;
    }

    PVE::API2::Storage::Content->register_method({
        name => 'copy_volume_token',
        path => '{volume}/copy',
        method => 'POST',
        description => "Copy a volume to another storage (token-authorised sibling of "
            . "the root-only built-in 'copy'). Source access is enforced per-volume; "
            . "the target requires Datastore.AllocateSpace.",
        protected => 1,      # runs in pvedaemon (as root); the ACL below authorises the token
        proxyto => 'node',
        permissions => {
            description => "Datastore.Audit on the SOURCE storage (a copy is a read); "
                . "the specific source volume is additionally checked with "
                . "check_volume_access, and Datastore.AllocateSpace on the TARGET "
                . "storage is checked in code.",
            check => ['perm', '/storage/{storage}', ['Datastore.Audit']],
        },
        parameters => {
            additionalProperties => 0,
            properties => {
                node => PVE::JSONSchema::get_standard_option('pve-node'),
                storage => PVE::JSONSchema::get_standard_option('pve-storage-id'),
                volume => {
                    description => "Source volume name within {storage} (block volname).",
                    type => 'string',
                    # Explicit allow-list. parse_volume_id only validates the storage-id
                    # prefix (volname is `.+`), so DO NOT rely on it to reject traversal.
                    # This rejects ':' (volid-splitting), '/' and '..' (traversal).
                    pattern => '^[A-Za-z0-9][A-Za-z0-9._\-]*$',
                    maxLength => 128,
                },
                target => {
                    description => "Target volume id, 'storage:volname'.",
                    type => 'string',
                    maxLength => 160,
                },
                target_node => PVE::JSONSchema::get_standard_option('pve-node', {
                    description => "Target node (defaults to the request node).",
                    optional => 1,
                }),
            },
        },
        returns => { type => 'string' },
        code => sub {
            my ($param) = @_;

            my $rpcenv = PVE::RPCEnvironment::get();
            my $authuser = $rpcenv->get_user();
            my $cfg = PVE::Storage::config();

            # --- source (a READ) -------------------------------------------------
            my $src_volid = "$param->{storage}:$param->{volume}";
            my ($src_sid, undef) = PVE::Storage::parse_volume_id($src_volid);
            die "internal: source storage mismatch\n" if $src_sid ne $param->{storage};

            # Datastore.Audit on the storage (declared above) is coarse; enforce
            # access to THIS volume so a token can't copy another owner's disk merely
            # because it can create volumes on the same storage. check_volume_access
            # honours VM ownership as well as Datastore.Audit/Allocate.
            my $ownervm;
            eval { (undef, undef, $ownervm) = PVE::Storage::parse_volname($cfg, $src_volid); };
            PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, $ownervm, $src_volid);

            my $src_size = eval { PVE::Storage::volume_size_info($cfg, $src_volid) };
            die "source volume '$src_volid' not found\n" if !$src_size;

            # --- target (a WRITE) ------------------------------------------------
            my ($dst_sid, $dst_volname) = PVE::Storage::parse_volume_id($param->{target});
            my $dst_volid = "$dst_sid:$dst_volname";
            $rpcenv->check($authuser, "/storage/$dst_sid", ['Datastore.AllocateSpace']);

            # Both storages must exist / be enabled.
            PVE::Storage::storage_config($cfg, $src_sid);
            PVE::Storage::storage_config($cfg, $dst_sid);

            # Advisory no-clobber. This is best-effort: the check and the migrate run
            # in different phases (the migrate is in the worker below), so two
            # concurrent calls can race. The real guarantee is the target plugin's
            # exclusive alloc during storage_migrate, which errors on a name clash.
            my $dst_exists = eval { PVE::Storage::volume_size_info($cfg, $dst_volid) };
            die "target volume '$dst_volid' already exists, refusing to overwrite\n"
                if $dst_exists;

            my $target_node = $param->{target_node} || PVE::INotify::nodename();

            my $worker = sub {
                # Copies via storage_migrate. This sshes as root to $target_node (a
                # valid cluster node — pve-node format bounds it to a peer, not an
                # arbitrary host). Same-node targets may hit the built-in copy's
                # ssh-to-localhost behaviour; validate per PVE version (see README).
                my $sshinfo = PVE::SSHInfo::get_ssh_info($target_node);
                PVE::Storage::storage_migrate(
                    $cfg, $src_volid, $sshinfo, $dst_sid,
                    { target_volname => $dst_volname },
                );
                return;
            };

            return $rpcenv->fork_worker('imgcopy', undef, $authuser, $worker);
        },
    });

    $REGISTERED = 1;
    return;
}

1;
