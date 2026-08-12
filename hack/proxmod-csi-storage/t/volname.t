#!/usr/bin/perl
# Tests the two volname allow-lists in ProxmodExt::CSIStorage: $RE_VOLNAME,
# which gates every caller-supplied source volname (and `copy`'s target), and
# $RE_TARGET_VOLNAME, the stricter one `rename` applies to its target.
#
# The pattern is the endpoint's only defence against path traversal on
# directory-backed storage — parse_volume_id validates the storage-id prefix
# and treats the volname as `.+`, so nothing downstream re-checks it. It is
# also the reason directory-storage copies work at all, since file-backed
# storages name every volume '<vmid>/<name>.<ext>'. Both properties are load
# bearing, so both are asserted here.
#
# The pattern is read out of the module source rather than duplicated, so this
# test cannot silently pass against a stale copy of it.

use strict;
use warnings;

use Test::More;
use FindBin qw($Bin);

my $module = "$Bin/../perl/ProxmodExt/CSIStorage.pm";

open(my $fh, '<', $module) or die "cannot read $module: $!";
my $src = do { local $/; <$fh> };
close($fh);

my ($pattern) = $src =~ /my \$RE_VOLNAME = qr\{(.+?)\};/s;
ok(defined $pattern, 'extracted $RE_VOLNAME from the module source')
    or BAIL_OUT("no \$RE_VOLNAME definition found in $module");

my $re = qr{$pattern};

# The directory plugin's '<vmid>/<name>' form must pass: proxmox-csi-plugin's
# volume.CopyVolume() emits it for every snapshot whose disk has a file
# extension, and MoveQemuDisk passes vol.Disk() straight through.
my @accept = (
    'vm-9999-disk-0',                 # flat: lvm, lvm-thin, zfspool
    'base-100-disk-0',
    '9999/vm-9999-disk-0.raw',        # dir plugin, controller-owned CSI volume
    '9999/vm-9999-snapshot-foo.raw',  # dir plugin, CSI snapshot
    '100/vm-100-pvc-abc.qcow2',       # lowest valid vmid
    '999999999/x',                    # highest valid vmid
);

my @reject = (
    '../etc/passwd',
    '9999/../x',
    '9999/../../etc/passwd',
    '9999/x/../y',
    'x:y',            # would split as a volid
    '/abs/path',
    '0999/x',         # vmid may not have a leading zero
    '99/x',           # below PVE's minimum vmid
    '1234567890/x',   # above PVE's maximum vmid
    '9999//x',
    'a/b/c',          # only one path component is allowed, and it must be a vmid
    '9999/',
    '.hidden',
    '',
);

is(scalar($src =~ s/\$RE_VOLNAME/$&/g), 4,
    'the pattern is defined once and applied to every volname a caller supplies');

ok($_ =~ $re, "accepts '$_'") for @accept;
ok($_ !~ $re, "rejects '$_'") for @reject;

cmp_ok(160, '>=', length($_), "maxLength 160 admits '$_'") for @accept;

# --- rename's target volname ------------------------------------------------
# A separate, stricter pattern, because it is used differently:
# PVE::Storage::Plugin::rename_volume composes the destination itself as
# "<basedir>/<target_vmid>/<target_volname>". A '<vmid>/' component here would
# not be the directory plugin's volname form, it would be a second path
# component the plugin never intended — so the '9999/…' shape $RE_VOLNAME must
# accept is exactly what this one must reject.

my ($target_pattern) = $src =~ /my \$RE_TARGET_VOLNAME = qr\{(.+?)\};/s;
ok(defined $target_pattern, 'extracted $RE_TARGET_VOLNAME from the module source')
    or BAIL_OUT("no \$RE_TARGET_VOLNAME definition found in $module");

my $target_re = qr{$target_pattern};

is(scalar($src =~ s/\$RE_TARGET_VOLNAME/$&/g), 2,
    'the target pattern is defined once and applied once');

ok($_ =~ $target_re, "target accepts '$_'") for (
    'vm-3021-pvc-abc.raw',            # what the CSI driver renames to
    'vm-9999-pvc-abc.raw',            # …and back again
    'vm-100-disk-0',                  # flat, no extension
);

ok($_ !~ $target_re, "target rejects '$_'") for (
    '3021/vm-3021-pvc-abc.raw',       # the plugin adds the vmid directory itself
    '../etc/passwd',
    'x/../y',
    '/abs/path',
    'x:y',
    '.hidden',
    '',
);

done_testing();
