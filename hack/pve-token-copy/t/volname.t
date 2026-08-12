#!/usr/bin/perl
# Tests the volname allow-list that gates both the source `volume` parameter
# and the target volname in PVECSICopy::Impl.
#
# The pattern is the endpoint's only defence against path traversal on
# directory-backed storage — parse_volume_id validates the storage-id prefix
# and treats the volname as `.+`, so nothing downstream re-checks it. It is
# also the reason directory-storage copies work at all, since file-backed
# storages name every volume '<vmid>/<name>.<ext>'. Both properties are load
# bearing, so both are asserted here.
#
# Unlike the proxmod extension, this module spells the pattern out twice (once
# as a JSONSchema string, once as a match in the target check) because the two
# contexts need different quoting. Asserting they are identical is the point of
# the first test: a fix applied to only one of them would leave a hole.
#
# The patterns are read out of the module source rather than duplicated, so
# this test cannot silently pass against a stale copy of them.

use strict;
use warnings;

use Test::More;
use FindBin qw($Bin);

my $module = "$Bin/../perl/PVECSICopy/Impl.pm";

open(my $fh, '<', $module) or die "cannot read $module: $!";
my $src = do { local $/; <$fh> };
close($fh);

# Both spellings: pattern => '...' in the schema, and m{...} in the target
# check. Anchored on ^...$ so comments mentioning the shape are not picked up.
my @found = $src =~ /(?:pattern => '|=~ m\{)(\^.+?\$)(?:'|\})/g;

is(scalar @found, 2, 'found both the schema pattern and the target check')
    or BAIL_OUT("expected 2 volname patterns in $module, found " . scalar(@found));
is($found[1], $found[0], 'the source and target patterns are identical');

my $re = qr{$found[0]};

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

ok($_ =~ $re, "accepts '$_'") for @accept;
ok($_ !~ $re, "rejects '$_'") for @reject;

cmp_ok(160, '>=', length($_), "maxLength 160 admits '$_'") for @accept;

done_testing();
