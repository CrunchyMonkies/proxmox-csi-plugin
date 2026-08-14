#!/usr/bin/perl
# Asserts that every method in ProxmodExt::CSIStorage which answers about the
# LOCAL machine declares `proxyto => 'node'`.
#
# PVE forwards a request that arrives at node A for node B only when the method
# asks it to. Without the flag the {node} path segment is decorative: the
# handler runs on whichever host received the request. proxmox-csi-plugin points
# at a single host per cluster, and `local` is node-local storage, so a missing
# proxyto does not fail loudly — it silently answers about the wrong machine,
# and `rename` reports "source volume ... not found" for a volume that plainly
# exists on the node named in the path. That shipped in 0.3.0 and was only found
# by correlating a week of production logs, which is why it is asserted here.
#
# The rule is derived, not enumerated: anything registered as a POST is a write
# against this node's storage and must proxy. A new POST added without the flag
# fails this test rather than passing unnoticed.
#
# Like t/volname.t, this reads the module source instead of loading it — the
# registration lives inside proxmod's $api->add_method, which needs a real PVE
# to call.

use strict;
use warnings;

use Test::More;
use FindBin qw($Bin);

my $module = "$Bin/../perl/ProxmodExt/CSIStorage.pm";

open(my $fh, '<', $module) or die "cannot read $module: $!";
my $src = do { local $/; <$fh> };
close($fh);

# Each _register_* sub makes exactly one $api->add_method call, closed by `);`
# at the sub's indentation.
my @blocks = $src =~ /\$api->add_method\(\n(.*?)\n    \);/gs;

ok(scalar(@blocks) >= 3, 'found the add_method registrations')
    or BAIL_OUT("no \$api->add_method calls found in $module");

my %seen;

for my $block (@blocks) {
    my ($name) = $block =~ /name\s*=>\s*'([^']+)'/;
    ok(defined $name, 'registration declares a name') or next;

    my ($method) = $block =~ /method\s*=>\s*'([^']+)'/;
    $method ||= 'GET';

    my $proxies = $block =~ /proxyto\s*=>\s*'node'/ ? 1 : 0;

    $seen{$name} = { method => $method, proxies => $proxies };

    # Every method here is mounted at node scope, so every one of them takes a
    # node parameter. That is what makes the missing-proxyto bug possible at all.
    like($block, qr/node\s*=>\s*get_standard_option\('pve-node'\)/,
        "'$name' is node-scoped");

    if ($method eq 'POST') {
        ok($proxies, "'$name' is a POST and declares proxyto => 'node'");
    }
}

# The two volume methods are the ones the CSI driver calls, and both were
# shipped without the flag. Name them explicitly so neither can be dropped or
# renamed out of the derived check above without this test noticing.
for my $name (qw(copy rename)) {
    ok(exists $seen{$name}, "'$name' is still registered")
        or next;
    is($seen{$name}{method}, 'POST', "'$name' is a POST");
    ok($seen{$name}{proxies}, "'$name' proxies to the node in its path");
}

# The index is a static list of subdirs, identical on every node. Proxying it
# would buy nothing but a round trip, so it deliberately does not.
ok(exists $seen{index}, 'the index is still registered');
ok(!$seen{index}{proxies},
    'the index does not proxy: its answer is not about the local machine');

done_testing();
