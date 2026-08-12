package PVE::JSONSchema;

# Compile-check stub. NOT a reimplementation of PVE::JSONSchema.
#
# ProxmodExt::CSIStorage imports get_standard_option at compile time. The
# import has to resolve for `perl -c` to succeed; the return value only
# matters at runtime inside pvedaemon, where the genuine
# libpve-common-perl module supplies the real schema fragments.
#
# Deliberately written from scratch rather than copied from libpve-common-perl
# (AGPL) so that `make build` works on a plain developer machine and in CI
# without vendoring Proxmox source into this Apache-2.0 repository.

use strict;
use warnings;

use Exporter 'import';

our @EXPORT_OK = qw(get_standard_option);

sub get_standard_option {
    my ($name, $overrides) = @_;
    return { %{ $overrides || {} } };
}

1;
