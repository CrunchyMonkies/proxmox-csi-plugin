package PVE::RESTHandler;

# Compile-check stub. NOT a reimplementation of PVE::RESTHandler.
#
# ProxmodExt::CSIStorage does `use base qw(PVE::RESTHandler)`, which needs the
# module to be loadable at compile time but calls nothing on it: the class is
# only ever used as the `class =>` argument to proxmod's $api->add_method, and
# every real method dispatch happens inside pvedaemon, where the genuine
# libpve-common-perl module is present.
#
# Deliberately written from scratch rather than copied from libpve-common-perl
# (AGPL) so that `make build` works on a plain developer machine and in CI
# without vendoring Proxmox source into this Apache-2.0 repository.

use strict;
use warnings;

1;
