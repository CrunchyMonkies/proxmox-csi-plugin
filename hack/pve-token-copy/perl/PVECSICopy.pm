package PVECSICopy;

# Trivial, always-compiles loader. Pulled into pvedaemon/pveproxy via a
# command-line `-MPVECSICopy` in each daemon's systemd ExecStart (an env-var
# loader is impossible: the daemons run `perl -T`, and taint mode ignores
# PERL5LIB/PERL5OPT). ALL real work is isolated in PVECSICopy::Impl behind an
# eval, so neither a failure here nor anywhere in the implementation (including a
# compile error, or a future PVE-internal change) can abort daemon startup — a
# load-time die would take the node's management API down. On any failure it warns
# and registers nothing, leaving Proxmox exactly as shipped.
#
# Keep this file byte-trivial: it must ALWAYS parse/compile.

use strict;
use warnings;

eval {
    require PVECSICopy::Impl;
    PVECSICopy::Impl::register();
    1;
} or do {
    warn "PVECSICopy: endpoint NOT registered (fail-safe, PVE left as shipped): "
        . ($@ || 'unknown error');
};

1;
