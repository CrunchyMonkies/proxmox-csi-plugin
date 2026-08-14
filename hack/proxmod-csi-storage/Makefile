# Build system for the proxmox-csi-storage proxmod extension.
#
# One Perl module, one manifest. Nothing is compiled or generated, and no
# Proxmox-owned path is touched.

DESTDIR ?=
prefix  ?= /usr

PERLDIR  := $(prefix)/share/perl5
SHAREDIR := $(prefix)/share/proxmod

INSTALL      := install
INSTALL_DATA := $(INSTALL) -m 0644

.PHONY: all build test install clean

all: build

# Nothing to build; check that the module at least parses. t/lib holds
# from-scratch stubs for the two PVE modules CSIStorage.pm needs at *compile*
# time, so this check is hermetic: it passes identically on a developer
# machine, in CI, and on a PVE host (where the stubs shadow the real modules,
# which is fine — nothing here executes them).
build: test
	perl -c -Iperl -It/lib perl/ProxmodExt/CSIStorage.pm

# Both tests read what they assert out of the module source, so they need no
# PVE and no stubs: the volname allow-list, which is the endpoint's only
# traversal guard and the only thing making directory-storage copies possible,
# and the proxyto flags, without which a method silently answers about whichever
# host received the request rather than the node named in its path.
test:
	perl t/volname.t
	perl t/proxyto.t

install:
	$(INSTALL) -d $(DESTDIR)$(PERLDIR)/ProxmodExt
	$(INSTALL_DATA) perl/ProxmodExt/CSIStorage.pm $(DESTDIR)$(PERLDIR)/ProxmodExt/
	# Writing here activates proxmod's dpkg trigger; no maintainer script
	# needed.
	$(INSTALL) -d $(DESTDIR)$(SHAREDIR)/extensions.d
	$(INSTALL_DATA) conf/50-proxmox-csi-storage.conf $(DESTDIR)$(SHAREDIR)/extensions.d/

clean:
	rm -rf debian/proxmox-csi-storage debian/.debhelper debian/files \
	       debian/*.substvars debian/debhelper-build-stamp
