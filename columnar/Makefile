# Citus toplevel Makefile

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

all: extension

# build extension
extension: $(citus_top_builddir)/src/include/citus_version.h
	$(MAKE) -C src/backend/columnar all
	
install-extension: extension
	$(MAKE) -C src/backend/columnar install

install-headers: extension
	$(INSTALL_DATA) $(citus_top_builddir)/src/include/citus_version.h '$(DESTDIR)$(includedir_server)/'

clean-extension:
	$(MAKE) -C src/backend/columnar/ clean

.PHONY: extension install-extension clean-extension

# Add to generic targets
install: install-extension install-headers

install-all: install-headers
	$(MAKE) -C src/backend/columnar/ install-all

clean: clean-extension clean-regression

# apply or check style
reindent:
	${citus_abs_top_srcdir}/ci/fix_style.sh

.PHONY: reindent

check: all install-all check-all

check-all:
	$(MAKE) -C src/test/regress check-all

clean-regression:
	$(MAKE) -C src/test/regress clean-regression

.PHONY: all check clean install install-all
