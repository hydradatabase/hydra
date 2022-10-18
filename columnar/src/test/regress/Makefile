# Makefile for tests of the Citus extension

citus_subdir = src/test/regress
citus_top_builddir = ../../..

include $(citus_top_builddir)/Makefile.global

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files := $(patsubst $(citus_abs_srcdir)/input/%.source,sql/%.sql, $(wildcard $(citus_abs_srcdir)/input/*.source))
output_files := $(patsubst $(citus_abs_srcdir)/output/%.source,expected/%.out, $(wildcard $(citus_abs_srcdir)/output/*.source))

check-all: check-regression-columnar

check-regression-columnar:
	$(pg_regress_check) \
		--temp-config columnar_regression.conf \
		--load-extension=columnar \
		--schedule=$(citus_abs_srcdir)/columnar_schedule

clean-regression:
	rm -fr $(citus_abs_srcdir)/tmp_check
	rm -fr $(citus_abs_srcdir)/log
	rm -fr $(citus_abs_srcdir)/results
	rm -f  $(citus_abs_srcdir)/regression.diffs
	rm -f  $(citus_abs_srcdir)/regression.out
	rm -f  $(citus_abs_srcdir)/$(output_files) 
	rm -fr $(citus_abs_srcdir)/$(input_files)

clean: clean-regression
