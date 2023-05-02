# Makefile for tests of the Citus extension

citus_subdir = src/test/regress
citus_top_builddir = ../../..

include $(citus_top_builddir)/Makefile.global

PG_VERSION_NUM := $(shell cat `$(PG_CONFIG) --includedir-server`/pg_config*.h \
		   | perl -ne 'print $$1 and exit if /PG_VERSION_NUM\s+(\d+)/')

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
input_files := $(patsubst $(citus_abs_srcdir)/input/%.source,sql/%.sql, $(wildcard $(citus_abs_srcdir)/input/*.source))
output_files := $(patsubst $(citus_abs_srcdir)/output/%.source,expected/%.out, $(wildcard $(citus_abs_srcdir)/output/*.source))

check-all: check-regression-columnar

check-regression-columnar:
ifeq ($(shell test $(PG_VERSION_NUM) -gt 149999; echo $$?),0)
	# postgres 15 removed the whole concept of input/output files
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < input/columnar_copyto.source > sql/columnar_copyto.sql
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < input/columnar_data_types.source > sql/columnar_data_types.sql
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < input/columnar_load.source > sql/columnar_load.sql
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < output/columnar_copyto.source > expected/columnar_copyto.out
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < output/columnar_data_types.source > expected/columnar_data_types.out
	sed "s#@abs_srcdir@#$(PWD)/src/test/regress#g" < output/columnar_load.source > expected/columnar_load.out
endif
	TEST_DIR=$(PWD) $(pg_regress_check) \
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
