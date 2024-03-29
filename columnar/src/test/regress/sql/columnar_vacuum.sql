SET columnar.compression TO 'none';

SELECT count(distinct storage_id) AS columnar_table_count FROM columnar.stripe \gset

CREATE TABLE t(a int, b int) USING columnar;

CREATE VIEW t_stripes AS
SELECT * FROM columnar.stripe a, pg_class b
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid(b.oid) AND b.relname='t';

SELECT count(*) FROM t_stripes;

INSERT INTO t SELECT i, i * i FROM generate_series(1, 10) i;
INSERT INTO t SELECT i, i * i FROM generate_series(11, 20) i;
INSERT INTO t SELECT i, i * i FROM generate_series(21, 30) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM t_stripes;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

-- vacuum full should merge stripes together
VACUUM FULL t;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM t_stripes;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

-- test the case when all data cannot fit into a single stripe
SELECT columnar.alter_columnar_table_set('t', stripe_row_limit => 1000);
INSERT INTO t SELECT i, 2 * i FROM generate_series(1,2500) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM t_stripes;

VACUUM FULL t;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM t_stripes;

-- VACUUM FULL doesn't reclaim dropped columns, but converts them to NULLs
ALTER TABLE t DROP COLUMN a;

SELECT stripe_num, attr_num, chunk_group_num, minimum_value IS NULL, maximum_value IS NULL
FROM columnar.chunk a, pg_class b
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid(b.oid) AND b.relname='t' ORDER BY 1, 2, 3;

VACUUM FULL t;

SELECT stripe_num, attr_num, chunk_group_num, minimum_value IS NULL, maximum_value IS NULL
FROM columnar.chunk a, pg_class b
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid(b.oid) AND b.relname='t' ORDER BY 1, 2, 3;

-- Make sure we cleaned-up the transient table metadata after VACUUM FULL commands
SELECT count(distinct storage_id) - :columnar_table_count FROM columnar.stripe;

-- do this in a transaction so concurrent autovacuum doesn't interfere with results
BEGIN;
SAVEPOINT s1;
SELECT count(*) FROM t;
SELECT pg_size_pretty(pg_relation_size('t'));
INSERT INTO t SELECT i FROM generate_series(1, 10000) i;
SELECT pg_size_pretty(pg_relation_size('t'));
SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s1;

-- not truncated by VACUUM or autovacuum yet (being in transaction ensures this),
-- so relation size should be same as before.
SELECT pg_size_pretty(pg_relation_size('t'));
COMMIT;

-- vacuum should truncate the relation to the usable space
VACUUM t;
SELECT pg_size_pretty(pg_relation_size('t'));
SELECT count(*) FROM t;

-- add some stripes with different compression types and create some gaps,
-- then vacuum to print stats

BEGIN;
SELECT columnar.alter_columnar_table_set('t',
    chunk_group_row_limit => 1000,
    stripe_row_limit => 2000,
    compression => 'pglz');
SAVEPOINT s1;
INSERT INTO t SELECT i FROM generate_series(1, 1500) i;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO t SELECT i / 5 FROM generate_series(1, 1500) i;
SELECT columnar.alter_columnar_table_set('t', compression => 'none');
SAVEPOINT s2;
INSERT INTO t SELECT i FROM generate_series(1, 1500) i;
ROLLBACK TO SAVEPOINT s2;
INSERT INTO t SELECT i / 5 FROM generate_series(1, 1500) i;
COMMIT;

VACUUM t;
select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

SELECT count(*) FROM t;

-- check that we report chunks with data for dropped columns
ALTER TABLE t ADD COLUMN c int;
INSERT INTO t SELECT 1, i / 5 FROM generate_series(1, 1500) i;
ALTER TABLE t DROP COLUMN c;

VACUUM t;

-- vacuum full should remove chunks for dropped columns
-- note that, a chunk will be stored in non-compressed for if compression
-- doesn't reduce its size.
SELECT columnar.alter_columnar_table_set('t', compression => 'pglz');
VACUUM FULL t;
VACUUM t;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP TABLE t;
DROP VIEW t_stripes;

-- Make sure we cleaned the metadata for t too
SELECT count(distinct storage_id) - :columnar_table_count FROM columnar.stripe;

-- A table with high compression ratio
SET columnar.compression TO 'pglz';
SET columnar.stripe_row_limit TO 1000000;
SET columnar.chunk_group_row_limit TO 100000;
CREATE TABLE t(a int, b char, c text) USING columnar;
INSERT INTO t SELECT 1, 'a', 'xyz' FROM generate_series(1, 1000000) i;

VACUUM t;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP TABLE t;

  -- Vacuuming on multiple stripes

SET columnar.compression TO default;
SET columnar.stripe_row_limit TO default;
SET columnar.chunk_group_row_limit TO default;

CREATE TABLE t(a INT, b INT) USING columnar;

SELECT columnar_test_helpers.columnar_relation_storageid(pg_class.oid) AS t_oid FROM pg_class WHERE relname='t' \gset

INSERT INTO t SELECT g, g % 10 from generate_series(1,100000) g;
INSERT INTO t SELECT g, g % 10 from generate_series(1,100000) g;

SELECT COUNT(*) = 2 FROM columnar.stripe WHERE storage_id = :t_oid;

VACUUM t;

  -- No change since we can't combine 2 stripe because row_number is higher
  -- than maximum row number per stripe

SELECT COUNT(*) = 2 FROM columnar.stripe WHERE storage_id = :t_oid;

DELETE FROM t WHERE a % 2 = 0;

SELECT COUNT(*) = 0 FROM columnar.chunk_group WHERE deleted_rows = 0 AND storage_id = :t_oid; 
SELECT COUNT(*) = 0 FROM columnar.row_mask WHERE deleted_rows = 0 AND storage_id = :t_oid; 

VACUUM t;

  -- Stripes are merged into one stripe because total number of non-deleted rows
  -- is less than maximum stripe row number

SELECT COUNT(*) = 1 FROM columnar.stripe WHERE storage_id = :t_oid;

SELECT COUNT(*) FROM columnar.chunk_group WHERE deleted_rows = 0 AND storage_id = :t_oid; 
SELECT COUNT(*) FROM columnar.row_mask WHERE deleted_rows = 0 AND storage_id = :t_oid; 

DROP TABLE t;

  -- Vacuum on single stripe

CREATE TABLE t(a INT, b INT) USING columnar;

SELECT columnar_test_helpers.columnar_relation_storageid(pg_class.oid) AS t_oid FROM pg_class WHERE relname='t' \gset

INSERT INTO t SELECT g, g % 10 from generate_series(1,100000) g;

SELECT COUNT(*) = (SELECT COUNT(*) FROM columnar.row_mask WHERE storage_id = :t_oid) 
  FROM columnar.chunk_group WHERE storage_id = :t_oid;

SELECT COUNT(*) AS columnar_chunk_group_rows FROM columnar.chunk_group WHERE storage_id = :t_oid \gset
SELECT COUNT(*) AS columnar_row_mask_rows FROM columnar.row_mask WHERE storage_id = :t_oid \gset

DELETE FROM t WHERE a % 2 = 0;

SELECT COUNT(*) AS columnar_chunk_group_after_delete_rows FROM columnar.chunk_group 
  WHERE deleted_rows >= 1 AND storage_id = :t_oid \gset
SELECT COUNT(*) AS columnar_row_mask_after_delete_rows FROM columnar.row_mask 
  WHERE deleted_rows >= 1 AND storage_id = :t_oid \gset

SELECT :columnar_chunk_group_after_delete_rows = :columnar_chunk_group_rows;
SELECT :columnar_row_mask_after_delete_rows = :columnar_row_mask_rows;

SELECT SUM(deleted_rows) FROM columnar.row_mask WHERE storage_id = :t_oid;
SELECT SUM(deleted_rows) FROM columnar.chunk_group WHERE storage_id = :t_oid;

VACUUM t;

  -- No more deleted_rows after vacuum

SELECT COUNT(*) = 0 FROM columnar.chunk_group WHERE deleted_rows >= 1 AND storage_id = :t_oid;
SELECT COUNT(*) = 0 FROM columnar.row_mask WHERE deleted_rows >= 1 AND storage_id = :t_oid;

SELECT COUNT(*) = (:columnar_chunk_group_rows / 2) FROM columnar.chunk_group 
  WHERE deleted_rows = 0 AND storage_id = :t_oid;
SELECT COUNT(*) = (:columnar_row_mask_rows / 2) FROM columnar.row_mask 
  WHERE deleted_rows = 0 AND storage_id = :t_oid;

SELECT COUNT(*) AS table_count FROM t \gset
SELECT COUNT(*) AS table_count_mod_7 FROM t WHERE a % 7 = 0 \gset

SELECT (:table_count_mod_7 / :table_count) < 0.2;

DELETE FROM t WHERE a % 7 = 0;

VACUUM t;

  -- Vacuuming will not be done because number of deleted rows / total_rows is less than 20%

SELECT COUNT(*) = (:columnar_chunk_group_rows / 2) FROM columnar.chunk_group WHERE storage_id = :t_oid;
SELECT COUNT(*) = (:columnar_row_mask_rows / 2) FROM columnar.row_mask WHERE storage_id = :t_oid;

DROP TABLE t;

-- Verify that we can vacuum humongous fields
CREATE TABLE t (id SERIAL, data TEXT) USING columnar;

SELECT columnar_test_helpers.columnar_relation_storageid(pg_class.oid) AS t_oid FROM pg_class WHERE relname='t' \gset

INSERT INTO t SELECT 1, repeat('a', 255000000);
INSERT INTO t SELECT 2, repeat('b', 255000000);
INSERT INTO t SELECT 3, repeat('c', 255000000);
INSERT INTO t SELECT 4, repeat('d', 255000000);
INSERT INTO t SELECT 5, repeat('e', 255000000);
INSERT INTO t SELECT 6, repeat('f', 255000000);

SELECT COUNT(*) = 6 FROM columnar.stripe WHERE storage_id = :t_oid;

VACUUM t;

SELECT COUNT(*) = 3 FROM columnar.stripe WHERE storage_id = :t_oid;

INSERT INTO t SELECT 7, repeat('g', 255000000);

VACUUM t;

SELECT COUNT(*) = 4 FROM columnar.stripe WHERE storage_id = :t_oid;

DROP TABLE t;

CREATE TABLE cache_test (i INT) USING columnar;
INSERT INTO cache_test SELECT generate_series(1, 100, 1);

SET columnar.enable_column_cache = 't';
SELECT columnar.vacuum('cache_test');

-- should be true
SHOW columnar.enable_column_cache;

DROP TABLE cache_test;
