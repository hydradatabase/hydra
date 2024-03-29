CREATE TABLE t1(a int, b int) USING columnar;

CREATE TABLE t2(a int, b int) USING columnar;

INSERT INTO t1 SELECT generate_series(1, 1000000, 1) AS a, generate_series(2, 2000000, 2) AS b;
INSERT INTO t2 SELECT generate_series(1, 2000000, 1) AS a, generate_series(2, 4000000, 2) AS b;

SELECT * FROM columnar.stats('t1'::regclass);
SELECT * FROM columnar.stats('t2'::regclass);

SELECT * FROM columnar.vacuum('t1'::regclass);

SELECT * FROM columnar.stats('t1'::regclass);
SELECT * FROM columnar.stats('t2'::regclass);

SELECT * FROM columnar.vacuum('t2'::regclass);

SELECT * FROM columnar.stats('t1'::regclass);
SELECT * FROM columnar.stats('t2'::regclass);

DELETE FROM t1 WHERE a % 2 = 0;
DELETE FROM t1 WHERE a % 100 = 0;

SELECT * FROM columnar.stats('t1'::regclass);
SELECT * FROM columnar.stats('t2'::regclass);

SELECT * FROM columnar.vacuum('t1'::regclass);
SELECT * FROM columnar.vacuum('t2'::regclass);

SELECT * FROM columnar.stats('t1'::regclass);
SELECT * FROM columnar.stats('t2'::regclass);

SELECT COUNT(*) FROM t1;
SELECT COUNT(*) FROM t2;

SELECT SUM(a), SUM(b) FROM t1;
SELECT SUM(a), SUM(b) FROM t2;

INSERT INTO t1 SELECT generate_series(1000001, 2000000, 1) AS a, generate_series(2000002, 4000000, 2) AS b;
-- should show no holes between the previous insert and this insert
SELECT * FROM columnar.stats('t1'::regclass);

DELETE FROM t1 WHERE a BETWEEN 500000 AND 1500000;

SELECT * FROM columnar.vacuum('t1'::regclass);
-- should show no holes
SELECT * FROM columnar.stats('t1'::regclass);

DROP TABLE t1;
DROP TABLE t2;

CREATE TABLE t1(a int, b int) USING columnar;
INSERT INTO t1 SELECT generate_series(1, 1000000, 1) AS a, generate_series(2, 2000000, 2) AS b;

DELETE FROM t1 WHERE a % 3 = 0;

SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);
SELECT * FROM columnar.vacuum('t1'::regclass, 1);

DROP TABLE t1;

CREATE table t1 (
  i1 int,
  i2 int,
  i3 int,
  i4 int
) using columnar;
INSERT INTO t1 SELECT t, t, t, t FROM generate_series(1, 7000000) t;
DELETE FROM t1 WHERE i1 % 13 = 0;
UPDATE t1 SET i1 = i1 * 2, i2 = i2 * 2, i3 = i3 * 3, i4 = i4 * 4;
UPDATE t1 SET i4 = null where i4 % 23 = 0;

SELECT columnar.vacuum('t1');

SELECT count(i1), count(i2), count(i3), count(i4) FROM t1;

DROP TABLE t1;

CREATE TABLE cache_test (i INT) USING columnar;
INSERT INTO cache_test SELECT generate_series(1, 100, 1);

SET columnar.enable_column_cache = 't';
VACUUM cache_test;

-- should be true
SHOW columnar.enable_column_cache;

ANALYZE cache_test;

-- should be true
SHOW columnar.enable_column_cache;

DROP TABLE cache_test;
