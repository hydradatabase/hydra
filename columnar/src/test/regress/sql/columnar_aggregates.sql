-- Custom direct aggregates implementation 

-- 1. SMALLINT

CREATE TABLE t_smallint(a SMALLINT) USING columnar;

INSERT INTO t_smallint SELECT g % 16384 FROM GENERATE_SERIES(0, 3000000) g;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t_smallint;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_smallint;

SET columnar.enable_vectorization TO false;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t_smallint;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_smallint;

SET columnar.enable_vectorization TO default;

DROP TABLE t_smallint;

-- 2. INT

CREATE TABLE t_int(a INT) USING columnar;

INSERT INTO t_int SELECT g FROM GENERATE_SERIES(0, 3000000) g;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_int;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_int;

SET columnar.enable_vectorization TO false;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_int;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_int;

SET columnar.enable_vectorization TO default;

DROP TABLE t_int;

-- 3. BIGINT

CREATE TABLE t_bigint(a BIGINT) USING columnar;

INSERT INTO t_bigint SELECT g FROM GENERATE_SERIES(0, 3000000) g;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_bigint;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_bigint;

SET columnar.enable_vectorization TO false;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_bigint;

SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t_bigint;

SET columnar.enable_vectorization TO default;

DROP TABLE t_bigint;

-- 4. DATE

CREATE TABLE t_date(a DATE) USING columnar;

INSERT INTO t_date VALUES ('2000-01-01'), ('2020-01-01'), ('2010-01-01'), ('2000-01-02');

EXPLAIN (verbose, costs off, timing off, summary off) SELECT MIN(a), MAX(a) FROM t_date;

SELECT MIN(a), MAX(a) FROM t_date;

SET columnar.enable_vectorization TO false;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT MIN(a), MAX(a) FROM t_date;

SELECT MIN(a), MAX(a) FROM t_date;

SET columnar.enable_vectorization TO default;

DROP TABLE t_date;

-- Test exception when we fallback to PG aggregator node

SET client_min_messages TO 'DEBUG1';

CREATE TABLE t_mixed(a INT, b BIGINT, c DATE, d TIME) using columnar;

INSERT INTO t_mixed VALUES (0, 1000, '2000-01-01', '23:50'), (10, 2000, '2010-01-01', '00:50');

-- Vectorized aggregate not found (TIME aggregates are not implemented)

EXPLAIN (verbose, costs off, timing off, summary off) SELECT MIN(d) FROM t_mixed;

-- Unsupported aggregate argument combination.

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(a + b) FROM t_mixed;

-- Vectorized Aggregates accepts only non-const values.

EXPLAIN (verbose, costs off, timing off, summary off) SELECT COUNT(1) FROM t_mixed;

-- Vectorized aggregate with DISTINCT not supported.

EXPLAIN (verbose, costs off, timing off, summary off) SELECT COUNT(DISTINCT a) FROM t_mixed;

-- github#145
-- Vectorized aggregate doesn't accept function as argument

EXPLAIN (verbose, costs off, timing off, summary off) SELECT SUM(length(b::text)) FROM t_mixed;

DROP TABLE t_mixed;

-- github#180
-- Vectorized aggregate does't accept filter on columns

CREATE TABLE t_filter(a INT) USING columnar;

INSERT INTO t_filter SELECT g FROM GENERATE_SERIES(0,100) g;

EXPLAIN (verbose, costs off, timing off, summary off) SELECT COUNT(a) FILTER (WHERE a > 90) FROM t_filter;

SELECT COUNT(a) FILTER (WHERE a > 90) FROM t_filter;

DROP TABLE t_filter;

SET client_min_messages TO default;