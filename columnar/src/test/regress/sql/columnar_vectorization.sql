CREATE TABLE t (id int, ts timestamp) USING columnar;
INSERT INTO t SELECT id, now() + id * '1 day'::interval FROM generate_series(1, 100000) id;
EXPLAIN (costs off) SELECT * FROM t WHERE ts between '2026-01-01'::timestamp and '2026-02-01'::timestamp;
DROP TABLE t;

CREATE TABLE t (id int, ts timestamptz) USING columnar;
INSERT INTO t SELECT id, now() + id * '1 day'::interval FROM generate_series(1, 100000) id;
EXPLAIN (costs off) SELECT * FROM t WHERE ts between '2026-01-01'::timestamptz and '2026-02-01'::timestamptz;
DROP TABLE t;
