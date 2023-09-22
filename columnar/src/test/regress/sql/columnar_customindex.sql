--
-- Test custom index: Test shows only that we are exchanging Index scan with CustomIndexScan while
-- all other information is still the same. The feature is not visible through EXPLAIN because output
-- will be same in both cases, but difference is noticeable how the columnar storage engine is read.
-- With index scan ALL columns are requested from storage (this can be significant overhead in performance)
-- while with CustomIndexScan we will request only columns that are needed - that are going to be used as output.
-- Test also shows that we are not using CustomIndexScan with heap tables and only on columnar tables.
--

SET columnar.enable_columnar_index_scan TO TRUE;

CREATE TABLE t(a INT PRIMARY KEY, b INT, c TEXT) USING columnar;
CREATE INDEX t_idx ON t USING btree(b);

CREATE TABLE t_heap(a INT PRIMARY KEY, b INT, c TEXT);
CREATE INDEX t_idx_heap ON t_heap USING btree(b);

INSERT INTO t SELECT g, g % 20, 'abcde_' || g FROM generate_series(1, 300000) g;
INSERT INTO t_heap SELECT g, g % 20, 'abcde_' || g FROM generate_series(1, 300000) g;

-- make sure that we test index scan
set columnar.enable_custom_scan TO 'off';
set enable_seqscan TO off;
set seq_page_cost TO 10000000;

EXPLAIN (VERBOSE) SELECT a FROM t WHERE b > 18 ORDER BY b LIMIT 10;

EXPLAIN (VERBOSE) SELECT a FROM t_heap WHERE b > 18 ORDER BY b LIMIT 10;

DROP TABLE t;
DROP TABLE t_heap;

SET columnar.enable_custom_index_scan TO default;
