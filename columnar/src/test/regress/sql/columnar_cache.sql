CREATE TABLE big_table (
  id INT,
  firstname TEXT,
  lastname TEXT
) USING columnar;

INSERT INTO big_table (id, firstname, lastname)
  SELECT i,
         CONCAT('firstname-', i),
         CONCAT('lastname-', i)
    FROM generate_series(1, 1000000) as i;

-- get some baselines from multiple chunks
SELECT firstname,
       lastname,
       SUM(id)
  FROM big_table
 WHERE id < 1000
 GROUP BY firstname,
       lastname
UNION
SELECT firstname,
       lastname,
       SUM(id)
  FROM big_table
 WHERE id BETWEEN 15000 AND 16000
 GROUP BY firstname,
       lastname
 ORDER BY firstname;


-- enable caching
SET columnar.enable_column_cache = 't';

-- the results should be the same as above
SELECT firstname,
       lastname,
       SUM(id)
  FROM big_table
 WHERE id < 1000
 GROUP BY firstname,
       lastname
UNION
SELECT firstname,
       lastname,
       SUM(id)
  FROM big_table
 WHERE id BETWEEN 15000 AND 16000
 GROUP BY firstname,
       lastname
 ORDER BY firstname;

-- disable caching
SET columnar.enable_column_cache = 'f';

CREATE TABLE test_2 (
  value INT,
  updated_value INT
) USING columnar;

INSERT INTO test_2 (value)
  SELECT generate_series(1, 1000000, 1);

BEGIN;
SELECT SUM(value)
  FROM test_2;

UPDATE test_2
   SET updated_value = value * 2;

SELECT SUM(updated_value)
  FROM test_2;

DELETE FROM test_2
 WHERE value % 2 = 0;

SELECT SUM(value)
  FROM test_2;
COMMIT;

DROP TABLE test_2;

set columnar.enable_column_cache = 't';

CREATE TABLE test_2 (
  value INT,
  updated_value INT
) USING columnar;

INSERT INTO test_2 (value)
  SELECT generate_series(1, 1000000, 1);

BEGIN;
SELECT SUM(value)
  FROM test_2;

UPDATE test_2
   SET updated_value = value * 2;

SELECT SUM(updated_value)
  FROM test_2;

DELETE FROM test_2
 WHERE value % 2 = 0;

SELECT SUM(value)
  FROM test_2;
COMMIT;

DROP TABLE test_2;

CREATE TABLE t1 (i int) USING columnar;

INSERT INTO t1 SELECT generate_series(1, 1000000, 1);
EXPLAIN SELECT COUNT(*) FROM t1;
DROP TABLE t1;
