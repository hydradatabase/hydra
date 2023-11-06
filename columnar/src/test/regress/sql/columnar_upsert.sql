CREATE TABLE upsert_test (
  i INT,
  t TEXT
) USING columnar;

CREATE UNIQUE INDEX ON upsert_test (t);

INSERT INTO upsert_test (i, t) VALUES (1, 'hello');
INSERT INTO upsert_test (i, t) VALUES (2, 'world');

-- should work, then roll back
BEGIN;
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'foo';
SELECT * FROM upsert_test;
ROLLBACK;

SELECT * FROM upsert_test;

-- should take affect immediately
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'bar';
SELECT * FROM upsert_test;

-- should work as expected
BEGIN;
INSERT INTO upsert_test (t) VALUES ( 'world' ) ON CONFLICT (t) DO UPDATE SET t = 'foo';
SELECT * FROM upsert_test;
COMMIT;

-- should also work as expected, a select can mask a bug
DELETE FROM upsert_test;
BEGIN;
INSERT INTO upsert_test (i, t) VALUES (1, 'hello');
INSERT INTO upsert_test (t) VALUES ( 'hello' ) ON CONFLICT (t) DO UPDATE SET t = 'bar';
COMMIT;

SELECT * FROM upsert_test;

DROP TABLE upsert_test;
