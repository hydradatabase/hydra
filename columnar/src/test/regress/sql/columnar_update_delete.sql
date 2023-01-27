
CREATE TABLE columnar_update(i int, j int) USING columnar;

INSERT INTO columnar_update VALUES (1, 10);
INSERT INTO columnar_update VALUES (2, 20);
INSERT INTO columnar_update VALUES (3, 30);

UPDATE columnar_update SET j = j+1 WHERE i = 2;
DELETE FROM columnar_update WHERE i = 2;

INSERT INTO columnar_update VALUES
  (3, 5),
  (4, 5),
  (5, 5)
  ON CONFLICT DO NOTHING;

-- should fail because we can't create an index on columnar_update.i
INSERT INTO columnar_update VALUES
  (3, 5),
  (4, 5),
  (5, 5)
  ON CONFLICT (i) DO NOTHING;

SELECT * FROM columnar_update WHERE i = 2 FOR SHARE;
SELECT * FROM columnar_update WHERE i = 2 FOR UPDATE;

SELECT * FROM columnar_update WHERE ctid = '(0,2)';

DROP TABLE columnar_update;

CREATE TABLE parent(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

CREATE TABLE p0 PARTITION OF parent
  FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
  USING COLUMNAR;
CREATE TABLE p1 PARTITION OF parent
  FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
  USING COLUMNAR;
CREATE TABLE p2 PARTITION OF parent
  FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO parent VALUES('2020-01-15', 10, 100, 'one thousand'); -- columnar
INSERT INTO parent VALUES('2020-02-15', 20, 200, 'two thousand'); -- columnar
INSERT INTO parent VALUES('2020-03-15', 30, 300, 'three thousand'); -- row
INSERT INTO parent VALUES('2020-03-21', 31, 301, 'three thousand and one'); -- row
INSERT INTO parent VALUES('2020-03-22', 32, 302, 'three thousand and two'); -- row
INSERT INTO parent VALUES('2020-03-23', 33, 303, 'three thousand and three'); -- row

SELECT * FROM parent;


UPDATE p2 SET i = i+1 WHERE ts = '2020-03-15';
DELETE FROM p2 WHERE ts = '2020-03-21';


UPDATE p1 SET i = i+1 WHERE ts = '2020-02-15';
DELETE FROM p1 WHERE ts = '2020-02-15';

UPDATE parent SET i = i+1 WHERE ts = '2020-03-15';
DELETE FROM parent WHERE ts = '2020-03-22';


UPDATE parent SET i = i+1 WHERE ts > '2020-02-15';
DELETE FROM parent WHERE ts > '2020-02-15';

UPDATE parent SET i = i+1 WHERE n = 300;
DELETE FROM parent WHERE n = 303;

SELECT * FROM parent;

-- detach partition
ALTER TABLE parent DETACH PARTITION p0;
DROP TABLE p0;

DROP TABLE parent;

-- delete / update in transaction

CREATE TABLE columnar_transaction_update(i INT, j INT) using columnar;

INSERT INTO columnar_transaction_update VALUES (1, 10);
INSERT INTO columnar_transaction_update VALUES (2, 20);
INSERT INTO columnar_transaction_update VALUES (3, 30);

INSERT INTO columnar_transaction_update SELECT g, g * 10 FROM generate_series(4, 100) g;
INSERT INTO columnar_transaction_update SELECT g, g * 10 FROM generate_series(101, 1000) g;
INSERT INTO columnar_transaction_update SELECT g, g * 10 FROM generate_series(1001, 20000) g;

SELECT COUNT(*) from columnar_transaction_update;

START TRANSACTION;

DELETE FROM columnar_transaction_update where i % 2 = 0;
SELECT COUNT(*) from columnar_transaction_update;

UPDATE columnar_transaction_update SET j = -1 WHERE i % 3 = 0;
SELECT COUNT(*) from columnar_transaction_update WHERE j = -1;

COMMIT;

SELECT COUNT(*) from columnar_transaction_update;
SELECT COUNT(*) from columnar_transaction_update WHERE j = -1;

DROP TABLE columnar_transaction_update;
