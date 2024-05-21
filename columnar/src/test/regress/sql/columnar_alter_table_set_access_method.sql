--
-- Testing alter_table_set_access_method
--

-- 1. check conversion of 'heap' table to 'columnar'

CREATE TABLE t (a INT) USING heap;

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

INSERT INTO t VALUES (1),(2),(3);

SELECT COUNT(*) = 3 FROM t;

SELECT columnar.alter_table_set_access_method('t', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT COUNT(*)  = 3 FROM t;

DROP TABLE t;

-- 2. check conversion of 'columnar' table to 'heap'

CREATE TABLE t (a INT) USING columnar;

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

INSERT INTO t VALUES (1),(2),(3);

SELECT COUNT(*) = 3 FROM t;

SELECT columnar.alter_table_set_access_method('t', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT COUNT(*)  = 3 FROM t;

DROP TABLE t;

-- 3. check conversion of tables with trigger

CREATE TABLE t (a INT) USING heap;

CREATE or REPLACE FUNCTION trs_before() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'BEFORE STATEMENT %', TG_OP;
  RETURN NULL;
END;
$$;

CREATE TRIGGER tr_before_stmt BEFORE INSERT ON t
  FOR EACH STATEMENT EXECUTE PROCEDURE trs_before();

CREATE or REPLACE FUNCTION trs_after() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'AFTER STATEMENT %', TG_OP;
  RETURN NULL;
END;
$$;

CREATE TRIGGER tr_after_stmt AFTER INSERT ON t
  FOR EACH STATEMENT EXECUTE PROCEDURE trs_after();

CREATE or REPLACE FUNCTION trr_before() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
   RAISE NOTICE 'BEFORE ROW %: (%)', TG_OP, NEW.a;
   RETURN NEW;
END;
$$;

CREATE TRIGGER tr_before_row BEFORE INSERT ON t
  FOR EACH ROW EXECUTE PROCEDURE trr_before();

create or replace function trr_after() returns trigger language plpgsql as $$
BEGIN
   RAISE NOTICE 'AFTER ROW %: (%)', TG_OP, NEW.a;
   RETURN NEW;
END;
$$;

-- This trigger should not be applied to columnar table
CREATE TRIGGER tr_after_row AFTER INSERT ON t
  FOR EACH ROW EXECUTE PROCEDURE trr_after();

INSERT INTO t VALUES (1);

SELECT COUNT(*) = 4 FROM pg_trigger WHERE tgrelid = 't'::regclass::oid;

SELECT columnar.alter_table_set_access_method('t', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT COUNT(*) = 3 FROM pg_trigger WHERE tgrelid = 't'::regclass::oid;

INSERT INTO t VALUES (1);

-- Convert back to 'heap'

SELECT columnar.alter_table_set_access_method('t', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT COUNT(*) = 3 FROM pg_trigger WHERE tgrelid = 't'::regclass::oid;

INSERT INTO t VALUES (1);

SELECT COUNT(*) = 3 FROM t;

DROP TABLE t;

-- 4. check conversion of tables with indexes which can be created with columnar

CREATE TABLE index_table (a INT) USING heap;

CREATE INDEX idx1 ON index_table (a);

-- also create an index with statistics
CREATE INDEX idx2 ON index_table ((a+1));

ALTER INDEX idx2 ALTER COLUMN 1 SET STATISTICS 300;

SELECT COUNT(1) FROM pg_class WHERE relname = 'index_table' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT indexname FROM pg_indexes WHERE tablename = 'index_table' ORDER BY indexname;

SELECT columnar.alter_table_set_access_method('index_table', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 'index_table' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT indexname FROM pg_indexes WHERE tablename = 'index_table' ORDER BY indexname;

-- Convert back to 'heap'

SELECT columnar.alter_table_set_access_method('index_table', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 'index_table' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT indexname FROM pg_indexes WHERE tablename = 'index_table' ORDER BY indexname;

DROP TABLE index_table;

-- 5. Convert table with indexes and constraints

CREATE TABLE tbl (
  c1 CIRCLE,
  c2 TEXT,
  i int4[],
  p point,
  a int,
  EXCLUDE USING gist
    (c1 WITH &&, (c2::circle) WITH &&)
    WHERE (circle_center(c1) <> '(0,0)'),
  EXCLUDE USING btree
    (a WITH =)
	INCLUDE(p)
	WHERE (c2 < 'astring')
);

CREATE INDEX tbl_gin ON tbl USING gin (i);
CREATE INDEX tbl_gist ON tbl USING gist(p);
CREATE INDEX tbl_brin ON tbl USING brin (a) WITH (pages_per_range = 1);

CREATE INDEX tbl_hash ON tbl USING hash (c2);
ALTER TABLE tbl ADD CONSTRAINT tbl_unique UNIQUE (c2);

CREATE UNIQUE INDEX tbl_btree ON tbl USING btree (a);
ALTER TABLE tbl ADD CONSTRAINT tbl_pkey PRIMARY KEY USING INDEX tbl_btree;

SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'tbl'
ORDER BY indexname;

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'tbl'::regclass;

SELECT columnar.alter_table_set_access_method('tbl', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tbl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT indexname FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = 'tbl'::regclass
ORDER BY conname;

-- Convert back to 'heap'

SELECT columnar.alter_table_set_access_method('tbl', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tbl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT indexname FROM pg_indexes WHERE tablename = 'tbl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = 'tbl'::regclass
ORDER BY conname;

DROP TABLE tbl;

-- 6. Check non existing table
SELECT columnar.alter_table_set_access_method('some_test', 'columnar');

-- 7. Check if method is different than columnar / heap
CREATE TABLE t(a INT);
SELECT columnar.alter_table_set_access_method('t', 'other');
DROP TABLE t;

-- 8. Check if table have identity columns
CREATE TABLE identity_cols_test (a INT, b INT GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 42));
SELECT columnar.alter_table_set_access_method('identity_cols_test', 'columnar');
DROP TABLE identity_cols_test;

-- 9. Check conversion to same AM

CREATE TABLE t(a INT);

-- fail (heap -> heap)
SELECT columnar.alter_table_set_access_method('t', 'heap');
SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

-- ok (heap -> columnar)
SELECT columnar.alter_table_set_access_method('t', 'columnar');
SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

-- fail (columnar -> columnar)
SELECT columnar.alter_table_set_access_method('t', 'columnar');
SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

-- ok (columnar -> heap)
SELECT columnar.alter_table_set_access_method('t', 'heap');
SELECT COUNT(1) FROM pg_class WHERE relname = 't' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

DROP TABLE t;

-- 10. Check case sensitivity

CREATE TABLE "tBl" (
  c1 CIRCLE,
  "C2" TEXT,
  i int4[],
  p point,
  a int,
  EXCLUDE USING gist
    (c1 WITH &&, ("C2"::circle) WITH &&)
    WHERE (circle_center(c1) <> '(0,0)'),
  EXCLUDE USING btree
    (a WITH =)
	INCLUDE(p)
	WHERE ("C2" < 'astring')
);

CREATE INDEX "TBL_GIN" ON "tBl" USING gin (i);
CREATE INDEX tbl_gist ON "tBl" USING gist(p);
CREATE INDEX tbl_brin ON "tBl" USING brin (a) WITH (pages_per_range = 1);

CREATE INDEX tbl_hash ON "tBl" USING hash ("C2");
ALTER TABLE "tBl" ADD CONSTRAINT tbl_unique UNIQUE ("C2");

CREATE UNIQUE INDEX tbl_btree ON "tBl" USING btree (a);
ALTER TABLE "tBl" ADD CONSTRAINT tbl_pkey PRIMARY KEY USING INDEX tbl_btree;

SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'tBl'
ORDER BY indexname;

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = '"tBl"'::regclass;

SELECT columnar.alter_table_set_access_method('"tBl"', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tBl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT indexname FROM pg_indexes WHERE tablename = 'tBl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = '"tBl"'::regclass
ORDER BY conname;

-- Convert back to 'heap'

SELECT columnar.alter_table_set_access_method('"tBl"', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tBl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT indexname FROM pg_indexes WHERE tablename = 'tBl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = '"tBl"'::regclass
ORDER BY conname;

DROP TABLE "tBl";

-- 11. Check case sensitivity and schema sensitivity

CREATE SCHEMA "tEST";

CREATE TABLE "tEST"."tBl" (
  c1 CIRCLE,
  "C2" TEXT,
  i int4[],
  p point,
  a int,
  EXCLUDE USING gist
    (c1 WITH &&, ("C2"::circle) WITH &&)
    WHERE (circle_center(c1) <> '(0,0)'),
  EXCLUDE USING btree
    (a WITH =)
	INCLUDE(p)
	WHERE ("C2" < 'astring')
);

CREATE INDEX "TBL_GIN" ON "tEST"."tBl" USING gin (i);
CREATE INDEX tbl_gist ON "tEST"."tBl" USING gist(p);
CREATE INDEX tbl_brin ON "tEST"."tBl" USING brin (a) WITH (pages_per_range = 1);

CREATE INDEX tbl_hash ON "tEST"."tBl" USING hash ("C2");
ALTER TABLE "tEST"."tBl" ADD CONSTRAINT tbl_unique UNIQUE ("C2");

CREATE UNIQUE INDEX tbl_btree ON "tEST"."tBl" USING btree (a);
ALTER TABLE "tEST"."tBl" ADD CONSTRAINT tbl_pkey PRIMARY KEY USING INDEX tbl_btree;

SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'tBl'
ORDER BY indexname;

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = '"tEST"."tBl"'::regclass;

SELECT columnar.alter_table_set_access_method('"tEST"."tBl"', 'columnar');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tBl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'columnar');

SELECT indexname FROM pg_indexes WHERE tablename = 'tBl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = '"tEST"."tBl"'::regclass
ORDER BY conname;

-- Convert back to 'heap'

SELECT columnar.alter_table_set_access_method('"tEST"."tBl"', 'heap');

SELECT COUNT(1) FROM pg_class WHERE relname = 'tBl' AND relam = (SELECT oid FROM pg_am WHERE amname = 'heap');

SELECT indexname FROM pg_indexes WHERE tablename = 'tBl' ORDER BY indexname;

SELECT conname FROM pg_constraint
WHERE conrelid = '"tEST"."tBl"'::regclass
ORDER BY conname;

DROP TABLE "tEST"."tBl";

DROP SCHEMA "tEST";