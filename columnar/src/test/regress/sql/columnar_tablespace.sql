--
-- Test the columnar table move between tablespace.
--

-- An empty location can be allowed as a way to say that the tablespace
-- should be created as a directory in pg_tblspc, rather than being a
-- symlink.
SET allow_in_place_tablespaces = true;

CREATE TABLE regress_tbl (id int, info text) USING columnar;
INSERT INTO regress_tbl
  SELECT id, md5(id::text) FROM generate_series(1, 10000) id;

CREATE TABLESPACE regress_tblspc LOCATION '';

-- no relation moved to the new tablespace
SELECT c.relname FROM pg_class c, pg_tablespace s
  WHERE c.reltablespace = s.oid AND s.spcname = 'regress_tblspc';

ALTER TABLE regress_tbl SET TABLESPACE regress_tblspc;

-- a columnar table moved to the new tablespace
SELECT c.relname FROM pg_class c, pg_tablespace s
  WHERE c.reltablespace = s.oid AND s.spcname = 'regress_tblspc';

SELECT count(*) FROM regress_tbl;
