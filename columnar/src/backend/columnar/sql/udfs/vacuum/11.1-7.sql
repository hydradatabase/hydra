COMMENT ON FUNCTION columnar.stats(regclass)
  IS 'columnar stripe statistics';

CREATE OR REPLACE FUNCTION columnar.vacuum_full(schema NAME DEFAULT 'public', sleep_time REAL DEFAULT .1, stripe_count INT DEFAULT 25)
RETURNS VOID AS $$
DECLARE
  tables REGCLASS[];
  tablename REGCLASS;
  finished BOOL;
  count INT;
BEGIN
  SELECT ARRAY_AGG(c.relname) INTO tables
  FROM pg_catalog.pg_class c
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
  WHERE c.relkind = 'r'
        AND n.nspname <> 'pg_catalog'
        AND n.nspname !~ '^pg_toast'
        AND n.nspname <> 'information_schema'
    AND pg_catalog.pg_table_is_visible(c.oid)
    AND am.amname = 'columnar'
    AND n.nspname = schema
  ORDER BY 1;

  FOREACH tablename IN ARRAY tables
  LOOP
    finished := 'f';
    count := 1;
    WHILE count > 0 LOOP
      SELECT columnar.vacuum(tablename, stripe_count) INTO count;
      PERFORM pg_sleep(sleep_time);
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION columnar.vacuum_full(name, real, int)
  IS 'vacuum columnar schema in full incrementally';
