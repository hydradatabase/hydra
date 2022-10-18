CREATE OR REPLACE FUNCTION columnar.upgrade_columnar_storage(rel regclass)
  RETURNS VOID
  STRICT
  LANGUAGE c AS 'MODULE_PATHNAME', $$upgrade_columnar_storage$$;

COMMENT ON FUNCTION columnar.upgrade_columnar_storage(regclass)
  IS 'function to upgrade the columnar storage, if necessary';
