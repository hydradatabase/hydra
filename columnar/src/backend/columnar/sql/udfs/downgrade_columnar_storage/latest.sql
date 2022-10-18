CREATE OR REPLACE FUNCTION columnar.downgrade_columnar_storage(rel regclass)
  RETURNS VOID
  STRICT
  LANGUAGE c AS 'MODULE_PATHNAME', $$downgrade_columnar_storage$$;

COMMENT ON FUNCTION columnar.downgrade_columnar_storage(regclass)
  IS 'function to downgrade the columnar storage, if necessary';
