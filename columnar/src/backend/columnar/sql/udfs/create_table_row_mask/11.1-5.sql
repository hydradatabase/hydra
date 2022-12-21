CREATE OR REPLACE FUNCTION columnar.create_table_row_mask(
    table_name regclass)
    RETURNS bool
    LANGUAGE C
AS 'MODULE_PATHNAME', 'create_table_row_mask';

COMMENT ON FUNCTION columnar.create_table_row_mask(
    table_name regclass)
IS 'Create empty row mask for table';