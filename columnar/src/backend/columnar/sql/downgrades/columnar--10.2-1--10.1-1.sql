-- columnar--10.2-1--10.1-1.sql

-- downgrade storage for all columnar relations
SELECT columnar.downgrade_columnar_storage(c.oid) FROM pg_class c, pg_am a
  WHERE c.relam = a.oid AND amname = 'columnar';

DROP FUNCTION columnar.upgrade_columnar_storage(regclass);
DROP FUNCTION columnar.downgrade_columnar_storage(regclass);

-- drop "first_row_number" column and the index defined on it
DROP INDEX columnar.stripe_first_row_number_idx;
ALTER TABLE columnar.stripe DROP COLUMN first_row_number;
