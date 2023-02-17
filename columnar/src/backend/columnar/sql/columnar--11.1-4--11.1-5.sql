-- columnar--11.1-4--11.1-5.sql

#include "udfs/alter_table_set_access_method/11.1-5.sql"

SET search_path TO columnar;

CREATE SEQUENCE row_mask_seq START WITH 1 INCREMENT BY 1; 

CREATE TABLE row_mask (
	id BIGINT NOT NULL,
	storage_id BIGINT NOT NULL,
	stripe_id BIGINT NOT NULL,
	chunk_id INT NOT NULL,
	start_row_number BIGINT NOT NULL,
	end_row_number BIGINT NOT NULL,
	deleted_rows INT NOT NULL,
	mask BYTEA,
	PRIMARY KEY (id, storage_id, start_row_number, end_row_number)
) WITH (user_catalog_table = true);

ALTER TABLE columnar.chunk_group ADD COLUMN deleted_rows BIGINT NOT NULL DEFAULT 0;

ALTER TABLE columnar.row_mask ADD CONSTRAINT row_mask_stripe_unique
UNIQUE (storage_id, stripe_id, start_row_number);

ALTER TABLE columnar.row_mask ADD CONSTRAINT row_mask_chunk_unique
UNIQUE (storage_id, stripe_id, chunk_id, start_row_number);

REVOKE SELECT ON columnar.row_mask FROM PUBLIC;

COMMENT ON TABLE row_mask IS 'Columnar chunk mask metadata';

#include "udfs/create_table_row_mask/11.1-5.sql"

-- find all columnar tables and create empty row mask for them

SELECT columnar.create_table_row_mask(oid) FROM pg_class
	WHERE
	pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'columnar')
	AND
	pg_class.oid IN
	(
		SELECT (quote_ident(pg_tables.schemaname) || '.' ||
				quote_ident(pg_tables.tablename))::regclass::oid
		FROM pg_tables
		WHERE pg_tables.schemaname <> 'information_schema' AND
			  pg_tables.schemaname <> 'pg_catalog' AND
			  pg_tables.schemaname <> 'columnar'
	);


RESET search_path;