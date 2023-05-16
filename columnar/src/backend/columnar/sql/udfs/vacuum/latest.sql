CREATE OR REPLACE FUNCTION columnar.vacuum(rel regclass, optional_count int DEFAULT 0)
  RETURNS INT
  LANGUAGE c AS 'MODULE_PATHNAME', $$vacuum_columnar_table$$;

COMMENT ON FUNCTION columnar.vacuum(regclass, int)
  IS 'vacuum columnar table';

CREATE OR REPLACE FUNCTION columnar.stats(
  IN regclass,
  OUT stripeId bigint,
  OUT fileOffset bigint,
  OUT rowCount integer,
  OUT deletedRows integer,
  OUT chunkCount integer,
  OUT dataLength integer
) RETURNS SETOF record
LANGUAGE c
AS 'MODULE_PATHNAME', $$columnar_stats$$;
