DROP FUNCTION columnar.vacuum(REGCLASS, INT);
DROP FUNCTION columnar.stats(
  IN regclass,
  OUT stripeId bigint,
  OUT fileOffset bigint,
  OUT rowCount integer,
  OUT deletedRows integer,
  OUT chunkCount integer,
  OUT dataLength integer
);
DROP FUNCTION columnar.vacuum_full(NAME, REAL, INT);

#include "udfs/vacuum/11.1-10.sql"
