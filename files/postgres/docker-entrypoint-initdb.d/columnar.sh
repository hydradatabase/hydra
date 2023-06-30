#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE EXTENSION IF NOT EXISTS columnar;
  ALTER EXTENSION columnar UPDATE;
  ALTER DATABASE "${POSTGRES_DB}" SET default_table_access_method = 'columnar';
EOSQL
