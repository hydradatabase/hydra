#syntax=docker/dockerfile:1

FROM postgres_base

COPY --from=columnar /pg_ext /

RUN mkdir -p /docker-entrypoint-initdb.d && echo 'CREATE EXTENSION IF NOT EXISTS columnar;\nALTER EXTENSION columnar UPDATE;' > /docker-entrypoint-initdb.d/columnar.sql
