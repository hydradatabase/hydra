ARG COLUMNAR_EXT_IMAGE=ghcr.io/hydrasco/columnar_ext:latest
ARG SPILO_IMAGE=ghcr.io/hydrasco/spilo:latest

FROM $COLUMNAR_EXT_IMAGE as columnar-ext

FROM $SPILO_IMAGE

COPY --from=columnar-ext /pg_ext /
COPY files/default/postgres-appliance/scripts /scripts/

ENV PGVERSION=13
