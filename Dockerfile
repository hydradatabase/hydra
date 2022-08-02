ARG COLUMNAR_EXT_IMAGE
ARG SPILO_IMAGE

FROM $COLUMNAR_EXT_IMAGE as columnar-ext

FROM $SPILO_IMAGE

COPY --from=columnar-ext /pg_ext /
COPY files/default/postgres-appliance/scripts /scripts/

ENV PGVERSION=13
