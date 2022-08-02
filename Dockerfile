ARG COLUMNAR_EXT_IMAGE
ARG SPILO_IMAGE

FROM $COLUMNAR_EXT_IMAGE as columnar-ext

FROM $SPILO_IMAGE

COPY --from=columnar-ext /pg_ext /
COPY files/default/postgres-appliance/scripts /scripts/

# Default envs
ENV PGVERSION=13 SPILO_PROVIDER=local PGUSER_SUPERUSER=postgres PGPASSWORD_SUPERUSER=hydra
