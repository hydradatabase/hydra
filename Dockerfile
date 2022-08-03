#syntax=docker/dockerfile-upstream:latest

FROM columnar_ext as columnar-ext

FROM spilobase

COPY --from=columnar-ext /pg_ext /
COPY files/default/postgres-appliance/scripts /scripts/

# Default envs
ENV PGVERSION=13 SPILO_PROVIDER=local PGUSER_SUPERUSER=postgres PGPASSWORD_SUPERUSER=hydra
