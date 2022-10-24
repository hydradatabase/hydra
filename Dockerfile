#syntax=docker/dockerfile:1

FROM postgres_base

# columnar ext
COPY --from=columnar /pg_ext /

# http deps
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
        ca-certificates \
        libcurl4-gnutls-dev \
	; \
	rm -rf /var/lib/apt/lists/*
# http ext
COPY --from=http /pg_ext /

COPY files/postgres/docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
