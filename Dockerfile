#syntax=docker/dockerfile:1

FROM postgres_base

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
        # http deps
        ca-certificates \
        libcurl4-gnutls-dev \
        # mysql deps
        default-libmysqlclient-dev \
	; \
	rm -rf /var/lib/apt/lists/*


# columnar ext
COPY --from=columnar /pg_ext /
# mysql ext
COPY --from=mysql /pg_ext /
# http ext
COPY --from=http /pg_ext /

COPY files/postgres/docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
