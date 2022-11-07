#syntax=docker/dockerfile:1

FROM postgres_base

ARG PYTHON_VERSION

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
        # http deps
        ca-certificates \
        libcurl4-gnutls-dev \
        # mysql deps
        default-libmysqlclient-dev \
        # multicorn deps
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-dev \
	; \
	rm -rf /var/lib/apt/lists/*

# columnar ext
COPY --from=columnar /pg_ext /
# mysql ext
COPY --from=mysql /pg_ext /
# http ext
COPY --from=http /pg_ext /
# multicorn ext
COPY --from=multicorn /pg_ext /
COPY --from=multicorn /python-dist-packages /usr/local/lib/python${PYTHON_VERSION}/dist-packages

COPY files/postgres/docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
