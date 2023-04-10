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
        # s3 deps
        lsb-release \
        wget \
	; \
	rm -rf /var/lib/apt/lists/*

# mysql ext
COPY --from=mysql /pg_ext /

# http ext
COPY --from=http /pg_ext /

# multicorn ext
COPY --from=multicorn /pg_ext /
COPY --from=multicorn /python-dist-packages /usr/local/lib/python${PYTHON_VERSION}/dist-packages

# s3 ext
COPY --from=s3 /pg_ext /
COPY third-party/s3/scripts /tmp
RUN set -eux; \
    /tmp/install-arrow; \
    rm -f /tmp/install
COPY --from=s3 /lib/* /s3_lib/
RUN cp -r /s3_lib/* /usr/lib/$(uname -m)-linux-gnu/

# ivm ext
COPY --from=ivm /pg_ivm /

# columnar ext
COPY --from=columnar /pg_ext /

COPY files/postgres/docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
