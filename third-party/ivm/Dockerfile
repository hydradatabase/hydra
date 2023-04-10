FROM ubuntu:22.04 as setup

ARG PGSQL_IVM_TAG
ARG POSTGRES_BASE_VERSION=13

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install gnupg postgresql-common git -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get upgrade -y && apt-get install lsb-release gcc make libssl-dev autoconf pkg-config postgresql-${POSTGRES_BASE_VERSION} postgresql-server-dev-${POSTGRES_BASE_VERSION} libcurl4-gnutls-dev liblz4-dev libzstd-dev -y

FROM setup as builder

RUN git clone https://github.com/sraoss/pg_ivm --branch ${PGSQL_IVM_TAG} --single-branch && \
  cd pg_ivm && \
  DESTDIR=/pg_ivm make && \
  DESTDIR=/pg_ivm make install

FROM scratch as output

COPY --from=builder /pg_ivm /pg_ivm
