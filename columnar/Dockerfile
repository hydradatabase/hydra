FROM ubuntu:22.04 as builder

ARG POSTGRES_BASE_VERSION=13

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install gnupg postgresql-common -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN apt update && DEBIAN_FRONTEND=noninteractive apt upgrade -y && DEBIAN_FRONTEND=noninteractive apt install lsb-release gcc make libssl-dev autoconf pkg-config postgresql-${POSTGRES_BASE_VERSION} postgresql-server-dev-${POSTGRES_BASE_VERSION} libcurl4-gnutls-dev liblz4-dev libzstd-dev -y

RUN --mount=target=/citus,rw cd /citus && ./configure && cd src/backend/columnar && DESTDIR=/pg_ext make install

FROM scratch

COPY --from=builder /pg_ext /pg_ext
