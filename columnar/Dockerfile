FROM ubuntu:22.04 as setup

ARG POSTGRES_BASE_VERSION=13

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install gnupg postgresql-common -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get upgrade -y && apt-get install lsb-release gcc make libssl-dev autoconf pkg-config postgresql-${POSTGRES_BASE_VERSION} postgresql-server-dev-${POSTGRES_BASE_VERSION} libcurl4-gnutls-dev liblz4-dev libzstd-dev -y

FROM setup as builder

RUN --mount=target=/columnar,rw cd /columnar && ./configure && cd src/backend/columnar && DESTDIR=/pg_ext make install

FROM setup as checker

WORKDIR /columnar
COPY . .

RUN chown -R postgres:postgres .

USER postgres
ENV PATH=/usr/lib/postgresql/${POSTGRES_BASE_VERSION}/bin:$PATH
RUN ./configure && make clean extension

USER root
RUN make install-all

USER postgres
RUN make check-all

USER root
RUN DESTDIR=/pg_ext make -C /columnar/src/backend/columnar install 

FROM scratch as output

COPY --from=builder /pg_ext /pg_ext
