#syntax=docker/dockerfile:1

FROM postgres_base as setup

ARG MYSQL_FDW_TAG
ARG POSTGRES_BASE_VERSION=13

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install gnupg postgresql-common git -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN export DEBIAN_FRONTEND=noninteractive && \
   apt-get update && \
   apt-get upgrade -y && \
   apt-get install -y \
     lsb-release \
     gcc \
     make \
     libssl-dev \
     autoconf \
     pkg-config \
     postgresql-${POSTGRES_BASE_VERSION} \
     postgresql-server-dev-${POSTGRES_BASE_VERSION} \
     libcurl4-gnutls-dev \
     liblz4-dev \
     libzstd-dev \
     default-libmysqlclient-dev

FROM setup as builder

RUN git clone https://github.com/EnterpriseDB/mysql_fdw && \
  cd mysql_fdw && \
  git checkout ${MYSQL_FDW_TAG} && \
  DESTDIR=/pg_ext make USE_PGXS=1 && \
  DESTDIR=/pg_ext make USE_PGXS=1 install

FROM scratch as output

COPY --from=builder /pg_ext /pg_ext
