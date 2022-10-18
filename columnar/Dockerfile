FROM ubuntu:18.04

# Postgres 13
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install curl gnupg lsb-release apt-utils -y
RUN curl -sSf https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN cat /etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get install gcc make libssl-dev autoconf pkg-config postgresql-13 postgresql-server-dev-13 -y

COPY . /citus

RUN apt-get install libcurl4-gnutls-dev liblz4-dev libzstd-dev -y
RUN cd /citus && ./configure
RUN cd /citus/src/backend/columnar && DESTDIR=/pg_ext make install
