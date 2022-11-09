#syntax=docker/dockerfile:1

FROM postgres_base as setup

ARG MYSQL_FDW_TAG
ARG MULTICORN_TAG
ARG PYTHON_VERSION
ARG S3CSV_FDW_COMMIT
ARG GSPREADSHEET_FDW_COMMIT
ARG POSTGRES_BASE_VERSION=14

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install gnupg postgresql-common git -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get update; \
    apt-get upgrade -y; \
    apt-get install -y \
      postgresql-${POSTGRES_BASE_VERSION} \
      postgresql-server-dev-${POSTGRES_BASE_VERSION} \
      git \
      build-essential \
      libreadline-dev \
      zlib1g-dev \
      wget \
      flex \
      bison \
      libxml2-dev \
      libxslt-dev \
      libssl-dev \
      libxml2-utils \
      xsltproc \
      python${PYTHON_VERSION} \
      python${PYTHON_VERSION}-dev \
      python3-pip

# Update pip to the latest
RUN wget https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py && rm get-pip.py

FROM setup as builder

RUN git clone https://github.com/pgsql-io/multicorn2 --branch ${MULTICORN_TAG} --single-branch && \
  cd multicorn2 && \
  DESTDIR=/pg_ext USE_PGXS=1 make && \
  DESTDIR=/pg_ext USE_PGXS=1 make install

# install runtime python deps
RUN python3 -m pip install \
    git+https://github.com/eligoenergy/s3csv_fdw.git@${S3CSV_FDW_COMMIT} \
    # gspreadsheet_fdw deps
    gspread \
    oauth2client

# Use a fork
RUN git clone https://github.com/HydrasDB/gspreadsheet_fdw --single-branch && \
    cd gspreadsheet_fdw && \
    git checkout ${GSPREADSHEET_FDW_COMMIT} && \
    python3 setup.py install

FROM scratch as output

ARG PYTHON_VERSION

COPY --from=builder /pg_ext /pg_ext
COPY --from=builder /usr/local/lib/python${PYTHON_VERSION}/dist-packages /python-dist-packages
