#syntax=docker/dockerfile:1

FROM postgres_base as builder

ARG POSTGRES_BASE_VERSION
ARG ARROW_TAG
ARG AWS_SDK_TAG
ARG PARQUET_S3_FDW_COMMIT

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install gnupg postgresql-common git -y
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get update; \
    apt-get upgrade -y; \
    apt-get install -y \
      postgresql-${POSTGRES_BASE_VERSION} \
      postgresql-server-dev-${POSTGRES_BASE_VERSION} \
      build-essential \
      cmake \
      ninja-build \
      lsb-release \
      wget \
      # aws sdk deps
      libcurl4-openssl-dev \
      libssl-dev \
      uuid-dev \
      zlib1g-dev \
      libpulse-dev

RUN set -eux; \
    git clone https://github.com/aws/aws-sdk-cpp --recurse-submodules --branch ${AWS_SDK_TAG} --single-branch ;\
    mkdir sdk_build ;\
    cd sdk_build ;\
    cmake ../aws-sdk-cpp -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/usr/local/ -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_ONLY="s3" ;\
    nproc | xargs -I % make -j% ;\
    nproc | xargs -I % make -j% install

COPY scripts /tmp
RUN set -eux; \
    /tmp/install-arrow; \
    rm -f /tmp/install

# make sure the right postgres version is active
ENV PATH="/usr/lib/postgresql/${POSTGRES_BASE_VERSION}/bin:$PATH"

RUN set -eux; \
  git clone https://github.com/HydrasDB/parquet_s3_fdw --single-branch; \
  cd parquet_s3_fdw; \
  git checkout ${PARQUET_S3_FDW_COMMIT}; \
  DESTDIR=/pg_ext make USE_PGXS=1; \
  DESTDIR=/pg_ext make USE_PGXS=1 install

FROM scratch as output

COPY --from=builder /pg_ext /pg_ext
COPY --from=builder /usr/local/lib/* /lib
