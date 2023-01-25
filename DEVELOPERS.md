# Developing Hydra

Most of the work on Hydra exists outside of this repo through various extensions that we add to the image.
The hydra project exists for coordination and to build the final Docker image which contains Postgres and
these extensions.

Currently active projects are:

* [the Hydra columnar engine](https://github.com/hydradatabase/citus)

## Build

The Hydra Docker image is based on [zalando/spilo](https://github.com/zalando/spilo).
The image is a distribution of Spilo and the [Columnar PostgreSQL extension](https://github.com/hydradatabase/citus).

To build, run:

```
TAG=1234 TARGET=hydra make docker_build
```

## Image Build Tags

Image build tag is in the format of `${SPILO_SHA}_${COLUMNAR_EXT_SHA}`, e.g. `72fb97e_ff32dd9`.
The `latest` tag is always tagged to the latest main branch.

## Hydra Production Version

The `HYDRA_PROD_VER` file stores the version that Hydra runs in production.
Please bump it when a new production version is released.

## Spilo Version Update

Hydra Docker build overrides the following Spilo scripts to enable extra PostgreSQL extensions:

* [configure_spilo.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/configure_spilo.py)
* [spilo_commons.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/spilo_commons.py)
* [post_init.sh](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/post_init.sh)

To update to a newer Spilo version, you need to copy the above files from the specific Spilo version and add the corresponding bits to enable the Columnar extensions.
Make sure you use `diff` to understand what needs to be added first before updating.
In the future, we may provide a script to simplify the update process.
