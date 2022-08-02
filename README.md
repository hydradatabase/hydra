# hydra

This repo contains build scripts of the Hydra Postgres image. The image is based on [zalando/spilo](https://github.com/zalando/spilo).

## Distributions

There are two distributions of the image:

1. Spilo + [Columnar Postgres extension](https://github.com/HydrasCo/citus)
2. Spilo + [Columnar Postgres extension](https://github.com/HydrasCo/citus) + [Hydra Postgres extension](https://github.com/HydrasCo/Hydras)

To build the first distribution, run:

```
TAG=1234 make docker_build
```

To build the second distribution, run:

```
TAG=1234 make docker_build_all
```

## Image Build Tags

Image build tag is in the format of `${SPILO_SHA}_${COLUMNAR_EXT_SHA}_${HYDRA_EXT_SHA}`, e.g. `72fb97e_ff32dd9_243ba49`.
The `latest` tag is alwasy updated to the latest.

## Spilo Version Update

The Hydra Docker build overrides the following Spilo scripts to enable extra Postgres extensions:

* [configure_spilo.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/configure_spilo.py)
* [spilo_commons.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/spilo_commons.py)
* [post_init.sh](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/post_init.sh)

To update to a newer Spilo version, you need to copy the above files from the specific Spilo version and add the corresponding bits to enable the Columnar/Hydra extensions.
Make sure you use `diff` to understand what needs to be added first before updating.
In the future, we may provide a script to make the update proecss easier.
