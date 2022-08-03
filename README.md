# Hydra

[Hydra](https://hydras.io/) is an open-source PostgreSQL data warehouse alternative to Snowflake.
Here is a list of high-leveled features:

* [x] columnar store
* [ ] vectorized execution

## Quickstart

The following is a quick local setup. For production use, sign up for [our cloud offering](https://hydras.io/#early-access).

```console
docker run --rm -d -p 127.0.0.1:5432:5432 ghcr.io/hydrasco/hydra
psql postgres://postgres:hydra@127.0.0.1:5432
```

You can find detailed usage [here](https://docs.hydras.io/features/columnar).

## Build

The Hydra Docker image is based on [zalando/spilo](https://github.com/zalando/spilo).
There are two image distributions:

1. Spilo + [Columnar PostgreSQL extension](https://github.com/HydrasCo/citus)
2. Spilo + [Columnar PostgreSQL extension](https://github.com/HydrasCo/citus) + [Hydra PostgreSQL extension](https://github.com/HydrasCo/Hydras)

To build the first distribution, run:

```
TAG=1234 TARGET=hydra make docker_build
```

To build the second distribution, run:

```
TAG=1234 TARGET=hydra-all make docker_build
```

## Image Build Tags

Image build tag is in the format of `${SPILO_SHA}_${COLUMNAR_EXT_SHA}_${HYDRA_EXT_SHA}`, e.g. `72fb97e_ff32dd9_243ba49`.
The `latest` tag is always tagged to the latest main branch.

## Spilo Version Update

Hydra Docker build overrides the following Spilo scripts to enable extra PostgreSQL extensions:

* [configure_spilo.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/configure_spilo.py)
* [spilo_commons.py](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/spilo_commons.py)
* [post_init.sh](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/post_init.sh)

To update to a newer Spilo version, you need to copy the above files from the specific Spilo version and add the corresponding bits to enable the Columnar/Hydra extensions.
Make sure you use `diff` to understand what needs to be added first before updating.
In the future, we may provide a script to simplify the update process.

## Community

* Website and blog: https://hydras.io
* Twitter: [@HydrasCo](https://twitter.com/HydrasCo)
* [Discord chat](https://discord.com/invite/zKpVxbXnNY) for quick questions.
* [GitHub Discussions](https://github.com/HydrasCo/hydra/discussions) for longer topics.
* [GitHub Issues](https://github.com/HydrasCo/hydra/issues) for bugs and missing features.
