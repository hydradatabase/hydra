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
