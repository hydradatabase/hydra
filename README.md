# spilo-build

This repo contains build scripts to build the [Hydras Spilo image](https://github.com/HydrasCo/spilo) with the [Columnar](https://github.com/HydrasCo/citus) & [Hydra](https://github.com/HydrasCo/Hydras) Postgres extensions.

## Docker Build

You can build the Spilo image with the following command:

```console
TAG=1234 make docker_build
```

This make task clones the corresponding repos, builds the Postgres extensions, and copy the extensions to the final Spilo image.

## Docker Push

You can push the Spilo image with the following command:

```console
TAG=1234 make docker_push
```

## GitHub Actions

GitHub Actions is configured to publish to the Spilo ECR repo on master branch, after a successful `docker build`.
The repo URL is `011789831835.dkr.ecr.us-east-1.amazonaws.com/spilo`.
Image tag is in the format of `${SPILO_SHA}_${COLUMNAR_SHA}_${HYDRAS_SHA}`, e.g. `72fb97e_ff32dd9_243ba49`.
The `latest` tag is also updated.
