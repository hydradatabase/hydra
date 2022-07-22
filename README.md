# spilo-build

This repo contains build scripts to build the [Hydras Spilo image](https://github.com/HydrasCo/spilo) with the [Columnar](https://github.com/HydrasCo/citus) & [Hydra](https://github.com/HydrasCo/Hydras) Postgres extensions.

## Build

You can build the Spilo image with the following command:

```console
TAG=1234 make
```

This make task clones the corresponding repos, builds the Postgres extensions, and copy the extensions to the final Spilo image.
