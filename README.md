# Hydra

[Hydra](https://hydras.io/) is the open-source Snowflake alternative for scaling startups.
Hydra is 100% Postgres - use the full Postgres ecosystem to set up your "source of truth" reporting.

Here is a list of high-leveled features:

* [x] columnar store
* [ ] vectorized execution

## Quickstart

The following is a quick local setup. For production use, sign up for [our cloud offering](https://hydras.io/#early-access).

```console
docker run --rm -d -p 127.0.0.1:5432:5432 ghcr.io/HydrasCo/hydra
psql postgres://postgres:hydra@127.0.0.1:5432
```

You can find detailed usage [here](https://docs.hydras.io/features/columnar).

## Community

* Website: [hydras.io](https://hydras.io)
* Twitter: [@HydrasCo](https://twitter.com/HydrasCo)
* [Discord chat](https://discord.com/invite/zKpVxbXnNY) for quick questions.
* [GitHub Discussions](https://github.com/HydrasCo/hydra/discussions) for longer topics.
* [GitHub Issues](https://github.com/HydrasCo/hydra/issues) for bugs and missing features.

## Overview

The Hydra image is built on top of [Spilo](https://github.com/zalando/spilo). Spilo is a containerization of
Postgres designed to be run in a production environment. The Hydra build adds extensions and modifies certain
configuration files to enable those extensions. 

## Building the image

Please see [DEVELOPERS.md](DEVELOPERS.md) for information on building the image.