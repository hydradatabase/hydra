# Hydra

[Hydra](https://hydras.io/) is the open-source Snowflake alternative for scaling startups.
Hydra is 100% Postgres - use the full Postgres ecosystem to set up your "source of truth" reporting.

Here is a list of high-leveled features Hydra offers:

* [x] append-only columnar store - [docs](https://docs.hydras.io/features/columnar)
* [x] managed service - [sign up for early access](https://hydras.io/#early-access)
* [x] TLS publicly-signed certificates - [docs](https://docs.hydras.io/features/tls)
* [ ] updates and deletes for columnar store
* [ ] vectorized execution
* [ ] separation of compute and storage

## Quickstart

The following is a quick local setup. For production use, sign up for [our cloud offering](https://hydras.io/#early-access).

```console
cp .env.example .env
docker compose build --pull
docker compose up
psql postgres://postgres:hydra@127.0.0.1:5432
```

You can find detailed usage [here](https://docs.hydras.io/features/columnar).

## Community

* Website: [hydras.io](https://hydras.io)
* [Documentation](https://docs.hydras.io/)
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

## License and Acknowledgments

Hydra is only possible by building on the shoulders of giants.

The code in this repo is licensed under the [Apache 2.0 license](LICENSE). Pre-built images are
subject to additional licenses as follows:

* [Hydra columnar engine](https://github.com/HydrasCo/citus) - AGPL 3.0
* [Spilo](https://github.com/zalando/spilo) - Apache 2.0
* The underlying Spilo image contains a large number of open source projects, including:
  * Postgres - [the Postgres license](https://www.postgresql.org/about/licence/)
  * [WAL-G](https://github.com/wal-g/wal-g) - Apache 2.0
  * [Ubuntu's docker image](https://hub.docker.com/_/ubuntu/) - various copyleft licenses (MIT, GPL, Apache, etc)

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this
image complies with any relevant licenses for all software contained within.
