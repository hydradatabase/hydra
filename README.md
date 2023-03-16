![Hydra - the open source data warehouse](.images/header.png)

[Get a 14-day Free Trial](https://hydra.so/) - [Documentation](https://docs.hydra.so/) - [Demo](https://www.youtube.com/watch?v=DD1oD1LWNOo) - [Website](https://hydra.so/)

The open source Snowflake alternative. OLAP Postgres.

[Hydra](https://hydra.so/) is an open source data warehouse built on Postgres. It’s easy to use and designed for OLAP and HTAP workloads. Hydra serves analytical reporting with parallelized query execution and vectorization on columnar storage. Operational work and high-throughput transactions write to standard Postgres heap tables. All Postgres extensions, tools, and connectors work with Hydra.

Eliminate data silos today. Solve hard problems fast.

* [x] 🗃 hosted postgres database - [docs](https://docs.hydra.so/getting-started)
* [x] 📎 columnar store - [docs](https://docs.hydra.so/concepts/using-hydra-columnar)
* [x] 📊 external tables - [docs](https://docs.hydra.so/concepts/using-hydra-external-tables)
* [x] 📅 postgres scheduler - [docs](https://docs.hydra.so/cloud-warehouse-operations/using-hydra-scheduler)
* [x] 🤹‍♀️ query parallelization
* [x] 🐎 vectorized execution of WHERE clauses
* [x] 📝 updates and deletes for columnar store - [docs](https://docs.hydra.so/concepts/updates-and-deletes)
* [ ] 🧹 vacuum stripe optimizations and space reclamation
* [ ] 🏎️ vectorized execution of aggregate functions
* [ ] 🚅 use of SIMD in vectorized execution
* [ ] ↔️ separation of compute and storage

![Where does Hydra fit](.images/hydra-db.png)

## ⏩ Quick Start

The Hydra [Docker image](https://github.com/hydradatabase/hydra/pkgs/container/hydra) is a drop-in replacement for [postgres Docker image](https://hub.docker.com/_/postgres).

You can also try out Hydra locally using [docker-compose](https://docs.docker.com/compose/).

```
git clone https://github.com/hydradatabase/hydra && cd hydra
cp .env.example .env
docker compose up
psql postgres://postgres:hydra@127.0.0.1:5432
```

### Or

Managed in the [cloud](https://hydra.so/).

## 📄 Documentation

You can find our documentation [here](https://docs.hydra.so/getting-started/setup-guide).

## 👩🏾‍🤝‍👨🏻 Community

- [Discord chat](https://discord.com/invite/zKpVxbXnNY) for quick questions
- [GitHub Discussions](https://github.com/hydradatabase/hydra/discussions) for longer topics
- [GitHub Issues](https://github.com/hydradatabase/hydra/issues) for bugs and missing features
- [@hydradatabase](https://twitter.com/hydradatabase) on Twitter

## ✅ Status

- [x] Early Access: Closed, private testing
- [ ] Open Alpha: Open for everyone
- [ ] Open Beta: Hydra can handle most non-enterprise use
- [ ] Production: Enterprise ready

We are currently in Early Access. Watch [releases](https://github.com/hydradatabase/hydra/releases) of this repo to get notified of updates.

![follow the repo](.images/follow.gif)

## 🛠 Developing Hydra
Please see [DEVELOPERS.md](DEVELOPERS.md) for information on contributing to Hydra and building the image.

## 📑 License and Acknowledgments
Hydra is only possible by building on the shoulders of giants.

The code in this repo is licensed under:
* [AGPL 3.0](https://github.com/hydradatabase/hydra/tree/main/columnar/LICENSE) for [Hydra Columnar](https://github.com/hydradatabase/hydra/tree/main/columnar)
* All other code is [Apache 2.0](LICENSE)

The docker image is built on the [Postgres docker image](https://hub.docker.com/_/postgres/), which contains a large number of open source projects, including:
* Postgres - [the Postgres license](https://www.postgresql.org/about/licence/)
* Debian or Alpine Linux image, depending on the image used
* Hydra includes the following additional software in the image:
  * multicorn - BSD license
  * mysql_fdw - MIT-style license
  * parquet_s3_fdw - MIT-style license
  * pgsql-http - MIT license

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this
image complies with any relevant licenses for all software contained within.
