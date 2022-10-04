![Hydra - the open source data warehouse](.images/header.png)

[Request Access](https://hydras.io/#early-access) - [Documentation](https://docs.hydras.io/getting-started/readme) - [Demo](https://www.youtube.com/watch?v=DD1oD1LWNOo) - [Website](https://hydras.io/)

Data driven decisions at every stage - [Hydra](https://hydras.io/) is your open source data warehouse built on Postgres.

Postgres is the foundation of the Modern Data Stack. Hydra's mission is to make data-driven decisions accessible at every stage. We built Hydra on an open Postgres foundation that's easy to use. Everything that works with Postgres works with Hydra.

* [x] 🗃 hosted postgres database - [docs](https://docs.hydras.io/getting-started/hydra-with-postgres)
* [x] 📎 append-only columnar store - [docs](https://docs.hydras.io/features/columnar)
* [x] 📊 external tables - [docs](https://docs.hydras.io/features/external_tables)
* [x] 📅 postgres scheduler - [docs](https://docs.hydras.io/features/scheduler)
* [ ] ☠️ vectorized execution
* [ ] ⚙️ updates and deletes for columnar store
* [ ] ↔️ separation of compute and storage

![Where does Hydra fit](.images/hydra-db.png)

## ⏩ Quick Start

Try out Hydra locally using [docker-compose](https://docs.docker.com/compose/).

```
cp .env.example .env
docker compose up
psql postgres://postgres:hydra@127.0.0.1:5432
```

### Or

Managed in the [cloud](https://hydras.io/#early-access).

## 📄 Documentation

You can find our documentation [here](https://docs.hydras.io/getting-started/readme).

## 👩🏾‍🤝‍👨🏻 Community

- [Discord chat](https://discord.com/invite/zKpVxbXnNY) for quick questions
- [GitHub Discussions](https://github.com/HydrasDB/hydra/discussions) for longer topics
- [GitHub Issues](https://github.com/HydrasDB/hydra/issues) for bugs and missing features
- [@HydrasDB](https://twitter.com/hydrasdb) on Twitter

## ✅ Status

- [x] Early Access: Closed, private testing
- [ ] Open Alpha: Open for everyone
- [ ] Open Beta: Hydra can handle most non-enterprise use
- [ ] Production: Enterprise ready

We are currently in Early Access. Watch [releases](https://github.com/HydrasDB/hydra/releases) of this repo to get notified of updates.

![follow the repo](.images/follow.gif)

## 🛠 Developing Hydra
Please see [DEVELOPERS.md](DEVELOPERS.md) for information on contributing to Hydra and building the image.

## 📑 License and Acknowledgments
Hydra is only possible by building on the shoulders of giants.

The code in this repo is licensed under the [Apache 2.0 license](LICENSE). Pre-built images are
subject to additional licenses as follows:

* [Hydra columnar engine](https://github.com/HydrasDB/citus) - AGPL 3.0
* [Spilo](https://github.com/zalando/spilo) - Apache 2.0
* The underlying Spilo image contains a large number of open source projects, including:
  * Postgres - [the Postgres license](https://www.postgresql.org/about/licence/)
  * [WAL-G](https://github.com/wal-g/wal-g) - Apache 2.0
  * [Ubuntu's docker image](https://hub.docker.com/_/ubuntu/) - various copyleft licenses (MIT, GPL, Apache, etc)

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this
image complies with any relevant licenses for all software contained within.
