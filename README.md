![Hydra - the open source data warehouse](.images/hydraGH.svg)
<div align="center">
<em>The Modern EDW - Everyone Data Warehouse</em>

[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/hydradatabase.svg?style=social&label=Follow%20%40hydradatabase)](https://twitter.com/hydradatabase)
[![GitHub Repo stars](https://img.shields.io/github/stars/hydradatabase/hydra?style=social)](https://github.com/hydradatabase/hydra)

<h3>
    <a href="https://dashboard.hydra.so/signup">Free Cloud Trial</a> |
    <a href="https://docs.hydra.so">Docs</a> |
    <a href="https://discord.com/invite/SQrwnAxtDw">Discord</a> |
    <a href="https://hydra.so/">Website</a> 
</h3>

</div>

Hydra is a modern, open source replacement for enterprise data warehouses. It’s fast and feature-rich so devs can build better analytics, quicker.

Hydra implements an open source columnar engine to Postgres, driving 23X query performance and scalability over basic Postgres. When comparing to traditional warehouses, Hydra delivers 6000X more throughput and faster lookups.

## Contents
- Quick Start
- Features
- Community and Status
- License

## 🚀 Quick Start
### Run Hydra locally

The Hydra [Docker image](https://github.com/hydradatabase/hydra/pkgs/container/hydra) is a drop-in replacement for [postgres Docker image](https://hub.docker.com/_/postgres).

You can also try out Hydra locally using [docker-compose](https://docs.docker.com/compose/).

```
git clone https://github.com/hydradatabase/hydra && cd hydra
cp .env.example .env
docker compose up
psql postgres://postgres:hydra@127.0.0.1:5432
```

### Use Hydra Cloud

Hydra Cloud is the fastest and most reliable way to run Hydra. It is a cloud-based data warehouse that allows you to consolidate data from various sources into a single, unified system. It provides a user-friendly interface for automated data ingestion and transformation.

Hydra Cloud provides a scalable and secure cloud environment where automatic backups, resource scaling, high availability, point-in-time recovery, and more is available instantly with new databases.

Sign up for [Hydra Cloud](https://dashboard.hydras.io/signup) and get a free, managed database.

## 🎁 Features
🗃 hosted postgres database - [docs](https://docs.hydra.so/getting-started)
<br>📎 columnar store with updates and deletes- [docs](https://docs.hydra.so/concepts/using-hydra-columnar)
<br>🤹‍♀️ query parallelization
<br>🐎 vectorized execution of WHERE clauses
<br>📊 external tables - [docs](https://docs.hydra.so/concepts/using-hydra-external-tables)

## 🤝 Community and Status
[DEVELOPERS.md](https://github.com/hydradatabase/hydra/blob/main/DEVELOPERS.md) for contributing and building the image.
<br>[Discord](https://discord.com/invite/zKpVxbXnNY) discussion with the Community and Hydra team
<br>[GitHub Discussions](https://github.com/hydradatabase/hydra/discussions) for longer topics
<br>[GitHub Issues](https://github.com/hydradatabase/hydra/issues) for bugs and missing features
<br>[Blog](https://blog.hydra.so/) for latest announcements, tutorials, product updates
<br>[@hydradatabase](https://twitter.com/hydradatabase) for the tweets, memes, and social posts
<br>[Docs](https://docs.hydra.so/) for Hydra features and warehouse ops

<br>

![follow the repo](.images/follow.gif)

- [x]  Private Alpha: Limited to select design partners
- [x]  Public Beta: Talk with Hydra team to learn more
- [ ]  Hydra 1.0 Release- Generally Available (GA) production use with financially backed SLA

### Coming Soon
Watch [releases](https://github.com/hydradatabase/hydra/releases) of this repo to get notified of updates.
- [ ]  🧹 vacuum stripe optimizations and space reclamation
- [ ]  🏎️ vectorized execution of aggregate functions
- [ ]  🚅 use of SIMD in vectorized execution
- [ ]  ↔️ separation of compute and storage

## 📝 License
Hydra is only possible by building on the shoulders of giants.

The code in this repo is licensed under:

- [AGPL 3.0](https://github.com/hydradatabase/hydra/tree/main/columnar/LICENSE) for [Hydra Columnar](https://github.com/hydradatabase/hydra/tree/main/columnar)
- All other code is [Apache 2.0](https://github.com/hydradatabase/hydra/blob/main/LICENSE)

The docker image is built on the [Postgres docker image](https://hub.docker.com/_/postgres/), which contains a large number of open source projects, including:

- Postgres - [the Postgres license](https://www.postgresql.org/about/licence/)
- Debian or Alpine Linux image, depending on the image used
- Hydra includes the following additional software in the image:
    - multicorn - BSD license
    - mysql_fdw - MIT-style license
    - parquet_s3_fdw - MIT-style license
    - pgsql-http - MIT license

As for any pre-built image usage, it is the image user's responsibility to ensure that any use of this image complies with any relevant licenses for all software contained within.
