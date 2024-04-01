# CHANGELOG

## 1.1.2

* turn off column cache when building an index, vacuum, and analyze ([#250][])
* fix memory leak when executing vectorized quals ([#242][])
* do not release the chunk group state if there is data ([#245][])
* fix case sensitivity bug in columnar.alter_table_set_access_method ([#237][])
* fix a crash when chunk_group_row_limit is larger than 110000 ([#235][])
* add extensions: wrappers, pgsodium, and supabase_vault (PG >=14) ([#236][])

Thanks to @japinli for multiple bug fixes in this release!

[#250]: https://github.com/hydradatabase/hydra/pull/250
[#242]: https://github.com/hydradatabase/hydra/pull/242
[#245]: https://github.com/hydradatabase/hydra/pull/245
[#237]: https://github.com/hydradatabase/hydra/pull/237
[#235]: https://github.com/hydradatabase/hydra/pull/235
[#236]: https://github.com/hydradatabase/hydra/pull/236

## 1.1.1

* fix uncapped memory growth when importing data via logical replication ([#226][])
* fix handling of large data columns (e.g. text or json) causing overflow of 1GB
  chunk size ([#220][])
* restrict the maximum singular datum size to 256MB to ensure that the maximum
  datum size of 1GB is not exceeded in columnar metadata ([#222][])
* do not insert vectorization node into the plan if columnar scan isn't being used
  ([#228][])

[#226]: https://github.com/hydradatabase/hydra/pull/226
[#220]: https://github.com/hydradatabase/hydra/pull/220
[#222]: https://github.com/hydradatabase/hydra/pull/222
[#228]: https://github.com/hydradatabase/hydra/pull/228

## 1.1.0

* added support for upserts and other `ON CONFLICT` clauses for `INSERT`
  queries ([#174][])
* added a custom index-backed scan ([#205][]). This scan is disabled by default as it
  may adversely affect performance. To enable this scan, use:
  `SET columnar.enable_columnar_index_scan = true;`
* added Postgres 16 support ([#200][])
  * parquet_s3_fdw and multicorn-based FDWs are not yet supported in PG 16. These
    extensions are included only on PG 13-15 builds.
* update pg_ivm to 1.7.0 ([#200][])
* update pgsql-http to 1.6.0 ([#200][])

[#174]: https://github.com/hydradatabase/hydra/pull/174
[#205]: https://github.com/hydradatabase/hydra/pull/205
[#200]: https://github.com/hydradatabase/hydra/pull/200

## 1.0.2

This version of Hydra Columnar requires `ALTER EXTENSION columnar
UPDATE` after installation.

* fix incorrect results when using an aggregate `FILTER` by not vectorizing
  these aggregates ([#181][])
* fix possible bypass of table constraints by forcing constraints to be checked
  during multi-insert ([#182][])
* fix possible table corruption when running `columnar.vacuum`
  ([#190][])
* support chunk filtering for types that have a family comparator, namely
  `varchar` ([#184][])
* allow higher values (up to 10M) for `chunk_group_row_limit` and
  `stripe_row_limit` ([#186][])
* fix txid wraparound ([#190][])

[#181]: https://github.com/hydradatabase/hydra/pull/181
[#182]: https://github.com/hydradatabase/hydra/pull/182
[#184]: https://github.com/hydradatabase/hydra/pull/184
[#190]: https://github.com/hydradatabase/hydra/pull/190
[#186]: https://github.com/hydradatabase/hydra/pull/186
[#190]: https://github.com/hydradatabase/hydra/pull/190

## 1.0.1

* updated pgvector to 0.5.1, allowing [use of HNSW indexes][pgvector-HNSW].
  Users of pgvector should run `ALTER EXTENSION vector UPDATE` in any
  database where the extension is installed. ([#171][])
* stop resetting to default columnar on Hydra Cloud (spilo). This will allow
  the user to change the default. ([#173][])
* improve handling of stripe assignment to workers to reduce spinlock
  contention, which can cause a crash ([#170][])
* change default `qual_pushdown_correlation_threshold` from 0.9 to 0.4. This
  addresses a performance regression we observed in some clickbench queries.
  ([#159][])

[#173]: https://github.com/hydradatabase/hydra/pull/173
[#171]: https://github.com/hydradatabase/hydra/pull/171
[#170]: https://github.com/hydradatabase/hydra/pull/170
[#159]: https://github.com/hydradatabase/hydra/pull/159
[pgvector-HNSW]: https://github.com/pgvector/pgvector#hnsw

## 1.0.0

No changes since 1.0.0-rc2.

## 1.0.0-rc2

* bugfix: disable custom vectorization on non-column arguments ([#147][])

[#147]: https://github.com/hydradatabase/hydra/pull/147

## 1.0.0-rc

In addition to bug fixes, this release includes significant vectorization optimization for aggregates (e.g. COUNT and SUM), enabled new index types, and added `pg_hint_plan`.

* vectorization of direct aggregates - PG14+ only ([#143][])
* fix an inefficiency with vacuum if there is only one stripe ([#143][])
* enable `O3` compilation optimizations ([#143][])
* disable parallelism for `CREATE TABLE ... AS` queries ([#138][])
* allow gin, gist, spgist, and rum indexes on columnar tables ([#133][])
* add [pg_hint_plan][] extension ([#134][])
* extensions are now installed during the build process using pgxman ([#137][])
* bugfix: `columnar.alter_table_set_access_method` should correctly disallow conversion of heap tables that have foreign keys ([#136][])
* bugfix: crash when using `explain` when cache is enabled ([#125][])
* bugfix: cache could evict a chunk currently in use ([#142][])

[#143]: https://github.com/hydradatabase/hydra/pull/143
[#138]: https://github.com/hydradatabase/hydra/pull/138
[#133]: https://github.com/hydradatabase/hydra/pull/133
[#134]: https://github.com/hydradatabase/hydra/pull/134
[#136]: https://github.com/hydradatabase/hydra/pull/136
[#125]: https://github.com/hydradatabase/hydra/pull/125
[#137]: https://github.com/hydradatabase/hydra/pull/137
[#142]: https://github.com/hydradatabase/hydra/pull/137
[pg_hint_plan]: https://github.com/ossc-db/pg_hint_plan

## 1.0.0-beta

Columnar-optimized vacuuming allows columnar tables to be compacted after updates and deletes without full rewrite, which will also improve peformance after vacuum. Auto-vacuum support combines recent inserts into a single stripe. A new column cache makes JOIN queries more efficient. Vector similarity search and data types are now available via pgvector.

* default table access method is now columnar ([#107][])
* add [pgvector][] extension ([#106][])
* add vacuum_full UDF ([#93][])
* bugfix: vacuum udf could get into a look and overwrite stripes ([#92][])
* add columnar decompressed chunk cache ([#86][])
* bugfix: vacuum hanging indefinitely in some cases ([#80][])
* bugfix: release memory during long sequential scans ([#78][])
* add user function to allow for incremental vacuum and space reclamation ([#71][])
* add support for postgres 15 ([#75][])
* add columnar vacuum to combine latest stripe into 1 stripe ([#51][], [#74][])

## 0.3.0-alpha

This release adds update and delete support for columnar tables. Incremental materialized views are now available via pg_ivm.

* add incremental materialized views (pg_ivm extension) ([#67][])
* bugfix: WHERE clause with certain custom types ([4f5b508][])
* add counts of deleted rows, optimize if no data has been deleted ([c987c6e][])
* add columnar updates and deletes ([f33b0bd][], [4f939f4][], [f5e0cc1][], [7e15b4c][])

## 0.2.0-alpha

This release has huge gains for performance, bringing parallelization to columnar scans and vectorization of WHERE clauses.

* upgrade to spilo 2.1-p9 ([4e06ec5][])
* bugfix: memory leak when decompressing chunks ([15193be][])
* bugfix: vectorization with NULL values ([e35a429][])
* add vectorization of WHERE clauses when comparing to a const value ([a470460][])
* add parallel execution with JOIN clauses ([3370bf9][])
* add conversion from columnar to heap ([d0db6a2][])
* add vectorization of WHERE clauses ([0d41837][])
* add parallel execution ([f399474][])

## 0.1.0-alpha

The initial release focuses on adding several new FDWs and productionization work for our cloud service.

* add parquet_s3_fdw ([02d2253][])
* add multicorn2, s3csv_fdw, and gspreadsheet_fdw extensions ([1d7cb47][], [a22ecdc][])
* add mysql_fdw extension ([92ae91e][], [bc47d31][])
* add pgsql-http extension, but disable for spilo ([dae1e07][], [59b37be][])
* change to using the official `postgres` image as our base image; spilo is maintained for Hydra hosted service ([fec064b][])
* add tests for upgrading from previous image to current image ([#31][])
* move all functions into `columnar` schema
* add user function to convert from heap to columnar
* upgrade to spilo 2.1-p7 ([#26][])
* use docker-compose to have a stable storage ([#16][])
* add pg_cron extension ([#15][])
* build Hydra with bake files (`buildx bake`) ([#7][])
* add acceptance tests and CI
* add Hydra to spilo image
* change build scripts to allow columnar to build as standalone extension

[#7]: https://github.com/hydradatabase/hydra/pull/7
[#15]: https://github.com/hydradatabase/hydra/pull/15
[#16]: https://github.com/hydradatabase/hydra/pull/16
[#26]: https://github.com/hydradatabase/hydra/pull/26
[#31]: https://github.com/hydradatabase/hydra/pull/31
[#51]: https://github.com/hydradatabase/hydra/pull/51
[#67]: https://github.com/hydradatabase/hydra/pull/67
[#71]: https://github.com/hydradatabase/hydra/pull/71
[#74]: https://github.com/hydradatabase/hydra/pull/74
[#75]: https://github.com/hydradatabase/hydra/pull/75
[#78]: https://github.com/hydradatabase/hydra/pull/78
[#80]: https://github.com/hydradatabase/hydra/pull/80
[#86]: https://github.com/hydradatabase/hydra/pull/86
[#92]: https://github.com/hydradatabase/hydra/pull/92
[#93]: https://github.com/hydradatabase/hydra/pull/93
[#106]: https://github.com/hydradatabase/hydra/pull/106
[#107]: https://github.com/hydradatabase/hydra/pull/107
[02d2253]: https://github.com/hydradatabase/hydra/commit/02d2253
[0d41837]: https://github.com/hydradatabase/hydra/commit/0d41837
[15193be]: https://github.com/hydradatabase/hydra/commit/15193be
[1d7cb47]: https://github.com/hydradatabase/hydra/commit/1d7cb47
[3370bf9]: https://github.com/hydradatabase/hydra/commit/3370bf9
[4e06ec5]: https://github.com/hydradatabase/hydra/commit/4e06ec5
[4f5b508]: https://github.com/hydradatabase/hydra/commit/4f5b508
[4f939f4]: https://github.com/hydradatabase/hydra/commit/4f939f4
[59b37be]: https://github.com/hydradatabase/hydra/commit/59b37be
[7e15b4c]: https://github.com/hydradatabase/hydra/commit/7e15b4c
[92ae91e]: https://github.com/hydradatabase/hydra/commit/92ae91e
[a22ecdc]: https://github.com/hydradatabase/hydra/commit/a22ecdc
[a470460]: https://github.com/hydradatabase/hydra/commit/a470460
[bc47d31]: https://github.com/hydradatabase/hydra/commit/bc47d31
[c987c6e]: https://github.com/hydradatabase/hydra/commit/c987c6e
[d0db6a2]: https://github.com/hydradatabase/hydra/commit/d0db6a2
[dae1e07]: https://github.com/hydradatabase/hydra/commit/dae1e07
[e35a429]: https://github.com/hydradatabase/hydra/commit/e35a429
[f33b0bd]: https://github.com/hydradatabase/hydra/commit/f33b0bd
[f399474]: https://github.com/hydradatabase/hydra/commit/f399474
[f5e0cc1]: https://github.com/hydradatabase/hydra/commit/f5e0cc1
[fec064b]: https://github.com/hydradatabase/hydra/commit/fec064b
[pgvector]: https://github.com/pgvector/pgvector
