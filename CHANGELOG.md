# CHANGELOG

## Future release

* add `pgvector` ([#106][])
* default table access method is now columnar ([#107][])
* add vacuum_full UDF ([#93][])
* bugfix: vacuum udf could get into a look and overwrite stripes ([#92][])
* add columnar decompressed chunk cache ([#86][])
* bugfix: vacuum hanging indefinitely in some cases ([#80][])
* bugfix: release memory during long sequential scans ([#78][])
* add user function to allow for incremental vacuum and space reclamation ([#71][])
* add support for postgres 15 ([#75][])
* add columnar vacuum to combine latest stripe into 1 stripe ([#51][], [#74][])

## 0.3.0-alpha

* add incremental materialized views (pg_ivm extension) ([#67][])
* bugfix: WHERE clause with certain custom types ([4f5b508][])
* add counts of deleted rows, optimize if no data has been deleted ([c987c6e][])
* add columnar updates and deletes ([f33b0bd][], [4f939f4][], [f5e0cc1][], [7e15b4c][])

## 0.2.0-alpha

* upgrade to spilo 2.1-p9 ([4e06ec5][])
* bugfix: memory leak when decompressing chunks ([15193be][])
* bugfix: vectorization with NULL values ([e35a429][])
* add vectorization of WHERE clauses when comparing to a const value ([a470460][])
* add parallel execution with JOIN clauses ([3370bf9][])
* add conversion from columnar to heap ([d0db6a2][])
* add vectorization of WHERE clauses ([0d41837][])
* add parallel execution ([f399474][])

## 0.1.0-alpha

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
