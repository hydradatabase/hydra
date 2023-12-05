# Benchmarks

We use two test suites for benchmarking: [clickbench][] against the standard 100M row
(80GB) dataset, and a data warehouse benchmark suite similar to TPC-H, with a
10GB dataset. These suites are called `clickbench-100M` and `warehouse-10G`
respectively. The [benchmarking code is open source][benchmarks].

The high level results we track are below: the total query execution time of the
`SELECT` queries run in the benchmark (i.e. without setup or data load time)
of each suite. Every query is individually tracked in Bencher,
and can be explored in detail on our [Bencher project page][bencher project].

All benchmarks are query time, reported in milliseconds. Lower values are better.

## Release Benchmarking

Hydra benchmarks each of its releases. These tests are run on a `c6a.4xlarge`
instance (16 vCPU, 32 GiB RAM) with 500 GiB of GP2 storage.

### clickbench-100M

<a href="https://bencher.dev/perf/hydra-postgres?benchmarks_page=6&testbeds_page=1&branches_page=1&reports_page=1&tab=testbeds&branches=bf6a468c-7b8a-4917-b3d1-c66216eb95db&testbeds=245d9139-a1f3-484c-8449-1c9422800618&measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&benchmarks=c4efd5bb-f4c4-4b75-9137-f2a841c04cfe"><img src="https://api.bencher.dev/v0/projects/hydra-postgres/perf/img?measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&branches=bf6a468c-7b8a-4917-b3d1-c66216eb95db&testbeds=245d9139-a1f3-484c-8449-1c9422800618&benchmarks=c4efd5bb-f4c4-4b75-9137-f2a841c04cfe&title=hydra+-+clickbench-100M+-+release" title="hydra - clickbench-100M - release" alt="hydra - clickbench-100M - release for hydra-postgres - Bencher" /></a>

### warehouse-10G

<a href="https://bencher.dev/perf/hydra-postgres?benchmarks_page=9&testbeds_page=1&branches_page=1&reports_page=1&tab=benchmarks&branches=bf6a468c-7b8a-4917-b3d1-c66216eb95db&testbeds=245d9139-a1f3-484c-8449-1c9422800618&measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&benchmarks=4cda199f-0eb9-40cf-96b5-1706efb6724c"><img src="https://api.bencher.dev/v0/projects/hydra-postgres/perf/img?measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&branches=bf6a468c-7b8a-4917-b3d1-c66216eb95db&testbeds=245d9139-a1f3-484c-8449-1c9422800618&benchmarks=4cda199f-0eb9-40cf-96b5-1706efb6724c&title=hydra+-+clickbench-100M+-+release" title="hydra - clickbench-100M - release" alt="hydra - clickbench-100M - release for hydra-postgres - Bencher" /></a>

## Continuous Benchmarking

Hydra uses [Bencher][bencher home] to continuously track benchmarks after every
commit to `main`. This allows us to determine the impact of performance improvements
as well as track performance regressions.

These tests use the open source Docker image and run on
[GitHub's 4-core test runners][runners]. You can review
[the GitHub action][action] for more methodology details.

### Reading the graphs

Since many commits will not affect performance, it is normal for graphs to be
principally flat for many commits in a row. These graphs do not have historical data from
previous releases.


### clickbench-100M

<a href="https://bencher.dev/perf/hydra-postgres?benchmarks_page=6&testbeds_page=1&branches_page=1&reports_page=1&tab=benchmarks&branches=e6bcbe0c-210d-4ab1-8fe4-5d9498800980&testbeds=1d3283b3-3e52-4dd0-a018-fb90c9361a2e&measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&benchmarks=c4efd5bb-f4c4-4b75-9137-f2a841c04cfe"><img src="https://api.bencher.dev/v0/projects/hydra-postgres/perf/img?measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&branches=e6bcbe0c-210d-4ab1-8fe4-5d9498800980&testbeds=1d3283b3-3e52-4dd0-a018-fb90c9361a2e&benchmarks=c4efd5bb-f4c4-4b75-9137-f2a841c04cfe&title=hydra+-+clickbench-100M" title="hydra - clickbench-100M" alt="hydra - clickbench-100M for hydra-postgres - Bencher" /></a>

### warehouse-10G

<a href="https://bencher.dev/perf/hydra-postgres?benchmarks_page=9&testbeds_page=1&branches_page=1&reports_page=1&tab=benchmarks&branches=e6bcbe0c-210d-4ab1-8fe4-5d9498800980&testbeds=1d3283b3-3e52-4dd0-a018-fb90c9361a2e&measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&benchmarks=4cda199f-0eb9-40cf-96b5-1706efb6724c"><img src="https://api.bencher.dev/v0/projects/hydra-postgres/perf/img?measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&branches=e6bcbe0c-210d-4ab1-8fe4-5d9498800980&testbeds=1d3283b3-3e52-4dd0-a018-fb90c9361a2e&benchmarks=4cda199f-0eb9-40cf-96b5-1706efb6724c&title=hydra+-+warehouse-10G" title="hydra - warehouse-10G" alt="hydra - warehouse-10G for hydra-postgres - Bencher" /></a>

[bencher home]: https://bencher.dev/
[clickbench]: https://github.com/ClickHouse/ClickBench
[benchmarks]: https://github.com/hydradatabase/benchmarks
[runners]: https://docs.github.com/en/actions/using-github-hosted-runners/about-larger-runners
[action]: https://github.com/hydradatabase/hydra/blob/main/.github/workflows/benchmark.yaml
[bencher project]: https://bencher.dev/perf/hydra-postgres?benchmarks_page=6&testbeds_page=1&branches_page=1&reports_page=1&tab=benchmarks&branches=e6bcbe0c-210d-4ab1-8fe4-5d9498800980&testbeds=1d3283b3-3e52-4dd0-a018-fb90c9361a2e&measures=c20a9c30-e20a-45b7-bba5-4a6e940f951f&benchmarks=c4efd5bb-f4c4-4b75-9137-f2a841c04cfe
