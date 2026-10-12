# Dataset generator

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

## Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
```

## Labs

Each lab has its own branch. The `main` branch contains all the labs merged.

| Lab | Branch | Content |
| --- | --- | --- |
| Lab 1 – Python project with uv | [`lab-1-uv`](https://github.com/Exodbus/big-data-framework/tree/lab-1-uv) | uv project, `dataset-users` and `dataset-orders` generators |
| Lab 2 – Object storage with S3 | [`lab-2-s3`](https://github.com/Exodbus/big-data-framework/tree/lab-2-s3) | Kubernetes Job `job-upload-bronze.yaml`, answers in [`docs/lab-2-s3.md`](docs/lab-2-s3.md) |
| Lab 3 – SQL analytics with DuckDB | [`lab-3-duckdb`](https://github.com/Exodbus/big-data-framework/tree/lab-3-duckdb) | `orders-report` command, answers in [`docs/lab-3-duckdb.md`](docs/lab-3-duckdb.md) |
