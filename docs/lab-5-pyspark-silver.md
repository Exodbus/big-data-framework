# Lab 5 – Bronze to silver with Spark DataFrames: answers

Spark 4.1.1 (Scala 2.13), `iceberg-spark-runtime-4.1_2.13:1.11.0`, service with 4 CPU and 4000 Mi. My dataset has 2829
orders and 50 users.

Changes from the lab page: `spark.stop()` before building the session (the shell already has one, and `getOrCreate()`
would skip the Iceberg extension), and `--endpoint-url "https://$AWS_S3_ENDPOINT"` instead of `--profile default`.

## SparkSession with S3 access

### The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?

The job is lost. In `local[*]` mode the driver and executors live in the pod, so the restart kills the JVM. Without a
restart, the keys are read once when the session starts, so S3 calls fail (`403`) once they expire.

### Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?

It doesn't send the session token, and Onyxia's STS keys are invalid without it.

## Silver layer

2829 orders, 2832 with the 3 duplicates, 2829 after deduplication. The SQL version matches, but its view is built on
`orders_typed` (no duplicates), so the two cells don't start from the same input.

### Explain the output of the code that was just run.

Users per state, and how many have no city. The 6 missing cities are all military addresses: `AE` (3/3) and `AP` (2/2),
plus one outside the top 10. They have no city, so `NULL` is correct here.

### Compare the DataFrame and SQL cells above. Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?

SQL for exploration, it's quicker to write and read. DataFrame API for a shared model: functions, parameters, unit
tests. Same Catalyst plan in both cases.

### `regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same behavior silently produce a wrong result if left unhandled?

`state` and `zip_code`. 0 empty strings today, but an unexpected address format would give `""`, which shows up as a
group in `GROUP BY` and passes a `not_null` check.

## Write the silver layer as Parquet

### List the objects written under `silver/users.parquet/`. How many part-files are there, and why?

One per table, because each DataFrame has 1 partition. Users: a 7 KB file, not splittable with `multiLine=True`.
Orders: the window shuffle is planned on 200 partitions and AQE merges them into 1 (less than 1 MB of data).

### `coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?

Parallelism: one task does the whole write, and since `coalesce` doesn't shuffle, the steps before it run on one
partition too.

## Write the silver layer as Iceberg tables

### Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?

Data: the `part-*.parquet` file in the Spark directory, the Parquet file in `data/` for Iceberg. Description: nothing on
the Spark side (`_SUCCESS` only marks the end of the write). Iceberg has `metadata/`: `v1.metadata.json` (schema,
snapshots), the manifest list `snap-*.avro`, the manifest `*-m0.avro` (data files and stats) and `version-hint.text`
(current version).

### A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?

Parquet: missing files, an empty directory or a mix of old and new files. Iceberg: the reader keeps the snapshot it
started with, since the new version is only visible after the commit.

### With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?

Both write a `v2.metadata.json` and one overwrites the other, so its data is lost without an error. S3 has no atomic
rename to prevent it. A catalog service (Polaris, Nessie) commits with compare-and-swap and rejects the second job,
which has to retry.

## Compare with the dbt lab

My dbt models are views, so I ran `dbt build` again (6.05 s, 29 pass, 1 warning) and exported them with DuckDB
`COPY` to `silver/dbt/`.

| | dbt | Spark |
| --- | --- | --- |
| orders | 2829 | 2829 |
| sum of quantity | 8615 | 8615 |
| users | 50 | 50 |

### What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?

`order_id`/`user_id`: `uuid` with dbt, `varchar` with Spark (we cast them to string). Consumers get 36-character strings
instead of 16 bytes, with no format check, and need a cast to join with a `uuid` column. `ordered_at`: `timestamptz`
with dbt, `timestamp` with Spark (written as `INT96`, the time zone is lost). dbt also has `username_normalized` and
`birthdate_is_valid`.

### Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?

The single file is easier to read. The directory scales, since each task writes its own file in parallel.

### The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?

A few `assert` on counts and a `groupBy` for duplicates. For the same guarantees, one check per test:

- `unique`: `df.groupBy(col).count().filter("count > 1").count() == 0`
- `not_null`: `df.filter(F.col(col).isNull()).count() == 0`
- `accepted_values`: `df.filter(~F.col(col).isin(values)).count() == 0`
- `relationships`: `orders.join(users, "user_id", "left_anti").count() == 0`

### `ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?

The Python variables, and the order in which cells are run. Spark knows the lineage of each DataFrame, but there is no
graph between models.

### Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?

dbt only creates views and runs tests (6.05 s). Spark also includes the JVM start, the Iceberg download, S3 reads and
the Parquet and Iceberg writes. On 331 KB, fixed costs are most of the time.

### For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?

10 GB: dbt with DuckDB, one machine is enough. 10 TB: Spark. Analysts who only know SQL: dbt.

## Exercises

### 1. `birthdate_is_valid`

```python
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_valid_df = (
    stg_users
    .join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate") <= F.to_date("first_order_at"), F.lit(True)),
    )
    .drop("first_order_at")
)
```

```sql
SELECT u.*,
       coalesce(u.birthdate <= to_date(f.first_order_at), true) AS birthdate_is_valid
FROM stg_users u
LEFT JOIN (
    SELECT user_id, min(ordered_at) AS first_order_at
    FROM stg_orders
    GROUP BY user_id
) f ON u.user_id = f.user_id
```

Both versions agree (`exceptAll` gives `0 0`): 45 valid, 5 invalid (born 2022 to 2026, orders in 2020). Same split as
dbt, but dbt also sets the invalid birthdates to `NULL`, here they're only flagged.

### 2. Python UDF and `pandas_udf`

Measured with `timeit.repeat` (5 runs) and the `noop` sink, `users_bronze` cached. Same zip codes for the three.

| Version | min | mean |
| --- | --- | --- |
| `regexp_extract` | 0.054 s | 0.059 s |
| Python UDF | 0.082 s | 0.108 s |
| `pandas_udf` | 0.100 s | 0.115 s |

`regexp_extract` runs in the JVM. The Python UDF sends each row to Python and back. The `pandas_udf` is last because on
50 rows the Arrow setup costs more than vectorization saves.

### 3. Caching the bronze DataFrames

| `stg_orders.count()` | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| without cache | 0.605 s | 0.574 s | 0.455 s |
| with cache | 0.329 s | 0.299 s | 0.332 s |

About twice as fast, the CSV is no longer read from S3. The plan shows `InMemoryTableScan`. In memory: 148 KB for orders
(331 KB of CSV, columnar and compressed), 9.4 KB for users.

### 4. Append and time travel

Iceberg: a second snapshot (`append`), 2830 rows now, 2829 with `VERSION AS OF 6881747351792077266`, and a new data file
of 1 row next to the old one.

Parquet: 2830 rows too, two new part-files (one empty), but no previous version to go back to.

Cleanup: `CALL lab.system.rollback_to_snapshot('silver.orders', 6881747351792077266)` for Iceberg (back to 2829, the
append stays in the history), and an overwrite of the Parquet directory with `stg_orders`.
