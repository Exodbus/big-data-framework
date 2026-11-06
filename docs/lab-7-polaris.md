# Lab 7 – Govern Iceberg tables with Polaris: answers

Polaris 1.8.0 deployed with Helm (PostgreSQL persistence). Spark 4.1.1, `iceberg-spark-runtime-4.1_2.13:1.11.0`. The
`warehouse/` of lab 6 was gone from the bucket. So I rebuilt `lab.silver.orders` (2829 rows) and `lab.silver.users`
(50 rows) from `silver/spark/*.parquet` before starting.

## SparkSession with the Polaris catalog

`SHOW NAMESPACES IN lab` returns `silver`. `SHOW NAMESPACES IN polaris` returns nothing.

### Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?

`lab`: Spark itself. The Hadoop catalog lists the directories under `s3a://user-c-affile-ece/warehouse` and finds
`silver`. `polaris`: the Polaris server, through the REST API. It answers from its PostgreSQL database. Its list is
empty, but `warehouse/silver/` exists in the bucket. So the answer doesn't come from the bucket.

### Where is the credential used?

Only in the requests to Polaris. The Iceberg REST client exchanges `clientId:secret` for an OAuth2 token on
`/api/catalog/v1/oauth/tokens`. Then it sends the token with every call. The bucket is read with the S3A keys of the
session. There is no credential vending (`stsUnavailable: true`).

## Register the Silver table in Polaris

`version-hint.text` = 1, so I registered `v1.metadata.json`. Result: snapshot `7906241661102291133`, 2829 rows, 1 data
file.

### What did `register_table` write, in Polaris and in the bucket?

In Polaris: a table entity that points to `warehouse/silver/orders/metadata/v1.metadata.json`. In the bucket: nothing.
There are 5 objects under `warehouse/silver/orders/` before and after.

### Why is it dangerous to keep writing to this table through the `lab` catalog?

Each catalog keeps its own pointer to the current metadata. For `lab` it's `version-hint.text`, for `polaris` it's the
Polaris database. A commit through `lab` would write `v2.metadata.json`. Polaris would stay on v1 and miss the new
data. The next commit through Polaris would start again from v1. The table would then have two diverging histories.
An `expire_snapshots` on one side could also delete files the other side still uses. And writes through `lab` skip
Polaris access control.

## Create tables as `dev`

### Where did Polaris store the files for `silver.users`, and why?

In `warehouse/silver/users/`. The path is the default base location of the catalog
(`s3://user-c-affile-ece/warehouse/`) + namespace + table name. The Hadoop catalog uses the same layout. So the table landed in the directory of
`lab.silver.users`. This folder now holds two independent tables. `lab` has `v1.metadata.json` + `version-hint.text`.
`polaris` has `00000-<uuid>.metadata.json`. Each one has its own data file and snapshot.

### What is the difference between "registered" and "created through Polaris" for the data files?

In both cases Spark writes the Parquet files with its S3A keys. Polaris never touches data. For `orders`, the files and
`v1.metadata.json` already existed. The Hadoop catalog wrote them, and Polaris only adopted a pointer. For `users`,
Polaris chose the location. The creation was a commit through its REST API. So the table history starts in Polaris.

## Schema change as `dev`

`ADD COLUMN country` wrote a new metadata file. In `metadata_log_entries`, both entries have the same
`latest_snapshot_id` (`5945523599685035420`).

### Which metadata file records the change? Is it the same file that `register_table` pointed to?

`users/metadata/00001-50f7313f-ddd2-46f8-b950-1512b0bf8399.metadata.json`. It replaces `00000-e597fe28-...`. It's not
the file from `register_table`: that one was `orders/metadata/v1.metadata.json`, another table. No snapshot and no
data file were added.

### Which privilege does Polaris check for this statement?

Polaris splits a commit into fine-grained operations. This one is `ADD_TABLE_SCHEMA`, the operation named in the
analyst error below. `dev` is allowed through `CATALOG_MANAGE_CONTENT`.

## Read as the analyst

Reads work. `SHOW TABLES` lists `orders` and `users`, and the count by product sums to 2829. Both writes fail:

- `INSERT`: `Forbidden: Principal 'analyst' ... is not authorized for op ADD_TABLE_SNAPSHOT`
- `ALTER TABLE ... ADD COLUMN phone`: same error for `ADD_TABLE_SCHEMA`

### Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?

`TABLE_WRITE_DATA` is missing. `catalog_reader` only grants `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and
`CATALOG_READ_PROPERTIES`. The analyst can list tables and read metadata and data. He can't commit.

### Did the failed `INSERT` create a snapshot?

No. As `dev`, `silver.orders` still has one snapshot (`7906241661102291133`, `overwrite`) and 2829 rows. The bucket
still has 5 objects. Spark wrote a Parquet file first. Then Polaris refused the REST commit. The write was aborted and
the file deleted. Polaris checks at commit time, not before the write.

## What Polaris cannot enforce

There is no credential vending. So every Spark session holds the S3A keys of the whole bucket. The analyst could still
read or write `warehouse/` directly, or through the `lab` catalog. Polaris would never see it. It only controls catalog
operations.

## Commit the Iceberg table metadata

I used `aws s3 sync --delete` to drop the lab 6 metadata that no longer exists in the bucket. The sync skipped
`orders/metadata/version-hint.text`: same 1-byte size, still `3` locally. I copied it by hand.
