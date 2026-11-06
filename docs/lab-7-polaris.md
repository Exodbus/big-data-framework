# Lab 7 – Govern Iceberg tables with Polaris: answers

Polaris 1.8.0 deployed with Helm, Spark 4.1.1, `iceberg-spark-runtime-4.1_2.13:1.11.0`. The lab 6 `warehouse/` was
gone from the bucket, so I rebuilt `lab.silver.orders` (2829 rows) and `lab.silver.users` (50 rows) from
`silver/spark/*.parquet`.

## SparkSession with the Polaris catalog

### Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?

`lab`: Spark, which lists the directories under `warehouse/` on S3. `polaris`: the Polaris server, from its database.
Its list was empty even though `warehouse/silver/` exists in the bucket.

### Where is the credential used?

In the requests to Polaris. Spark exchanges it for an OAuth2 token. The bucket is read with the S3A keys.

## Register the Silver table in Polaris

Registered `v1.metadata.json`: 2829 rows, snapshot `7906241661102291133`.

### What did `register_table` write, in Polaris and in the bucket?

A pointer to `v1.metadata.json` in Polaris. Nothing in the bucket: 5 objects before and after.

### Why is it dangerous to keep writing to this table through the `lab` catalog?

The two catalogs have separate pointers. A commit through `lab` writes `v2.metadata.json`, but Polaris stays on v1.
The history splits in two, and the write skips Polaris access control.

## Create tables as `dev`

### Where did Polaris store the files for `silver.users`, and why?

In `warehouse/silver/users/`: base location + namespace + table. It's the same folder as `lab.silver.users`, which
now shares it (`v1.metadata.json` for `lab`, `00000-<uuid>.metadata.json` for `polaris`).

### What is the difference between "registered" and "created through Polaris" for the data files?

None for the files, Spark writes them both times. `orders` was written by the Hadoop catalog and Polaris only got a
pointer. For `users`, Polaris chose the location and recorded the creation.

## Schema change as `dev`

### Which metadata file records the change? Is it the same file that `register_table` pointed to?

`users/metadata/00001-50f7313f-...metadata.json`. No, `register_table` pointed to `orders/metadata/v1.metadata.json`.
No new snapshot.

### Which privilege does Polaris check for this statement?

The operation is `ADD_TABLE_SCHEMA` (named in the analyst error). `dev` gets it through `CATALOG_MANAGE_CONTENT`.

## Read as the analyst

Reads work. `INSERT` fails with `not authorized for op ADD_TABLE_SNAPSHOT`, `ALTER TABLE` with `ADD_TABLE_SCHEMA`.

### Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?

`TABLE_WRITE_DATA`. `catalog_reader` has `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`.

### Did the failed `INSERT` create a snapshot?

No. Still one snapshot, 2829 rows, 5 objects. Polaris refused the commit and Spark deleted the file it had written.

## What Polaris cannot enforce

No credential vending, so the analyst has the S3A keys. He can still write through `lab` or directly on S3.

## Commit the Iceberg table metadata

`aws s3 sync --delete` skipped `orders/metadata/version-hint.text` (same size, `3` locally). I copied it by hand.
