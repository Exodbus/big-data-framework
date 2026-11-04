# Lab 6 – Explore Iceberg tables with Spark: answers

Spark 4.1.1, `iceberg-spark-runtime-4.1_2.13:1.11.0`, `hadoop` catalog `lab`. The lab 5 tables already had a history,
so I dropped them (`DROP TABLE ... PURGE`) and rebuilt `lab.silver.orders` from `silver/spark/orders.parquet`:
2829 orders, quantity 8615.

## Inspect the files

### Which files contain the actual rows?

`data/00000-8-...parquet` (84 KB, 2829 rows).

### Which files describe the table?

The ones in `metadata/`: `v1.metadata.json`, the manifest list `snap-*.avro`, the manifest `*-m0.avro` and
`version-hint.text`.

### Why does an Iceberg table need more than the Parquet files?

Parquet files alone don't say which files belong to the table or which version is current. The metadata does, which
makes atomic commits, time travel and schema evolution possible.

## Explore Iceberg metadata

### How many snapshots does the table currently have?

One (`overwrite`, no parent).

### How many data files belong to the current table?

One, 2829 rows.

### What information does the `snapshots` table contain that the Parquet files do not?

Commit time, operation, parent snapshot, manifest list, and a summary of added and total records and files.

## Create a second snapshot

### How many snapshots are there now?

Two: `overwrite` then `append`. 2832 rows.

### Did the original Parquet files disappear?

No. The 84 KB file is untouched; the append added a 3-row file (1.9 KB).

### Why does Iceberg create a new snapshot instead of simply replacing the table?

Files are never modified. A new snapshot only adds files, keeps readers on their version and keeps the old one for
time travel.

## Time travel

2829 rows with `VERSION AS OF` the first snapshot, 2832 now.

### What does time travel read?

The metadata of the old snapshot and only the data files it lists.

### Why can Iceberg access an older version without making a complete copy of the table?

Snapshots share data files. The second one reuses the 2829-row file.

### What would happen if the old snapshot's metadata and data files were physically deleted?

The query on that version fails. Here the 84 KB file is also used by the current snapshot, so the current table breaks
too. `expire_snapshots` only deletes files no snapshot uses anymore.

## Schema evolution

After `ADD COLUMN source` and the `INSERT`: 2832 rows with `source = NULL`, 1 with `spark`. The `ALTER` wrote
`v3.metadata.json` without a snapshot. The `INSERT ... LIMIT 1` copied an existing row, so one `order_id` is now
duplicated.

### Did the existing Parquet files have to be rewritten?

No. Old files don't have `source`, Iceberg reads it as `NULL`.

### Where is the new schema recorded?

In the table metadata (`v3.metadata.json`, `current-schema-id` = 1).

### Why is schema evolution a table-level operation rather than simply changing a Parquet file?

A Parquet schema can't change without rewriting the file. Iceberg tracks columns by ID at table level, so files with
different schemas still form one table.

## Compare Iceberg with Parquet

### Which one is simply a collection of Parquet files?

`silver/spark/orders.parquet/`.

### Which one has table metadata and snapshots?

`warehouse/silver/orders/`.

### How would you safely replace a Parquet directory while another query is reading it?

Write to a new directory, switch readers to it, delete the old one later.

### How does Iceberg make the table state visible atomically?

New files are written first, then the commit switches the table to the new metadata file in one step.

### Why is a catalog needed once several engines need to share the same Iceberg table?

To give every engine the same current version and reject concurrent commits. The `hadoop` catalog can't do that on S3
(no atomic rename).

## Exercises

```python
stg_users.writeTo("lab.silver.users").createOrReplace()

(spark.table("lab.silver.users").limit(3)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat("username", F.lit("_b2")))
    .writeTo("lab.silver.users").append())

spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
(spark.table("lab.silver.users").limit(2)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat("username", F.lit("_b3")))
    .withColumn("country", F.lit("US"))
    .writeTo("lab.silver.users").append())
```

1. `lab.silver.users`: 50 users.
2. After the append: 2 snapshots, 53 users.
3. Time travel: 50 before, 53 after.
4. `country`: 53 `NULL`, 2 `US`, 55 users in 3 files. Old files not rewritten.
5. Same layout for both tables: one data file per write, one metadata version per change. 3 snapshots, 3 data files
   and 4 metadata versions each. Small appends make small files, which would need compaction on a real table.
6. Parquet is a file format. Iceberg is a table format: metadata over Parquet files (schema, file list, snapshots). The
   catalog stores the pointer to each table's current metadata and updates it atomically.
