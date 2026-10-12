# Lab 3 – SQL analytics with DuckDB: answers

## S3 configuration

### The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?

The keys are stored in plain text in a file that remains on the disk, so anyone who gains access to it can steal them.
On Onyxia, this risk is limited because the keys are temporary and the service is personal.

### The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?

The Job must be given its own ServiceAccount with permissions limited to what is strictly necessary—for example, write
access only to `bronze/`—rather than using my credentials.

## Query the bronze layer

### Why does `approx_unique` return 40 and 3167?

`approx_unique` performs an estimate (HyperLogLog), not an actual count. It’s faster but not exact. For the exact value,
you need `count(DISTINCT ...)`.

### Which issue may occur with a large file whose first rows are not representative?

The data type may be guessed incorrectly. For example, a column initially treated as an integer may later contain text,
causing a read operation to fail.

## Parquet export

### Why is the `uuid` column barely compressed?

Each UUID is unique and random, so there are no duplicates to compress.

### Why is the compressed size of some columns larger than their uncompressed size?

These columns are already compact after Parquet encoding, so Snappy offers no benefit and just adds a small overhead.

## Hive partitioning

### Why is partitioning by `uuid` a bad idea?

We would end up with one file per order, resulting in a huge number of small files. Listing them becomes slow, and each
file requires additional queries.

### Which partition column would you choose for a dataset of orders growing every day?

The order date, by day or by month. Each day adds a new partition without affecting the old ones.

## CSV vs. Parquet at scale

### How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?

The file contains 3 row groups. Their maximum dates are in 2034 and 2048, which are before 2100: all 3 are ignored by
the filter.

### What would happen to the efficiency of the filter if the orders were shuffled?

Each row group would contain dates from the entire period, so the min/max range would no longer allow any to be
eliminated. Everything would be read.

### Which part of the difference is due to the network, which part to the parsing of the CSV file?

For CSV files, you need to download the entire file (network) and convert the text into data types (parsing). Parquet
downloads only the relevant columns and is already type-safe.