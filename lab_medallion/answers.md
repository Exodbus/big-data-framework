# Lab dbt - answers

## Bronze layer

1. The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?

The raw data is still there and unchanged. We fix the SQL and run `dbt build` again, and the silver layer is rebuilt correctly from the original data. Nothing is lost.

2. The source reads the whole file on every query. Which format and layout would you choose if the orders were ingested every hour for years?

Parquet files, partitioned by date (for example `year=/month=/day=/hour=`). Parquet is columnar, compressed and typed, so queries read less data. With partitions, a query on one period only reads the folders it needs. In dbt, we would also use an incremental model to process only the new data.

## Silver layer

3. The silver models are views. What happens when a view is queried, and when is it preferable to a table?

A view only stores the SQL query. Each time we query it, DuckDB reads the CSV on S3 and runs the whole transformation again. A view is better when the data is small, rarely queried, or when we always want the latest data.

4. UUID values are stored in 16 bytes. How many bytes does the VARCHAR representation use?

36 bytes (32 hexadecimal characters and 4 dashes), so more than twice as much. The UUID type is also faster to compare.

## Data tests

5. Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?

- Age dashboard: replace the invalid birthdates with NULL and show these customers as "unknown age". Wrong birthdates would give wrong ages.
- Sales per region: keep all the orders and only show a warning. The birthdate is not used here, and removing the rows would lose real sales.

6. The addresses associate states with random zip codes, KY 01352 for example. Which test would detect it, and which reference data would it require?

A singular test that checks that the first 3 digits of the zip code belong to the state. It needs a reference table "ZIP3 prefix -> state", from an official source such as the USPS. For example, Kentucky uses 400 to 427, so 013 is wrong for KY.

## Exercise 4: invalid ZIP codes

Where should this reference data come from?

From an official source, like the USPS or the US Census Bureau. We should not invent it, or the test would give false errors.

Should it be stored as a dbt seed?

Yes, because it is small (about 75 rows) and rarely changes. A full ZIP5 table (about 42,000 rows, updated often) should be loaded into bronze as a source instead.

Should the validation happen in the silver layer?

Yes, silver is where we check data quality. But we should flag the bad rows (a warning, or a zip_is_consistent column), not delete them. A wrong zip code does not make the orders wrong.

What happens when the reference data changes?

We update the CSV, commit it, and run `dbt seed` and `dbt build` again. Git keeps the history of each version. If we need the old versions inside the data, we can use a dbt snapshot.

## Exercise 5: view vs table

Where is the data stored?

- View: only the SQL is stored. The data stays in the CSV files on S3.
- Table: the result is copied into the lab_medallion.duckdb file.

When is the transformation executed?

- View: every time the view is queried.
- Table: only once, during `dbt run` or `dbt build`.

What happens when the underlying CSV changes?

- View: we see the change immediately.
- Table: it stays old until the next `dbt build`.

Which approach requires more storage?

The table, because it copies the data. In our test, the database went from 1.06 MB with views to 1.85 MB with tables.

Which approach avoids re-reading the source files for every query?

The table. In our test, a query on stg_orders took 0.362 s as a view and 0.001 s as a table. With views, stg_users was even slower (0.644 s), because it also reads orders.csv to find the first order.
