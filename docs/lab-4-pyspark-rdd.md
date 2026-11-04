# Lab 4 – Introduction to Spark's RDD API: answers

## RDD creation and partitions

Both `users.csv` and `orders.csv` are read into 2 partitions, and `repartition(8)` gives 8.

### What decides the number of partitions `textFile()` picks by default, for a file this size?

The `minPartitions` argument, which defaults to `min(sc.defaultParallelism, 2)`. Spark also splits a file by block
(32 MB in local mode, 128 MB on HDFS), but our files are 7 KB and 331 KB, far below one block. So we end up with the
minimum: 2.

### `repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?

`repartition()` always does a shuffle. The rows are redistributed over the new partitions, which means it can go up or
down. `coalesce()` doesn't shuffle by default; it merges partitions that already exist, so it can only reduce the
count (`coalesce(8)` on our 2 partitions would still give 2).

## Lazy evaluation and lineage

`toDebugString()` only prints the lineage: `HadoopRDD`, then `MapPartitionsRDD`, then `PythonRDD` for the filter.
Nothing is read until `count()`, which returns 2829.

### Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?

Yes. An RDD keeps no result between two actions, so the second `count()` reads the file and filters it again from the
start.

### At which point in this notebook would `.cache()` change that answer?

On `orders_data`, before the first `count()`. There would still be a new job for the second call, but its tasks would
read the partitions from memory and skip the file.

## The cost of no schema: the multiline address

`users_lines.count()` returns 101, which is 1 header plus 50 users written on 2 lines each. The UUID filter brings it
back to 50.

### Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?

The CSV reader knows about quotes. A newline inside a quoted field is part of the field. `textFile()` cuts at every
`\n` and has no idea what a CSV is.

### In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?

One record per line, and no unescaped newline inside a field. JSON Lines is a good example. It's the same property
that lets Spark split the file at any newline.

## Manual parsing of orders

### What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?

`split(",")` would give 6 values for 5 variables and Python would raise a `ValueError`. The `map` itself is lazy, so
the error only shows up at the first action that reaches that line, and it makes the whole job fail.

### Where does the DataFrame API perform the equivalent check, and when?

In the CSV reader, when the rows are read, with the declared or inferred schema. Commas between quotes are handled by
the parser. For broken rows, the `mode` option decides: `PERMISSIVE` (the default, puts nulls), `DROPMALFORMED` or
`FAILFAST`.

## Key-value RDDs: total quantity per user

Same result with `reduceByKey` and `groupByKey`, for example 253 for `ce9e1a11-...`.

I read the shuffle sizes from the Spark UI REST API (`/api/v1/applications/<app-id>/stages`):

| Aggregation | Shuffle write | Shuffle read |
| --- | --- | --- |
| `reduceByKey` | 2513 B | 2513 B |
| `groupByKey` | 6230 B | 6230 B |

So `groupByKey` moves about 2.5 times more data. The UI says 8 records for both, because PySpark sends the pairs to the
JVM in pickled batches and Spark counts the batches. The ratio in bytes is lower than the ratio in pairs (2829 against
100 at most): shuffle files are compressed, and the same `user_id` values come back again and again.

### `reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?

Every `(user_id, quantity)` pair, so 2829 records here. With `reduceByKey` each partition sends one partial sum per key,
which is at most 50 × 2 = 100 records. `groupByKey` sends more over the network and also has to keep all the values of
a key in memory on the reduce side.

### For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?

When you need all the values of a key at the same time. A median or a percentile, or the list of a user's orders sorted
by date. `reduceByKey` only works with a function that is associative and commutative (sum, max, min).

## Manual deduplication

2829 before, 2829 after. The `order_id` values were already unique, so nothing is removed.

### What plays the role of `partitionBy()` here?

The shuffle done by `reduceByKey`. The `map` puts `order_id` as the key, and `reduceByKey` hashes that key to send all
the rows of one `order_id` to the same partition.

### Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?

The window function. You can read "keep the most recent row per `order_id`" in the query itself, while with
`reduceByKey` you have to open `most_recent` to understand it. That function also compares dates as strings, which is
only correct because they all have the same ISO format.

## Joining orders with users

`join()` and `leftOuterJoin()` both give 2829 rows. Every order has its user.

### What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?

They have to be co-partitioned: same partitioner, same number of partitions. For instance, call `partitionBy(n)` on
both and cache them. Another way here, since `users` is small, is to broadcast it and do the join in a `map`.

### Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?

The order stays in the result as `(user_id, (order, None))`. `join()` would just drop it, with no warning.

### `enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset much larger than this one?

Every row goes to the driver. On a big dataset it runs out of memory. Use `take(n)`, filter or aggregate first, or
write the result to storage.

## Exercises

### 1. Average order quantity per user with `aggregateByKey`

```python
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),
        lambda acc, q: (acc[0] + 1, acc[1] + q),
        lambda a, b: (a[0] + b[0], a[1] + b[1]),
    )
)
user_avg = user_stats.mapValues(lambda s: s[1] / s[0])
```

One pass gives both the count and the total. `f143262f-...` gets 3.0 (12 for 4 orders), `ce9e1a11-...` gets 2.98
(253 for 85 orders).

### 2. Order with an unknown user

```python
orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))
orphans = (
    orders_with_fake
    .map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda kv: kv[1][1] is None)
)
```

`orphans.count()` returns 1. It's the fake order, with `None` in place of the username.

### 3. DataFrame version

```python
orders_df = spark.createDataFrame(orders_rdd)
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
```

Same numbers. The DataFrame version is one line; the RDD version also needs the parsing function, the header filter and
the key-value `map`.

`.explain()` prints the physical plan built by Catalyst: a `partial_sum` on each partition, an `Exchange
hashpartitioning`, then the final sum. That's the `reduceByKey` strategy, chosen by Spark on its own. The `Project`
keeps only `quantity` and `user_id` before the shuffle, and the column types are known. The shuffle is planned on 200
partitions (`spark.sql.shuffle.partitions`) and AQE reduces that at runtime.

`.toDebugString()` is just the chain of RDDs (`PythonRDD`, `PairwiseRDD`, `ShuffledRDD`) on 2 partitions. The logic
is inside Python lambdas that Spark can't look into, so there is nothing for it to optimize. The output also shows the
cache of `orders_rdd`: 2 partitions, 168 KiB in memory.

### 4. With and without `.cache()`

| | count 1 | count 2 | user_quantity |
| --- | --- | --- | --- |
| without cache | 0.148 s | 0.195 s | 0.399 s |
| with cache | 0.364 s | 0.150 s | 0.309 s |

With the cache, the first `count()` is the slowest one (0.364 s) because it computes the RDD and stores it. After that
the actions read from memory and `user_quantity` goes from 0.399 s to 0.309 s. On a 331 KB file these gaps are close
to noise, most of the time is spent scheduling tasks and starting the Python workers. On a dataset this small, the cache
doesn't change much.
