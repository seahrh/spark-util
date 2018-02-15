# spark-util
Tiny Spark utility for common use cases and bug workarounds. Unit tested on Spark 2.2.
## Dataset union bug
Union of `Dataset` is bugged ([SPARK-21109](https://issues.apache.org/jira/browse/SPARK-21109)). Internally, union resolves by column position (not by name). 

Solution: Convert `Dataset` to `DataFrame`, then reorder the column positions so that they are the same as the first operand.

You can also apply union on more than two DataFrames in one call (varargs). This is unlike the Spark API which takes in only two at a time.

```scala
import com.sgcharts.sparkutil.union

val ds1: Dataset[MyCaseClass] = ???
val ds2: Dataset[MyCaseClass] = ???
val ds3: Dataset[MyCaseClass] = ???
import spark.implicits._
val res: Dataset[MyCaseClass] = union(ds1.toDF, ds2.toDF, ds3.toDF).as[MyCaseClass]
```
## DataFrameWriter#saveAsTable bug when saving Hive partitions
Hive partitions written by the [`DataFrameWriter`](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.DataFrameWriter)`#saveAsTable` API, are not registered in the Hive metastore ([SPARK-14927](https://issues.apache.org/jira/browse/SPARK-14927)). Hence the partitions are not accessible in Hive.

Instead of `saveAsTable`, save the output file and register the partition explicitly
- `partitionBy` saves the output files in a directory layout similar to Hive's partitioning scheme 

   e.g. if partition columns are year and month, `year=2016/month=01/`
- Call the API corresponding to file format e.g. `parquet`, `orc`, `json`
- Register the Hive partition with Spark SQL

[`TablePartition`](src/main/scala/com/sgcharts/sparkutil/TablePartition.scala) contains a `Dataset` to be written to a *single* partition. Get a `DataFrameWriter` by calling `writer`. Optionally, set the number of files per partition (default=1). Use the `DataFrameWriter` to implement the `overwrite` and `append` methods. 
```scala
import com.sgcharts.sparkutil.TablePartition
import org.apache.spark.sql.SaveMode

// Extend TablePartition

override def overwrite(): Unit = {
  writer(SaveMode.Overwrite).partitionBy("date", "country").parquet(tablePath)
  addPartition()
}

override def append(): Unit = {
  writer(SaveMode.Append).partitionBy("date", "country").parquet(tablePath)
  addPartition()
}
```
See [`ParquetPartition`](src/test/scala/com/sgcharts/sparkutil/ParquetPartition.scala) for a complete example.
