# spark-util
Tiny Spark utility for common use cases and bug workarounds. Tested on Spark 2.x.
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
## DataFrame#saveAsTable bug when saving Hive partitions
Hive partitions written by the DataFrame#saveAsTable API, are not registered in the Hive metastore ([SPARK-14927](https://issues.apache.org/jira/browse/SPARK-14927)). Hence the partitions are not accessible in Hive.

Instead of `saveAsTable`, use the "partitionBy, format" idiom when 
