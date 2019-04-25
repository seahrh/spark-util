[![buildstatus](https://travis-ci.org/seahrh/spark-util.svg?branch=master)](https://travis-ci.org/seahrh/spark-util)

# spark-util
Apache Spark utility for common use cases and bug workarounds.

Contents
---------
- [Getting started](#getting-started)
- [Handling the imbalanced class problem with SMOTE](#handling-the-imbalanced-class-problem-with-smote)
- [Dataset union bug](#dataset-union-bug)
- [Saving partitioned Hive table bug](#saving-partitioned-hive-table-bug)
- [Count by key](#count-by-key)
- [Logging](#logging)
## Getting started
Add the following to your `build.sbt`

Latest tag: [![Scaladex](https://index.scala-lang.org/seahrh/spark-util/latest.svg)](https://index.scala-lang.org/seahrh/spark-util)
```scala
libraryDependencies += "com.sgcharts" %% "spark-util" % "<latest_tag>"
```
## Handling the imbalanced class problem with SMOTE
There is a number of ways to deal with the imbalanced class problem
but they have drawbacks:
- Under-sample the majority class - losing data
- Over-sample the minority class - risk overfitting

Alternatively, SMOTE over-samples the minority class by creating “synthetic” examples
rather than by over-sampling with replacement.

### Synthetic examples
Synthetic examples are generated in the following way:
- Take the difference between the feature vector (sample) under consideration and its nearest neighbour
- Multiply this difference by a random number between 0 and 1, and add it to the sample

For discrete attributes, the synthetic example randomly picks either the sample or the neighbour, and copies that value.

By forcing the decision region of the minority class to become more general,
SMOTE reduces overfitting.

Based on Chawla, N. V., Bowyer, K. W., Hall, L. O., & Kegelmeyer, W. P. (2002).
SMOTE: synthetic minority over-sampling technique. Journal of artificial intelligence research, 16, 321-357.

### SMOTE in Spark
Nearest neighbours are approximated with the [Locality Sensitive Hashing (LSH)](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.BucketedRandomProjectionLSH) model introduced in Spark ML 2.1.

Collecting data is deliberately avoided, so that the driver does not require additional memory.

Usage
```scala
import com.sgcharts.sparkutil.Smote

val df: DataFrame = Smote(
    sample = minorityClassDf,
    discreteStringAttributes = Seq("name"),
    discreteLongAttributes = Seq("house_id", "house_zip"),
    continuousAttributes = Seq("age", "rent_amount")
).syntheticSample
```

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
## Saving partitioned Hive table bug
Hive partitions written by the [`DataFrameWriter`](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.DataFrameWriter)`#saveAsTable` API, are not registered in the Hive metastore ([SPARK-14927](https://issues.apache.org/jira/browse/SPARK-14927)). Hence the partitions are not accessible in Hive. 

As of Spark 2.3.1, the `DataFrameWriter` API is still bugged out, with the `insertInto` and `saveAsTable` giving different problems on Hive. For example, `saveAsTable` will always overwrite existing partitions, effectively allowing only one partition to exist at any one time. 

Solution: Instead of using the `saveAsTable` API, register the partition explicitly.

[`TablePartition`](src/main/scala/com/sgcharts/sparkutil/TablePartition.scala) contains a `Dataset` to be written to a *single* partition. Optionally, set the number of files to write per partition (default=1).

Usage: saving a single partition
```scala
import com.sgcharts.sparkutil.ParquetTablePartition

val ds: Dataset[MyTableSchema] = ???

val part = ParquetTablePartition[MyTableSchema](
  ds=ds,
  db="mydb",
  table="mytable",
  path="/hive/warehouse/mydb.db/mytable/ds=20181231/country=sg",
  partition="ds='20181231',country='sg'"
)

// Overwrite partition
part.overwrite()

// Or append data to an existing partition
part.append()

```

## Count by key
[`CountAccumulator`](src/main/scala/com/sgcharts/sparkutil/CountAccumulator.scala) extends [`org.apache.spark.util.AccumulatorV2`](https://spark.apache.org/docs/2.2.0/api/java/org/apache/spark/util/AccumulatorV2.html). It can count any key that implements [`Ordering`](http://www.scala-lang.org/api/2.12.0/scala/math/Ordering.html). The accumulator returns a [`SortedMap`](http://www.scala-lang.org/api/2.12.3/scala/collection/immutable/SortedMap.html) of the keys and their counts.

Unlike `HashMap`, `SortedMap` uses [`compareTo`](https://docs.oracle.com/javase/8/docs/api/java/lang/Comparable.html) instead of `equals` to determine whether two keys are the same. For example, consider the `BigDecimal` class whose `compareTo` method is inconsistent with `equals`. If only two keys `BigDecimal("1.0")` and `BigDecimal("1.00")` exist, the resulting `SortedMap` will contain only one entry because the two keys are equal when compared using the `compareTo` method. 

On creation, `CountAccumulator` automatically registers the accumulator with `SparkContext`.

Usage
```scala
import org.apache.spark.SparkContext
import com.sgcharts.sparkutil.CountAccumulator

val sc: SparkContext = ???
val a = CountAccumulator[String](sc, Option("my accumulator name")) // Counting String keys
val df: DataFrame = ???
df.foreach(x => a.add(x))
val result: SortedMap[String, Long] = a.value
```
See [CountAccumulatorSpec](src/test/scala/com/sgcharts/sparkutil/CountAccumulatorSpec.scala) for more examples.

Based on [hammerlab's](https://github.com/hammerlab/spark-util) `spark-util`.
## Logging
Spark uses log4j (not logback).

Writes to console `stderr` (default `log4j.properties` in spark/conf)

Usage
```scala
import com.sgcharts.sparkutil.Log4jLogging

object MySparkApp extends Log4jLogging {
    log.info("Hello World!")
}
```
See https://stackoverflow.com/questions/29208844/apache-spark-logging-within-scala
