package com.sgcharts.sparkutil

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

sealed trait ParquetPartition[T] extends UnmanagedTablePartition[T]

sealed trait TDailyPartition[T] extends ParquetPartition[T] {
  val date: String

  override def spec: String = s"""date="$date""""

  override def path: String = s"$tablePath/date=$date"

  override def overwrite(): Unit = {
    writer(SaveMode.Overwrite).partitionBy("date").parquet(tablePath)
    addPartition()
  }

  override def append(): Unit = {
    writer(SaveMode.Append).partitionBy("date").parquet(tablePath)
    addPartition()
  }
}

/**
  * Example of multiple partition columns
  * @tparam T dataset of type T
  */
sealed trait TDailyCountryPartition[T] extends TDailyPartition[T] {
  val country: String

  override def spec: String = s"""${super.spec},country="$country""""

  override def path: String = s"${super.path}/country=$country"

  override def overwrite(): Unit = {
    writer(SaveMode.Overwrite).partitionBy("date", "country").parquet(tablePath)
    addPartition()
  }

  override def append(): Unit = {
    writer(SaveMode.Append).partitionBy("date", "country").parquet(tablePath)
    addPartition()
  }
}

sealed trait THourlyPartition[T] extends ParquetPartition[T] {
  val hour: String

  override def spec: String = s"""hour="$hour""""

  override def path: String = s"$tablePath/hour=$hour"

  override def overwrite(): Unit = {
    writer(SaveMode.Overwrite).partitionBy("hour").parquet(tablePath)
    addPartition()
  }

  override def append(): Unit = {
    writer(SaveMode.Append).partitionBy("hour").parquet(tablePath)
    addPartition()
  }
}

final case class DailyPartition[T](
                                    override val spark: SparkSession,
                                    override val table: String,
                                    override val tablePath: String,
                                    override val ds: Dataset[T],
                                    override val date: String
                                  ) extends TDailyPartition[T]

final case class DailyCountryPartition[T](
                                           override val spark: SparkSession,
                                           override val table: String,
                                           override val tablePath: String,
                                           override val ds: Dataset[T],
                                           override val date: String,
                                           override val country: String
                                         ) extends TDailyCountryPartition[T]

final case class HourlyPartition[T](
                                     override val spark: SparkSession,
                                     override val table: String,
                                     override val tablePath: String,
                                     override val ds: Dataset[T],
                                     override val hour: String
                                   ) extends THourlyPartition[T]
