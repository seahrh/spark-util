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
                                    country: String
                                  ) extends TDailyPartition[T] {

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

final case class HourlyPartition[T](
                              override val spark: SparkSession,
                              override val table: String,
                              override val tablePath: String,
                              override val ds: Dataset[T],
                              hour: String
                            ) extends ParquetPartition[T] {

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
