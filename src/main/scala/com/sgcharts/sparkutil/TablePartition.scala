package com.sgcharts.sparkutil

import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}

/**
  * Model of a single table partition. Extend this trait to define partition columns.
  * @tparam T dataset of type T
  */
trait TablePartition[T] {
  val tablePath: String
  val ds: Dataset[T]

  def overwrite(): Unit

  def append(): Unit

  /**
    * Convenience method to get a [[org.apache.spark.sql.DataFrameWriter]].
    * By default, write one file per partition.
    *
    * @param mode Writer mode e.g. overwrite, append
    * @param numFiles Number of files to write per partition. Default is 1.
    * @return DataFrameWriter
    */
  protected def writer(mode: SaveMode, numFiles: Int = 1): DataFrameWriter[T] = {
    ds.coalesce(numFiles).write.mode(mode)
  }
}

/**
  * Model of a single table partition belonging to a table managed by Spark SQL.
  * @tparam T dataset of type T
  */
trait ManagedTablePartition[T] extends TablePartition[T]

/**
  * Model of a single table partition belonging to a table unmanaged by Spark SQL.
  * @tparam T dataset of type T
  */
trait UnmanagedTablePartition[T]  extends TablePartition[T] {
  val spark: SparkSession
  val table: String

  def path: String

  def spec: String

  protected def addPartition(): Unit = {
    val sql = s"""alter table $table add if not exists partition ($spec) location "$path""""
    spark.sql(sql)
  }
}
