package com.sgcharts.sparkutil

import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}

trait TablePartition[T] {
  val tablePath: String
  val ds: Dataset[T]
  val fileCount: Int = 1

  def overwrite(): Unit

  def append(): Unit

  protected def writer(mode: SaveMode, numFiles: Int = 1): DataFrameWriter[T] = {
    ds.coalesce(numFiles).write.mode(mode)
  }
}

trait ManagedTablePartition[T] extends TablePartition[T]

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
