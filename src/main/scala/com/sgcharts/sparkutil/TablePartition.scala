package com.sgcharts.sparkutil

import org.apache.spark.sql._

/**
  * Model of a single table partition.
  *
  * @tparam T dataset of type T
  */
sealed trait TablePartition[T] extends Log4jLogging {
  private[sparkutil] val ds: Dataset[T]
  private[sparkutil] val db: String
  private[sparkutil] val table: String
  private[sparkutil] val path: String
  private[sparkutil] val partition: String
  private[sparkutil] val numFiles: Int
  private[sparkutil] val sparkSession: SparkSession

  def overwrite(): Unit

  def append(): Unit

  /**
    * Convenience method to get a [[org.apache.spark.sql.DataFrameWriter]].
    * By default, write one file per partition.
    *
    * @param mode     Writer mode e.g. overwrite, append
    * @return DataFrameWriter
    */
  private[sparkutil] def writer(mode: SaveMode): DataFrameWriter[T] = {
    ds.coalesce(numFiles).write.mode(mode)
  }

  private[sparkutil] def addPartition(): DataFrame = {
    val sql: String = s"alter table $db.$table add if not exists partition ($partition) location '$path'"
    log.debug(sql)
    sparkSession.sql(sql)
  }

}

final case class ParquetTablePartition[T](
                                           override val ds: Dataset[T],
                                           override val db: String,
                                           override val table: String,
                                           override val path: String,
                                           override val partition: String,
                                           override val numFiles: Int = 1
                                         )(implicit spark: SparkSession) extends TablePartition[T] {
  override val sparkSession: SparkSession = spark

  override def overwrite(): Unit = {
    writer(SaveMode.Overwrite).parquet(path)
    addPartition()
  }

  override def append(): Unit = {
    writer(SaveMode.Append).parquet(path)
    addPartition()
  }
}

final case class OrcTablePartition[T](
                                           override val ds: Dataset[T],
                                           override val db: String,
                                           override val table: String,
                                           override val path: String,
                                           override val partition: String,
                                           override val numFiles: Int = 1
                                         )(implicit spark: SparkSession) extends TablePartition[T] {
  override val sparkSession: SparkSession = spark

  override def overwrite(): Unit = {
    writer(SaveMode.Overwrite).orc(path)
    addPartition()
  }

  override def append(): Unit = {
    writer(SaveMode.Append).orc(path)
    addPartition()
  }
}
