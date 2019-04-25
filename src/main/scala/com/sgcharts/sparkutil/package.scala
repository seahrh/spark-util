package com.sgcharts

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

package object sparkutil extends Log4jLogging {

  private def union(left: DataFrame, right: DataFrame): DataFrame = {
    val cols: Array[String] = left.columns
    val res: DataFrame = left.union(right.select(cols.head, cols.tail: _*))
    log.debug(
      s"""
         |Left schema ${left.schema.treeString}
         |Right schema ${right.schema.treeString}
         |Union schema ${res.schema.treeString}
       """.stripMargin)
    res
  }

  /**
    * DataFrame workaround for Dataset union bug.
    * Union is performed in order of the operands.
    *
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    * @param head first DataFrame
    * @param tail varargs of successive DataFrames
    * @return new DataFrame representing the union
    */
  def union(head: DataFrame, tail: DataFrame*): DataFrame = {
    val dfs: List[DataFrame] = head :: tail.toList
    dfs.reduceLeft(union)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def update(row: Row, fieldName: String, value: Any): Row = {
    val i: Int = row.fieldIndex(fieldName)
    val ovs = row.toSeq
    var vs = ovs.slice(0, i) ++ Seq(value)
    val size: Int = ovs.size
    if (i != size - 1) {
      vs ++= ovs.slice(i + 1, size)
    }
    new GenericRowWithSchema(vs.toArray, row.schema)
  }

  // based on https://stackoverflow.com/a/40801637/519951
  def toDF(rows: Array[Row], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(rows.toList.asJava, schema)
  }

  /**
    * Parses string to java.sql.Timestamp object, wrapped as Option.
    * If string cannot be parsed, then return None.
    *
    * @param str timestamp string in "yyyy-mm-dd hh:mm:ss[.fffffffff]" format
    * @return [[Timestamp]] if parsed successfully, else None
    */
  def toSqlTimestamp(str: String): Option[Timestamp] = {
    val t: String = str.trim.replace('T', ' ')
    if (t.isEmpty || t == "0000-00-00 00:00:00") {
      None
    } else {
      try {
        Option(Timestamp.valueOf(t))
      } catch {
        case _: IllegalArgumentException => None
      }
    }
  }
}
