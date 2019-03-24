package com.sgcharts

import org.apache.spark.sql.DataFrame

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
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    * @param head first DataFrame
    * @param tail varargs of successive DataFrames
    * @return new DataFrame representing the union
    */
  def union(head: DataFrame, tail: DataFrame*): DataFrame = {
    val dfs: List[DataFrame] = head :: tail.toList
    dfs.reduceLeft(union)
  }
}
