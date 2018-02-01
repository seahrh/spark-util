package com.sgcharts

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

package object sparkutil extends LazyLogging {

  /*def union(left: DataFrame, right: DataFrame): DataFrame = {
    val cols: Array[String] = left.columns
    val res: DataFrame = left.union(right.select(cols.head, cols.tail: _*))
    logger.debug(
      s"""
         |Left schema ${left.schema.treeString}
         |Right schema ${right.schema.treeString}
         |Union schema ${res.schema.treeString}
       """.stripMargin)
    res
  }*/

  /**
    * Dataframe workaround for dataset union bug
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    * @param head first dataframe
    * @param tail iterable of dataframes
    * @return
    */
  def union(head: DataFrame, tail: DataFrame*): DataFrame = {
    val cols: Array[String] = head.columns
    var res: DataFrame = head
    for (df <- tail) {
      res = res.union(df.select(cols.head, cols.tail: _*))
      logger.debug(s"union schema ${res.schema.treeString}")
    }
    res
  }
}
