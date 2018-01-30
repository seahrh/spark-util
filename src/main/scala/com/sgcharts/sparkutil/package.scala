package com.sgcharts

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

package object sparkutil extends LazyLogging {
  /**
    * Dataframe workaround for dataset union bug
    * @see [[https://issues.apache.org/jira/browse/SPARK-21109]]
    *
    * @param left  left dataframe
    * @param right right dataframe
    * @return
    */
  def union(left: DataFrame, right: DataFrame): DataFrame = {
    val cols: Array[String] = left.columns
    val res: DataFrame = left.union(right.select(cols.head, cols.tail: _*))
    logger.debug(
      s"""
         |Left schema ${left.schema.treeString}
         |Right schema ${right.schema.treeString}
         |Union schema ${res.schema.treeString}
       """.stripMargin)
    res
  }
}
