package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class sparkutilSpec extends FlatSpec with DataFrameSuiteBase {

  "Union" should "return empty DF with LHS schema if both inputs are empty" in {
    import spark.implicits._
    val l: DataFrame = Seq.empty[RightSchema].toDF
    val r: DataFrame = Seq.empty[WrongSchema].toDF
    assertDataFrameEquals(l, union(l, r))
  }
}

final case class RightSchema(s: String, i: Int, d: Double)

final case class WrongSchema(d: Double, s: String, i: Int)
