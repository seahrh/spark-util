package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class DatasetUnionSpec extends FlatSpec with DataFrameSuiteBase {

  import spark.implicits._

  "Union" should "perform union in the order of the operands" in {
    val a: DataFrame = Seq[SchemaA](
      SchemaA(s = "a1", i = 1, d = 1.1),
      SchemaA(s = "a2", i = 2, d = 2.2)
    ).toDF
    val b: DataFrame = Seq[SchemaB](
      SchemaB(s = "b3", i = 3, d = 3.3),
      SchemaB(s = "b4", i = 4, d = 4.4)
    ).toDF
    val c: DataFrame = Seq[SchemaC](
      SchemaC(s = "c5", i = 5, d = 5.5),
      SchemaC(s = "c6", i = 6, d = 6.6)
    ).toDF
    val abc: DataFrame = Seq[SchemaA](
      SchemaA(s = "a1", i = 1, d = 1.1),
      SchemaA(s = "a2", i = 2, d = 2.2),
      SchemaA(s = "b3", i = 3, d = 3.3),
      SchemaA(s = "b4", i = 4, d = 4.4),
      SchemaA(s = "c5", i = 5, d = 5.5),
      SchemaA(s = "c6", i = 6, d = 6.6)
    ).toDF
    assertDataFrameEquals(abc, union(a, b, c))
    val cab: DataFrame = Seq[SchemaC](
      SchemaC(s = "c5", i = 5, d = 5.5),
      SchemaC(s = "c6", i = 6, d = 6.6),
      SchemaC(s = "a1", i = 1, d = 1.1),
      SchemaC(s = "a2", i = 2, d = 2.2),
      SchemaC(s = "b3", i = 3, d = 3.3),
      SchemaC(s = "b4", i = 4, d = 4.4)
    ).toDF
    assertDataFrameEquals(cab, union(c, a, b))
    val bca: DataFrame = Seq[SchemaB](
      SchemaB(s = "b3", i = 3, d = 3.3),
      SchemaB(s = "b4", i = 4, d = 4.4),
      SchemaB(s = "c5", i = 5, d = 5.5),
      SchemaB(s = "c6", i = 6, d = 6.6),
      SchemaB(s = "a1", i = 1, d = 1.1),
      SchemaB(s = "a2", i = 2, d = 2.2)
    ).toDF
    assertDataFrameEquals(bca, union(b, c, a))
  }

  it should "return DF with the same schema as the first operand" in {
    val a: DataFrame = Seq.empty[SchemaA].toDF
    val b: DataFrame = Seq.empty[SchemaB].toDF
    assertDataFrameEquals(a, union(a, b))
  }
}

private final case class SchemaA(s: String, i: Int, d: Double)

private final case class SchemaB(d: Double, s: String, i: Int)

private final case class SchemaC(i: Int, d: Double, s: String)
