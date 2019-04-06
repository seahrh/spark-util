package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

// scalastyle:off magic.number
class SmoteSpec extends FlatSpec with DataFrameSuiteBase {

  import spark.implicits._

  "Input validation" should "throw exception if the sample is empty" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = spark.emptyDataFrame,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String]
      )(spark)
    }
    assert(caught.getMessage contains "sample must not be empty")
  }

  it should "throw exception if the number of hash tables is less than 1" in {
    val sample: DataFrame = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = 0.1, d2 = 0.2)
    ).toDF
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sample,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String],
        numHashTables = 0
      )(spark)
    }
    assert(caught.getMessage contains "number of hash tables must be greater than or equals 1")
  }
}

private final case class SmoteSpecSchema(s1: String, s2: String, l1: Long, l2: Long, d1: Double, d2: Double)
