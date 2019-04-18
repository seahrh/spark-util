package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec

// scalastyle:off magic.number
class SmoteSpec extends FlatSpec with DataFrameSuiteBase {

  import spark.implicits._

  private val sampleWithOneRow: Seq[SmoteSpecSchema] = Seq(
    SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = 0.1, d2 = 0.2)
  )

  "Input validation" should "throw exception when the sample is empty" in {
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

  it should "throw exception when the number of hash tables is less than 1" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sampleWithOneRow.toDF,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String],
        numHashTables = 0
      )(spark)
    }
    assert(caught.getMessage contains "number of hash tables must be greater than or equals 1")
  }

  it should "throw exception when the size multiplier is less than 2" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sampleWithOneRow.toDF,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String],
        sizeMultiplier = 1
      )(spark)
    }
    assert(caught.getMessage contains "size multiplier must be greater than or equals 2")
  }

  it should "throw exception when the number of nearest neighbours is less than 1" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sampleWithOneRow.toDF,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String],
        numNearestNeighbours = 0
      )(spark)
    }
    assert(caught.getMessage contains "number of nearest neighbours must be greater than or equals 1")
  }

  it should "throw exception when the bucket length is less than or equals zero" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sampleWithOneRow.toDF,
        discreteStringAttributes = Seq[String]("s1"),
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String],
        bucketLength = Option(0)
      )(spark)
    }
    assert(caught.getMessage contains "bucket length must be greater than zero")
  }

  it should "throw exception when no attributes are specified" in {
    val caught = intercept[IllegalArgumentException] {
      Smote(
        sample = sampleWithOneRow.toDF,
        discreteStringAttributes = Seq.empty[String],
        discreteLongAttributes = Seq.empty[String],
        continuousAttributes = Seq.empty[String]
      )(spark)
    }
    assert(caught.getMessage contains "there must be at least one attribute")
  }
}

private final case class SmoteSpecSchema(s1: String, s2: String, l1: Long, l2: Long, d1: Double, d2: Double)
