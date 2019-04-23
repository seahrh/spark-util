package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

import scala.util.Random

// scalastyle:off magic.number
class SmoteSpec extends FlatSpec with DataFrameSuiteBase {

  private implicit val rand: Random = new Random

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

  "Smote#setContinuousAttribute" should "take a value in a range bounded by its parents" in {
    val lo: Double = -0.1
    val hi: Double = 0.1
    val r1: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = lo, d2 = 0.3)
    ).toDF.head()
    val r2: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = hi, d2 = 0.4)
    ).toDF.head()
    for (_ <- 0 until 100) {
      val name: String = "d1"
      val a: Row = Smote.setContinuousAttribute(name, r1, r2)
      val d: Double = a.getAs[Double](name)
      assert(d >= lo && d <= hi)
    }
  }

  it should "take a value in a range bounded by its parents, regardless of order of operands" in {
    val lo: Double = -0.1
    val hi: Double = 0.1
    val r1: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = lo, d2 = 0.3)
    ).toDF.head()
    val r2: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = hi, d2 = 0.4)
    ).toDF.head()
    for (_ <- 0 until 100) {
      val name: String = "d1"
      val a: Row = Smote.setContinuousAttribute(name, r2, r1)
      val d: Double = a.getAs[Double](name)
      assert(d >= lo && d <= hi)
    }
  }

  "Smote#setDiscreteAttribute" should
    "take a value that exists in either parent, when the type is String" in {
    val left: String = "left"
    val right: String = "right"
    val r1: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = left, s2 = "b", l1 = 1, l2 = 2, d1 = 0.1, d2 = 0.3)
    ).toDF.head()
    val r2: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = right, s2 = "b", l1 = 1, l2 = 2, d1 = 0.2, d2 = 0.4)
    ).toDF.head()
    var leftSeen: Boolean = false
    var rightSeen: Boolean = false
    var i: Int = 0
    while (i < 100 && (!leftSeen || !rightSeen)) {
      val name: String = "s1"
      val a: Row = Smote.setDiscreteAttribute(name, r1, r2)
      a.getAs[String](name) match {
        case `left` => leftSeen = true
        case `right` => rightSeen = true
        case x => fail(s"Illegal value that does not exist in parents [$x]")
      }
      i += 1
    }
    assert(leftSeen && rightSeen)
  }

  it should "take a value that exists in either parent, when the type is Long" in {
    val left: Long = -9
    val right: Long = 1
    val r1: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = left, l2 = 2, d1 = 0.1, d2 = 0.3)
    ).toDF.head()
    val r2: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "b", s2 = "b", l1 = right, l2 = 2, d1 = 0.2, d2 = 0.4)
    ).toDF.head()
    var leftSeen: Boolean = false
    var rightSeen: Boolean = false
    var i: Int = 0
    while (i < 100 && (!leftSeen || !rightSeen)) {
      val name: String = "l1"
      val a: Row = Smote.setDiscreteAttribute(name, r1, r2)
      a.getAs[Long](name) match {
        case `left` => leftSeen = true
        case `right` => rightSeen = true
        case x => fail(s"Illegal value that does not exist in parents [$x]")
      }
      i += 1
    }
    assert(leftSeen && rightSeen)
  }

  it should "take a value that exists in either parent, regardless of order of operand" in {
    val left: Long = -9
    val right: Long = 1
    val r1: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = left, l2 = 2, d1 = 0.1, d2 = 0.3)
    ).toDF.head()
    val r2: Row = Seq[SmoteSpecSchema](
      SmoteSpecSchema(s1 = "b", s2 = "b", l1 = right, l2 = 2, d1 = 0.2, d2 = 0.4)
    ).toDF.head()
    var leftSeen: Boolean = false
    var rightSeen: Boolean = false
    var i: Int = 0
    while (i < 100 && (!leftSeen || !rightSeen)) {
      val name: String = "l1"
      val a: Row = Smote.setDiscreteAttribute(name, r2, r1)
      a.getAs[Long](name) match {
        case `left` => leftSeen = true
        case `right` => rightSeen = true
        case x => fail(s"Illegal value that does not exist in parents [$x]")
      }
      i += 1
    }
    assert(leftSeen && rightSeen)
  }

  "Smote#syntheticSample" should "return the correct output size" in {
    val in: Seq[SmoteSpecSchema] = Seq(
      SmoteSpecSchema(s1 = "a", s2 = "b", l1 = 1, l2 = 2, d1 = 0.1, d2 = 0.2),
      SmoteSpecSchema(s1 = "c", s2 = "d", l1 = 3, l2 = 4, d1 = 0.3, d2 = 0.4)
    )
    val sizeMultiplier: Int = 2
    val a: DataFrame = Smote(
      sample = in.toDF,
      discreteStringAttributes = Seq[String]("s1", "s2"),
      discreteLongAttributes = Seq[String]("l1", "l2"),
      continuousAttributes = Seq[String]("d1", "d2"),
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    assertResult(in.length * sizeMultiplier)(a.count)
  }

  it should "return the correct output size, when the input size is 1" in {
    val sizeMultiplier: Int = 2
    val a: DataFrame = Smote(
      sample = sampleWithOneRow.toDF,
      discreteStringAttributes = Seq.empty[String], // excluded because there must be at least 2 distinct values
      discreteLongAttributes = Seq.empty[String], // excluded because there must be at least 2 distinct values
      continuousAttributes = Seq[String]("d1", "d2"),
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    assertResult(sampleWithOneRow.length * sizeMultiplier)(a.count)
  }

  it should "create synthetic values and the right schema, when all attributes are selected" in {
    // discrete Long attribute cannot be a negative number
    // output can possibly have duplicates of input data
    val left = SmoteSpecSchema(s1 = "a", s2 = "c", l1 = 1, l2 = 3, d1 = -0.1, d2 = 0.1)
    val right = SmoteSpecSchema(s1 = "b", s2 = "d", l1 = 2, l2 = 4, d1 = -0.2, d2 = 0.2)
    val in = Seq[SmoteSpecSchema](left, right)
    val sizeMultiplier: Int = 50
    val res: DataFrame = Smote(
      sample = in.toDF,
      discreteStringAttributes = Seq[String]("s1", "s2"),
      discreteLongAttributes = Seq[String]("l1", "l2"),
      continuousAttributes = Seq[String]("d1", "d2"),
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    for (row <- res.collect) {
      val a = SmoteSpecSchema.from(row)
      assert(a.s1 == left.s1 || a.s1 == right.s1)
      assert(a.s2 == left.s2 || a.s2 == right.s2)
      assert(a.l1 == left.l1 || a.l1 == right.l1)
      assert(a.l2 == left.l2 || a.l2 == right.l2)
      assert(a.d1 >= right.d1 && a.d1 <= left.d1)
      assert(a.d2 >= left.d2 && a.d2 <= right.d2)
    }
    assertResult(SmoteSpecSchema.schema)(res.schema)
  }

  it should "create synthetic values and the right schema, when a single discrete String attribute is selected" in {
    val left = SmoteSpecSchema(s1 = "a", s2 = "c", l1 = 1, l2 = 3, d1 = -0.1, d2 = 0.1)
    val right = SmoteSpecSchema(s1 = "b", s2 = "d", l1 = 2, l2 = 4, d1 = -0.2, d2 = 0.2)
    val in = Seq[SmoteSpecSchema](left, right)
    val sizeMultiplier: Int = 50
    val res: DataFrame = Smote(
      sample = in.toDF,
      discreteStringAttributes = Seq[String]("s2"),
      discreteLongAttributes = Seq.empty[String],
      continuousAttributes = Seq.empty[String],
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    for (row <- res.collect) {
      val a = row.getAs[String]("s2")
      assert(a == left.s2 || a == right.s2)
    }
    assertResult(StructType(Seq(
      StructField("s2", StringType, nullable = true)
    )))(res.schema)
  }

  it should "create synthetic values and the right schema, when a single discrete Long attribute is selected" in {
    val left = SmoteSpecSchema(s1 = "a", s2 = "c", l1 = 1, l2 = 3, d1 = -0.1, d2 = 0.1)
    val right = SmoteSpecSchema(s1 = "b", s2 = "d", l1 = 2, l2 = 4, d1 = -0.2, d2 = 0.2)
    val in = Seq[SmoteSpecSchema](left, right)
    val sizeMultiplier: Int = 50
    val res: DataFrame = Smote(
      sample = in.toDF,
      discreteStringAttributes = Seq.empty[String],
      discreteLongAttributes = Seq[String]("l2"),
      continuousAttributes = Seq.empty[String],
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    for (row <- res.collect) {
      val a = row.getAs[Long]("l2")
      assert(a == left.l2 || a == right.l2)
    }
    assertResult(StructType(Seq(
      StructField("l2", LongType, nullable = false)
    )))(res.schema)
  }

  it should "create synthetic values and the right schema, when a single continuous attribute is selected" in {
    val left = SmoteSpecSchema(s1 = "a", s2 = "c", l1 = 1, l2 = 3, d1 = -0.1, d2 = 0.1)
    val right = SmoteSpecSchema(s1 = "b", s2 = "d", l1 = 2, l2 = 4, d1 = -0.2, d2 = 0.2)
    val in = Seq[SmoteSpecSchema](left, right)
    val sizeMultiplier: Int = 50
    val res: DataFrame = Smote(
      sample = in.toDF,
      discreteStringAttributes = Seq.empty[String],
      discreteLongAttributes = Seq.empty[String],
      continuousAttributes = Seq[String]("d2"),
      sizeMultiplier = sizeMultiplier
    )(spark).syntheticSample
    for (row <- res.collect) {
      val a = row.getAs[Double]("d2")
      assert(a >= left.d2 && a <= right.d2)
    }
    assertResult(StructType(Seq(
      StructField("d2", DoubleType, nullable = false)
    )))(res.schema)
  }
}

private final case class SmoteSpecSchema(s1: String, s2: String, l1: Long, l2: Long, d1: Double, d2: Double)

private object SmoteSpecSchema {
  private[sparkutil] def from(row : Row): SmoteSpecSchema = {
    SmoteSpecSchema(
      s1 = row.getAs[String]("s1"),
      s2 = row.getAs[String]("s2"),
      l1 = row.getAs[Long]("l1"),
      l2 = row.getAs[Long]("l2"),
      d1 = row.getAs[Double]("d1"),
      d2 = row.getAs[Double]("d2")
    )
  }

  private[sparkutil] val schema: StructType = StructType(Seq(
    StructField("s1", StringType, nullable = true),
    StructField("s2", StringType, nullable = true),
    StructField("l1", LongType, nullable = false),
    StructField("l2", LongType, nullable = false),
    StructField("d1", DoubleType, nullable = false),
    StructField("d2", DoubleType, nullable = false)
  ))
}
