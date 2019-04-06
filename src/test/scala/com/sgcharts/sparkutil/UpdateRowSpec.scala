package com.sgcharts.sparkutil

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class UpdateRowSpec extends FlatSpec {

  "Update" should "retain the schema of the original row" in {
    val schema: StructType = StructType(Array(
      StructField("s", StringType, nullable = false),
      StructField("l", LongType, nullable = false),
      StructField("d", DoubleType, nullable = false)
    ))
    val in = new GenericRowWithSchema(Array("abc", 1, 0.1), schema)
    assertResult(schema)(update(in, "s", "def").schema)
  }

  it should "update only the specified field and leave the other fields unchanged" in {
    val schema: StructType = StructType(Array(
      StructField("s", StringType, nullable = false),
      StructField("l", LongType, nullable = false),
      StructField("d", DoubleType, nullable = false)
    ))
    val in = new GenericRowWithSchema(Array("abc", 1, 0.1), schema)
    assertResult(
      new GenericRowWithSchema(Array("def", 1, 0.1), schema)
    )(update(in, "s", "def"))
    assertResult(
      new GenericRowWithSchema(Array("abc", 2, 0.1), schema)
    )(update(in, "l", 2))
    assertResult(
      new GenericRowWithSchema(Array("abc", 1, 0.2), schema)
    )(update(in, "d", 0.2))
  }
}
