package com.sgcharts.sparkutil

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class UpdateRowSpec extends FlatSpec {

  "Update" should "retain the schema of the original row" in {
    val schema: StructType = StructType(Array(
      StructField("s", StringType, nullable = false),
      StructField("l", LongType, nullable = false),
      StructField("d", DoubleType, nullable = false)
    ))
    val in = new GenericRowWithSchema(Array("string", 1, 0.1), schema)
    assertResult(schema)(update(in, "s", "new_string").schema)
  }
}
