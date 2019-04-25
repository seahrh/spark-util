package com.sgcharts.sparkutil

import java.sql.Timestamp

import org.scalatest.FlatSpec

class ToSqlTimestampSpec extends FlatSpec {

  "toSqlTimestamp" should "return Timestamp when input is in the format \"yyyy-mm-dd hh:mm:ss[.fffffffff]\"" in {
    val inp = "2018-12-31 23:59:59"
    assertResult(Option(Timestamp.valueOf(inp)))(toSqlTimestamp(inp))
  }

  it should "return Timestamp when input is in ISO format \"yyyy-mm-ddThh:mm:ss[.fffffffff]\"" in {
    val inp = "2018-12-31T23:59:59"
    assertResult(
      Option(Timestamp.valueOf("2018-12-31 23:59:59"))
    )(toSqlTimestamp(inp))
  }

  it should "return None when input is missing time component" in {
    val inp = "2018-12-31"
    assertResult(None)(toSqlTimestamp(inp))
  }

  it should "return None when input is empty string" in {
    assertResult(None)(toSqlTimestamp(""))
  }

  it should "return None when input is \"0000-00-00 00:00:00\"" in {
    assertResult(None)(toSqlTimestamp("0000-00-00 00:00:00"))
  }

}
