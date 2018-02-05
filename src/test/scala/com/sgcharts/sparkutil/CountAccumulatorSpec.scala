package com.sgcharts.sparkutil

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CountAccumulatorSpec extends FlatSpec with SharedSparkContext {
  override implicit def reuseContextIfPossible: Boolean = true

  "CountAccumulator" should "count keys of type String" in {
    val a = CountAccumulator[String](sc, Option("CountAccumulatorSpec"))
    val e = SortedMap[String, Long](
      "a" -> 1,
      "b" -> 2,
      "c" -> 3,
      "d" -> 4,
      "e" -> 5
    )
    val in: Array[String] = Random.shuffle(
      ArrayBuffer.fill(1)("a") ++ ArrayBuffer.fill(2)("b") ++ ArrayBuffer.fill(3)("c") ++
        ArrayBuffer.fill(4)("d") ++ ArrayBuffer.fill(5)("e")
    ).toArray
    sc.parallelize(in).foreach(x => a.add(x))
    assertResult(e)(a.value)
  }

  it should
    "count keys of type java.time.LocalDateTime (must first be converted to unix timestamp)" in {
    val a = CountAccumulator[Long](sc, Option("CountAccumulatorSpec"))
    // LocalDateTime must be converted to unix timestamp of type Long
    // because LocalDateTime does not have implicit Ordering defined.
    val t1: Long = LocalDateTime.parse(
      "2018-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC)
    val t2: Long = LocalDateTime.parse(
      "2017-12-31T23:59:59", DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC)
    val t3: Long = LocalDateTime.parse(
      "2018-01-01T01:00:00", DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC)
    val t4: Long = LocalDateTime.parse(
      "2017-11-30T12:00:00", DateTimeFormatter.ISO_DATE_TIME).toEpochSecond(ZoneOffset.UTC)
    val e = SortedMap[Long, Long](
      t1 -> 1,
      t2 -> 2,
      t3 -> 3,
      t4 -> 4
    )
    val in: Array[Long] = Random.shuffle(
      ArrayBuffer.fill(1)(t1) ++ ArrayBuffer.fill(2)(t2) ++ ArrayBuffer.fill(3)(t3) ++
        ArrayBuffer.fill(4)(t4)
    ).toArray
    sc.parallelize(in).foreach(x => a.add(x))
    assertResult(e)(a.value)
  }

  it should "count keys of type Long" in {
    val a = CountAccumulator[Long](sc, Option("CountAccumulatorSpec"))
    val e = SortedMap[Long, Long](
      1L -> 1,
      2L -> 2,
      3L -> 3,
      4L -> 4,
      5L -> 5
    )
    val in: Array[Long] = Random.shuffle(
      ArrayBuffer.fill(1)(1L) ++ ArrayBuffer.fill(2)(2L) ++ ArrayBuffer.fill(3)(3L) ++
        ArrayBuffer.fill(4)(4L) ++ ArrayBuffer.fill(5)(5L)
    ).toArray
    sc.parallelize(in).foreach(x => a.add(x))
    assertResult(e)(a.value)
  }

  it should "count keys of type Double" in {
    val a = CountAccumulator[Double](sc, Option("CountAccumulatorSpec"))
    val e = SortedMap[Double, Long](
      1.1 -> 1,
      2.2 -> 2,
      3.3 -> 3,
      4.4 -> 4,
      5.5 -> 5
    )
    val in: Array[Double] = Random.shuffle(
      ArrayBuffer.fill(1)(1.1) ++ ArrayBuffer.fill(2)(2.2) ++ ArrayBuffer.fill(3)(3.3) ++
        ArrayBuffer.fill(4)(4.4) ++ ArrayBuffer.fill(5)(5.5)
    ).toArray
    sc.parallelize(in).foreach(x => a.add(x))
    assertResult(e)(a.value)
  }
}
