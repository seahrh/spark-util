package com.sgcharts.sparkutil

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
}
