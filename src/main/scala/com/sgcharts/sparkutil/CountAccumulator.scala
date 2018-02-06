package com.sgcharts.sparkutil


import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.SortedMap
import scala.collection.mutable

/**
  * [[org.apache.spark.util.AccumulatorV2]] returns a [[scala.collection.immutable.SortedMap]] of keys and their counts.
  *
  * @see [[https://github.com/hammerlab/spark-util]]
  */
final case class CountAccumulator[T: Ordering](
                                                var map: mutable.Map[T, Long] = mutable.Map.empty[T, Long]
                                              )
  extends AccumulatorV2[T, SortedMap[T, Long]] {

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[T, SortedMap[T, Long]] = CountAccumulator(map.clone())


  override def reset(): Unit = {
    map = mutable.Map.empty[T, Long]
  }

  override def add(k: T): Unit = {
    map.update(k, map.getOrElse(k, 0L) + 1)
  }

  override def merge(other: AccumulatorV2[T, SortedMap[T, Long]]): Unit = {
    for ((k, v) <- other.value) {
      map.update(k, map.getOrElse(k, 0L) + v)
    }
  }

  override def value: SortedMap[T, Long] = SortedMap(map.toSeq: _*)
}

object CountAccumulator {

  /**
    * Factory method with different signature from [[CountAccumulator]] case class.
    * This will register the accumulator with Spark before returning a new instance.
    *
    * @param sc SparkContext
    * @param name accumulator name shown on SPark UI
    * @tparam T key of type T
    * @return CountAccumulator
    */
  def apply[T: Ordering](sc: SparkContext, name: Option[String]): CountAccumulator[T] = {
    val a = CountAccumulator[T]()
    name match {
      case Some(n) => sc.register(a, n)
      case _ => sc.register(a)
    }
    a
  }
}
