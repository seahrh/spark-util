package com.sgcharts.sparkutil

object LocalitySensitiveHashing {

  // Based on https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.BucketedRandomProjectionLSH
  def bucketLength(numRecords: Long, numDimensions: Long): Double = {
    10 * Math.pow(numRecords, -1 / numDimensions)
  }

}
