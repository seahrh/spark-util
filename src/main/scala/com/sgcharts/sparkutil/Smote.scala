package com.sgcharts.sparkutil

import org.apache.spark.ml
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

final case class Smote(
                        sample: Dataset[_],
                        discreteStringAttributes: Seq[String],
                        discreteLongAttributes: Seq[String],
                        continuousAttributes: Seq[String],
                        bucketLength: Option[Double] = None,
                        numHashTables: Int = 1,
                        sizeMultiplier: Int = 2,
                        numNearestNeighbours: Int = 4
                      )(implicit spark: SparkSession) extends Log4jLogging {
  require(sample.count != 0, "sample must not be empty")
  require(numHashTables >= 1, "number of hash tables must be greater than or equals 1")
  require(sizeMultiplier >= 2, "size multiplier must be greater than or equals 2")
  require(numNearestNeighbours >= 1, "number of nearest neighbours must be greater than or equals 1")

  private val rand = new scala.util.Random

  private val featuresCol: String = "_smote_features"

  private val allAttributes: Seq[String] =
    discreteStringAttributes ++ discreteLongAttributes ++ continuousAttributes

  require(allAttributes.nonEmpty, "there must be at least one attribute")

  private val blen: Double = bucketLength match {
    case Some(x) =>
      require(x > 0, "bucket length must be greater than zero")
      x
    case _ => LocalitySensitiveHashing.bucketLength(sample.count, allAttributes.length)
  }

  private val stringIndexerOutputCols: Seq[String] = discreteStringAttributes.map { s =>
    s + "_indexed"
  }

  private val oneHotEncoderInputCols: Seq[String] = stringIndexerOutputCols ++ discreteLongAttributes

  private val oneHotEncoderOutputCols: Seq[String] =
    oneHotEncoderInputCols.map { s =>
      s + "_1hot"
    }

  private val assemblerInputCols: Seq[String] = oneHotEncoderOutputCols ++ continuousAttributes

  private val stringIndexers: Seq[StringIndexer] = {
    val res: ArrayBuffer[StringIndexer] = ArrayBuffer()
    for (d <- discreteStringAttributes) {
      res += new StringIndexer()
        .setInputCol(d)
        .setOutputCol(d + "_indexed")
        .setHandleInvalid("error")
    }
    res
  }

  private val oneHotEncoder: OneHotEncoderEstimator = new OneHotEncoderEstimator()
    .setInputCols(oneHotEncoderInputCols.toArray)
    .setOutputCols(oneHotEncoderOutputCols.toArray)
    .setHandleInvalid("error")


  private val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(assemblerInputCols.toArray)
    .setOutputCol(featuresCol)


  private val lsh: BucketedRandomProjectionLSH = new BucketedRandomProjectionLSH()
    .setInputCol(featuresCol)
    .setBucketLength(blen)
    .setNumHashTables(numHashTables)

  private def transform(): DataFrame = {
    val stages: Seq[PipelineStage] = stringIndexers ++ Seq(oneHotEncoder, assembler)
    val pipe = new Pipeline().setStages(stages.toArray)
    val model: ml.PipelineModel = pipe.fit(sample)
    model.transform(sample)
  }

  private def setContinuousAttribute(name: String, base: Row, neighbour: Row): Row = {
    val lc: Double = base.getAs[Double](name)
    val rc: Double = neighbour.getAs[Double](name)
    val diff: Double = rc - lc
    val gap: Double = rand.nextFloat()
    val newValue: Double = lc + (gap * diff)
    update(base, name, newValue)
  }

  private def setDiscreteAttribute[T](name: String, base: Row, neighbour: Row): Row = {
    val ld = base.getAs[T](name)
    val rd = neighbour.getAs[T](name)
    val newValue = rand.nextInt(2) match {
      case 0 => ld
      case _ => rd
    }
    update(base, name, newValue)
  }

  private def syntheticExample(base: Row, neighbour: Row): Row = {
    var res: Row = base
    for (c <- continuousAttributes) {
      res = setContinuousAttribute(c, res, neighbour)
    }
    for (d <- discreteStringAttributes) {
      res = setDiscreteAttribute[String](d, res, neighbour)
    }
    for (d <- discreteLongAttributes) {
      res = setDiscreteAttribute[Long](d, res, neighbour)
    }
    res
  }

  def syntheticSample(): DataFrame = {
    val t: DataFrame = transform()
    val model: BucketedRandomProjectionLSHModel = lsh.fit(t)
    // merge into 1 partition for less network cost in approxNearestNeighbors
    val lshDf: DataFrame = model.transform(t).coalesce(1)
    val schema = lshDf.schema
    log.info(s"lshDf.count=${lshDf.count}\nlshDf.schema=$schema")
    val rows: Array[Row] = lshDf.collect()
    val res: ArrayBuffer[Row] = ArrayBuffer()
    for (row <- rows) {
      val key: Vector = row.getAs[Vector](featuresCol)
      val knn: Array[Row] = model.approxNearestNeighbors(
        dataset = lshDf,
        key = key,
        numNearestNeighbors = numNearestNeighbours
      ).toDF().collect()
      for (_ <- 1 until sizeMultiplier) {
        val nn: Row = knn(rand.nextInt(knn.length))
        res += syntheticExample(row, nn)
      }
    }
    toDF(res.toArray, schema).selectExpr(allAttributes: _*)
  }

}