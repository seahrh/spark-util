package com.sgcharts.sparkutil

import org.apache.spark.ml
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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

  private implicit val rand: Random = new Random

  private val featuresCol: String = "_smote_features"

  private val allAttributes: Seq[String] =
    discreteStringAttributes ++ discreteLongAttributes ++ continuousAttributes

  require(allAttributes.nonEmpty, "there must be at least one attribute")

  private val outSchema: StructType = StructType(
    discreteStringAttributes.map(x => StructField(x, StringType, nullable = true))
      ++ discreteLongAttributes.map(x => StructField(x, LongType, nullable = false))
      ++ continuousAttributes.map(x => StructField(x, DoubleType, nullable = false))
  )

  implicit private val encoder: ExpressionEncoder[Row] = RowEncoder(outSchema)

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

  private val preprocessed: DataFrame = {
    val stages: Seq[PipelineStage] = stringIndexers ++ Seq(oneHotEncoder, assembler)
    val pipe = new Pipeline().setStages(stages.toArray)
    val model: ml.PipelineModel = pipe.fit(sample)
    model.transform(sample)
  }

  private def syntheticExample(base: Row, neighbour: Row): Row = {
    var res: Row = base
    for (c <- continuousAttributes) {
      res = Smote.setContinuousAttribute(c, res, neighbour)
    }
    for (d <- discreteStringAttributes) {
      res = Smote.setDiscreteAttribute[String](d, res, neighbour)
    }
    for (d <- discreteLongAttributes) {
      res = Smote.setDiscreteAttribute[Long](d, res, neighbour)
    }
    res
  }

  private def nearestNeighbours(keyColumnPrefix: String): DataFrame = {
    val model: BucketedRandomProjectionLSHModel = lsh.fit(preprocessed)
    val lshDf: DataFrame = model.transform(preprocessed)
    log.trace(s"lshDf.count=${lshDf.count}\nlshDf.schema=${lshDf.schema}")
    val distCol: String = "_smote_distance"
    val keyCols: String = allAttributes map (x => s"$keyColumnPrefix$x") mkString ","
    val columnIndices: String = Seq.range(1, 2 * allAttributes.length + 1) mkString ","
    val collectListCols: String = allAttributes.map(x => s"$x) $x").mkString(
      "COLLECT_LIST(", ",COLLECT_LIST(", "")
    val datasetACols: String = allAttributes map (x => s"datasetA.$x $keyColumnPrefix$x") mkString ","
    val datasetBCols: String = allAttributes map (x => s"datasetB.$x $x") mkString ","
    val viewName: String = "_smote_similarity_matrix_raw"
    val sql: String =
      s"""
         |select $keyCols
         |,$collectListCols
         |from (
         |select $keyCols
         |,${allAttributes mkString ","}
         |,ROW_NUMBER() OVER (PARTITION BY $keyCols ORDER BY $distCol) as rank
         |from (
         |select $datasetACols
         |,$datasetBCols
         |,avg($distCol) $distCol
         |from $viewName
         |group by $columnIndices
         |) t1 ) t2
         |where rank<=$numNearestNeighbours
         |group by $keyCols
       """.stripMargin
    log.trace(sql)
    model.approxSimilarityJoin(lshDf, lshDf, threshold = Double.MaxValue, distCol = distCol)
      .createOrReplaceTempView(viewName)
    spark.sql(sql)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def neighbour(row: Row): Row = {
    val i: Int = allAttributes.headOption match {
      case Some(a) => rand.nextInt(row.getAs[Seq[Any]](a).length)
      case _ => 0
    }
    val vs: ArrayBuffer[Any] = ArrayBuffer()
    for (a <- discreteStringAttributes) {
      vs += row.getAs[Seq[String]](a)(i)
    }
    for (a <- discreteLongAttributes) {
      vs += row.getAs[Seq[Long]](a)(i)
    }
    for (a <- continuousAttributes) {
      vs += row.getAs[Seq[Double]](a)(i)
    }
    new GenericRowWithSchema(vs.toArray, outSchema)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def key(row: Row, keyColumnPrefix: String): Row = {
    val vs: ArrayBuffer[Any] = ArrayBuffer()
    for (a <- discreteStringAttributes) {
      vs += row.getAs[String](s"$keyColumnPrefix$a")
    }
    for (a <- discreteLongAttributes) {
      vs += row.getAs[Long](s"$keyColumnPrefix$a")
    }
    for (a <- continuousAttributes) {
      vs += row.getAs[Double](s"$keyColumnPrefix$a")
    }
    new GenericRowWithSchema(vs.toArray, outSchema)
  }

  def syntheticSample: DataFrame = {
    val keyColumnPrefix: String = "key_"
    val knn: DataFrame = nearestNeighbours(keyColumnPrefix)
    knn flatMap { row =>
      val arr: ArrayBuffer[Row] = ArrayBuffer()
      for (_ <- 0 until sizeMultiplier) {
        arr += syntheticExample(
          base = key(row, keyColumnPrefix),
          neighbour = neighbour(row)
        )
      }
      arr
    }
  }.selectExpr(allAttributes: _*)
}

object Smote {
  private[sparkutil] def setContinuousAttribute(name: String, base: Row, neighbour: Row)
                                               (implicit rand: Random): Row = {
    val lc: Double = base.getAs[Double](name)
    val rc: Double = neighbour.getAs[Double](name)
    val diff: Double = rc - lc
    val gap: Double = rand.nextDouble()
    val newValue: Double = lc + (gap * diff)
    update(base, name, newValue)
  }

  private[sparkutil] def setDiscreteAttribute[T](name: String, base: Row, neighbour: Row)
                                                (implicit rand: Random): Row = {
    val ld = base.getAs[T](name)
    val rd = neighbour.getAs[T](name)
    val newValue = rand.nextInt(2) match {
      case 0 => ld
      case _ => rd
    }
    update(base, name, newValue)
  }
}