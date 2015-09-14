package com.queirozf.clustering

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions.{udf, col, max, min}
import org.joda.time.{DateTime, LocalTime}

import helpers.UDF.{normalizerUDF, getRatioUDF, stringLengthUDF, timestampIsAMUDF, timestampIsPMUDF, timestampIsWeekDayUDF, timestampIsWeekendUDF}
import helpers.Rows.numericDataFrameToVectorRDD

/**
 * Script to clean our dataset.
 *
 * input dataset shape:
 *
 * |-- asin: string (nullable = true)
 * |-- helpful: array (nullable = true)
 * |    |-- element: long (containsNull = true)
 * |-- overall: double (nullable = true)
 * |-- reviewText: string (nullable = true)
 * |-- reviewTime: string (nullable = true)
 * |-- reviewerID: string (nullable = true)
 * |-- reviewerName: string (nullable = true)
 * |-- summary: string (nullable = true)
 * |-- unixReviewTime: long (nullable = true)
 *
 * after featurization, it looks like this
 *
 * |-- AM: double (nullable = true)
 * |-- PM: double (nullable = true)
 * |-- normPctHelpful: double (nullable = true)
 * |-- normReviewLength: double (nullable = true)
 * |-- normScore: double (nullable = true)
 * |-- weekDay: double (nullable = true)
 * |-- weekend: double (nullable = true)
 *
 * and then we run KMeans on it
 *
 *
 */
object MLLibKMeans {

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Arguments: <raw_amazon_features_dir> <output_dir> <k>")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val k = args(2).toInt
    val maxIterations = 1000
    val epsilon = 0.001

    val cnf = new SparkConf().setAppName("MLLibKMeans")
    val sc = new SparkContext(cnf)

    val sqlContext = new SQLContext(sc)

    // initializing the dataframe from json file
    val reviewsDF = sqlContext.jsonFile(inputDir)

    val schema = reviewsDF.schema

    // transform dataframe into RDD so that we can call filter
    // to remove any rows with Null values
    val cleanRDD = reviewsDF.rdd.filter { row: Row =>

      // // this caused an error in spark:
      // // val unixTimestampIndex = row.fieldIndex("unixReviewTime")
      // val unixTimestampIndex = 8
      // val tryLong = Try(row.getLong(unixTimestampIndex))

      row.anyNull == false
    }

    // then recreate the dataframe
    val cleanDF = sqlContext.createDataFrame(cleanRDD, schema)

    // transformations
    val featuresDF = cleanDF.select(
      col("overall").as("score"),
      stringLengthUDF(col("reviewText")).as("reviewLength"),
      timestampIsWeekDayUDF(col("unixReviewTime")).as("weekDay"),
      timestampIsWeekendUDF(col("unixReviewTime")).as("weekend"),
      timestampIsAMUDF(col("unixReviewTime")).as("AM"),
      timestampIsPMUDF(col("unixReviewTime")).as("PM"),
      getRatioUDF(col("helpful")).as("pctHelpful"))

    // only scoreGiven, reviewTextLength and percentHelpful columns
    // need to be normalized, because the others were given either 0.0
    // or 1.0 and are thus already normalized
    val maxValues = featuresDF.select(
      max(col("score")),
      max(col("reviewLength")),
      max(col("pctHelpful"))).first

    val maxScore = maxValues.getDouble(0)
    val maxReviewLength = maxValues.getDouble(1)
    val maxpercentHelpful = maxValues.getDouble(2)

    val minValues = featuresDF.select(
      min(col("score")),
      min(col("reviewLength")),
      min(col("pctHelpful"))).first

    val minScore = minValues.getDouble(0)
    val minReviewLength = minValues.getDouble(1)
    val minPercentHelpful = minValues.getDouble(2)

    // produce full dataframe
    val normalizedFeaturesDF = featuresDF.select(
      normalizerUDF(minScore, maxScore)(col("score")).as("normScore"),
      normalizerUDF(minReviewLength, maxReviewLength)(col("reviewLength")).as("normReviewLength"),
      col("weekDay"),
      col("weekend"),
      col("AM"),
      col("PM"),
      normalizerUDF(minPercentHelpful, maxpercentHelpful)(col("pctHelpful")).as("normPctHelpful"))

    // now we can start kmeans proper
    val vectorRDD = numericDataFrameToVectorRDD(normalizedFeaturesDF)

    vectorRDD.cache()

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setEpsilon(epsilon)
      .run(vectorRDD)

    val cost = model.computeCost(vectorRDD)

    println(s"Total cost is $cost")

    sc.stop()

  }

}
