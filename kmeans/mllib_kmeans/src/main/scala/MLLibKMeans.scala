package com.queirozf.clustering

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions.{udf, col, max, min}

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
      System.err.println("Arguments: <json_features_dir> <output_dir> <k>")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val k = args(2).toInt

    val maxIterations = 10000
    val epsilon = 0.001

    val cnf = new SparkConf().setAppName("MLLibKMeans")
    val sc = new SparkContext(cnf)

    val sqlContext = new SQLContext(sc)

    // initializing the dataframe from json file
    // features have already been normalized
    val featuresDF = sqlContext.jsonFile(inputDir)

      // now we can start kmeans proper
    val vectorRDD = numericDataFrameToVectorRDD(featuresDF)

    vectorRDD.cache()

    val model = new KMeans()
      .setInitializationMode(KMeans.RANDOM)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setEpsilon(epsilon)
      .run(vectorRDD)

    // just for force and action, to make sure it isn't lazy
    val cost = model.computeCost(vectorRDD)

    sc.stop()

  }

}
