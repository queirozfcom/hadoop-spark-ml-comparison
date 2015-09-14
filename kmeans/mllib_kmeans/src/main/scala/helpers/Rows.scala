package com.queirozf.clustering.helpers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * Created by felipe on 13/09/15.
 */
object Rows {

  def numericDataFrameToVectorRDD(df:DataFrame):RDD[Vector] = {
    df.rdd.map{ row =>
      Vectors.dense(row.toString.stripPrefix("[").stripSuffix("]").split(',').map(_.toDouble))
    }
  }

}
