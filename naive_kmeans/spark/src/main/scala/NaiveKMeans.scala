// linear algebra library
import breeze.linalg.{Vector, DenseVector, squaredDistance}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext,Row}
import org.apache.spark.sql.functions.{udf,col,max,min}

import scala.util.Random

/**
 * Naive distributed K-means clustering.
 * 
 * input dataset is a normalized feature vector, in JSON format,
 * each sample having the following attributes:
 *
 * |-- AM: double (nullable = true)
 * |-- PM: double (nullable = true)
 * |-- normPctHelpful: double (nullable = true)
 * |-- normReviewLength: double (nullable = true)
 * |-- normScore: double (nullable = true)
 * |-- weekDay: double (nullable = true)
 * |-- weekend: double (nullable = true)
 *
 *
 */
object NaiveKMeans{

    // turns a Row of Doubles into a DenseVector of Doubles
  def parseVector(row: Row): Vector[Double] = {
    val doubles:Seq[Double] = for(i <- 0 to row.length-1) yield row.getDouble(i)
    DenseVector(doubles.toArray)
  }

  // function to discover which cluster (as measured by their 
  // centers) our sample is closest to
  def closestCluster(sample: Vector[Double], centers: Array[Vector[Double]]): Int = {
    
    var bestIndex = 0
    // to make sure this is larger than anything else
    var closest = Double.PositiveInfinity

    // test distance to each center
    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(sample, centers(i))
      
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {

    if(args.length < 1){
        System.err.println("Please set arguments for <s3_input_dir> <s3_output_dir> <numclusters>")
        System.exit(1)
    }

    val rand       = Random
    val inputDir   = args(0)
    val outputDir  = args(1)
    val k          = args(2).toInt

    val convergeDist = 0.001
    val seed = rand.nextInt

    val maxIters = 100

    val cnf        = new SparkConf().setAppName("Naive distributed K-means implementation using Apache Spark")
    val sc         = new SparkContext(cnf)

    val sqlContext = new SQLContext(sc)

    val featuresDF = sqlContext.jsonFile(inputDir)

    // val featuresRDD = featuresDF.rdd.cache()
    val featuresRDD = featuresDF.rdd

    // these are the starting centers
    // it's an Array of centers
    val kPoints:Array[Vector[Double]] = featuresRDD.map(parseVector _).takeSample(withReplacement = false, num=k , seed=seed).toArray

    // start with a large number for the delta
    var delta = 1.0

    var currIter = 0

    // iterate until the centers haven't moved more than convergeDist
    while(delta > convergeDist && currIter <= maxIters) {
      
      // RDD[(clusterIndex,(sample,1))]
      val closestClustersRDD = featuresRDD
                                .map{sample =>
                                  parseVector(sample)
                                }.map{ vect =>
                                  (closestCluster(vect, kPoints), (vect, 1))
                                }

      // aggregate by clusterIndex
      val pointCounts     = closestClustersRDD.reduceByKey{ case ((p1, c1), (p2, c2)) => 
        (p1 + p2, c1 + c2) 
      }

      val newPoints = pointCounts.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      delta = 0.0

      // how much the centers have moved
      for (i <- 0 until k) {
        delta += squaredDistance(kPoints(i), newPoints(i))
      }

      // setting the new centers
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }

      println(s"Finished iteration $currIter, (delta = $delta) ")
      currIter = currIter + 1
    }

    // final centers
    kPoints.foreach(println)
    sc.stop()
  }
}
