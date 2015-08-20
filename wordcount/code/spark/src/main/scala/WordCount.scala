import scala.math.random
import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount{
	
	def main(args:Array[String]){

		val input_dir = "s3://bdnc2/input"
		val output_dir = "s3://bdnc2/output_spark"	

		val cnf      = new SparkConf().setAppName("Spark WordCount")
		val sc       = new SparkContext(cnf)
	    val textFile = sc.textFile(input_dir)
		val counts   = textFile.flatMap(line => line.split("\\+")) 
                       .map(word => (word, 1)) 
                       .reduceByKey( (a,b) => a+b )

		counts.saveAsTextFile(output_dir)
		sc.stop()
	}
}
