import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{udf,col}

import org.joda.time.{DateTime,LocalTime}

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
*
*/
object CleanData{

	def main(args:Array[String]){

		if(args.length < 1){
			System.err.println("Please set arguments for <s3_input_dir> <s3_output_dir>")
			System.exit(1)
		}

		val inputDir  = args(0)
		val outputDir = args(1)	

		val cnf      = new SparkConf().setAppName("Cleaning and Featurizing Amazon Review Data")
		val sc       = new SparkContext(cnf)
		
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

	    // initializing the dataframe from json file
	    val reviewsDF = sqlContext.jsonFile(inputDir)

	    // transformations
	    val reviewsV2DF = reviewsDF.select(col("overall").as("scoreGiven"),
	    	stringLengthUDF(col("reviewText")).as("reviewTextLength"),
	    	timestampIsWeekDayUDF(col("unixReviewTime")).as("weekDay"),
	    	timestampIsWeekendUDF(col("unixReviewTime")).as("weekend"),
	    	timestampIsAMUDF(col("unixReviewTime")).as("AM"),
	    	timestampIsPMUDF(col("unixReviewTime")).as("PM"),
	    	getRatioUDF(col("overall")).as("percentHelpful"))
		

		reviewsV2DF.write.save(outputDir)	
		sc.stop()
		
	}

	// returns the length of given text
	private val stringLengthUDF = udf{txt:String => 
		txt.length.toDouble
	}

	// outputs 1.0 if given timestamp represents a weekday, 0.0 otherwise
	private val timestampIsWeekDayUDF = udf{ unixTimestamp:String =>
	    val date = new DateTime(unixTimestamp.toLong * 1000L)
	   	date.getDayOfWeek match{
	   		case 1 => 1.0
	   		case 2 => 1.0
	   		case 3 => 1.0
	   		case 4 => 1.0
	   		case 5 => 1.0
	   		case 6 => 0.0
	   		case 7 => 0.0
	   	}
	}

	// outputs 1.0 if given timestamp represents weekend, 0.0 otherwise
	private val timestampIsWeekendUDF = udf{ unixTimestamp:String =>
	    val date = new DateTime(unixTimestamp.toLong * 1000L)
	   	date.getDayOfWeek match{
	   		case 1 => 0.0
	   		case 2 => 0.0
	   		case 3 => 0.0
	   		case 4 => 0.0
	   		case 5 => 0.0
	   		case 6 => 1.0
	   		case 7 => 1.0
	   	}
	}

	// outputs 1.0 if given timestamp represents a time before noon (AM), 0.0 otherwise
	private val timestampIsAMUDF = udf{ unixTimestamp:String =>
		val elevenFiftyNine = new LocalTime(11,59,59)
		val date = new DateTime(unixTimestamp.toLong * 1000L)

		date.toLocalTime.compareTo(elevenFiftyNine) match{
			case -1 => 1.0
			case 1 => 0.0
			case _ => 0.0 // just to make sure nothing gets through
		}

	}

	// outputs 1.0 if given timestamp represents a time after noon (PM), 0.0 otherwise
	private val timestampIsPMUDF = udf{ unixTimestamp:String =>
		val elevenFiftyNine = new LocalTime(11,59,59)
		val date = new DateTime(unixTimestamp.toLong * 1000L)

		date.toLocalTime.compareTo(elevenFiftyNine) match{
			case -1 => 0.0
			case 1 => 1.0
			case _ => 1.0 // just to make sure nothing gets through
		}
	}

	// given a pair of ints, return the first divided by the second
	private val getRatioUDF = udf{ pair: Array[Long] =>
		pair(0)/pair(1)
	}

}
