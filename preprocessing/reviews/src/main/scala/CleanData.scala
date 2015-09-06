import scala.util.Try

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext,Row}
import org.apache.spark.sql.functions.{udf,col,max,min}

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
        
        // initializing the dataframe from json file
        val reviewsDF = sqlContext.jsonFile(inputDir)

        val schema = reviewsDF.schema

        // transform dataframe into RDD so that we can call filter
        // to remove any rows with Null values
        val cleanRDD = reviewsDF.rdd.filter{row:Row => 

            // // this caused an error in spark:
            // // val unixTimestampIndex = row.fieldIndex("unixReviewTime")
            // val unixTimestampIndex = 8
            // val tryLong = Try(row.getLong(unixTimestampIndex))

            row.anyNull == false
        }

        // then recreate the dataframe
        val cleanDF = sqlContext.createDataFrame(cleanRDD,schema)

        // transformations
        val featuresDF = cleanDF.select(
            col("overall").as("scoreGiven"),
            stringLengthUDF(col("reviewText")).as("reviewTextLength"),
            timestampIsWeekDayUDF(col("unixReviewTime")).as("weekDay"),
            timestampIsWeekendUDF(col("unixReviewTime")).as("weekend"),
            timestampIsAMUDF(col("unixReviewTime")).as("AM"),
            timestampIsPMUDF(col("unixReviewTime")).as("PM"),
            getRatioUDF(col("helpful")).as("percentHelpful"))
            
        // only scoreGiven, reviewTextLength and percentHelpful columns
        // need to be normalized, because the others were given either 0.0
        // or 1.0 and are thus already normalized    
        val maxValues = featuresDF.select( 
            max(col("scoreGiven")),
            max(col("reviewTextLength")),
            max(col("percentHelpful"))).first

        val maxScore = maxValues.getDouble(0)
        val maxReviewTextLength = maxValues.getDouble(1)
        val maxpercentHelpful = maxValues.getDouble(2)

        val minValues = featuresDF.select(
            min(col("scoreGiven")),
            min(col("reviewTextLength")),
            min(col("percentHelpful"))).first

        val minScore = minValues.getDouble(0)
        val minReviewTextLength = minValues.getDouble(1)
        val minPercentHelpful = minValues.getDouble(2)

        // produce full dataframe
        val normalizedFeaturesDF = featuresDF.select(
            normalizerUDF(minScore,maxScore)(col("scoreGiven")),
            normalizerUDF(minReviewTextLength,maxReviewTextLength)(col("reviewTextLength")),
            col("weekDay"),
            col("weekend"),
            col("AM"),
            col("PM"),
            normalizerUDF(minPercentHelpful,maxpercentHelpful)(col("percentHelpful")))

        // save as JSON so we can use it again later    
        normalizedFeaturesDF.toJSON.saveAsTextFile(outputDir) 
        sc.stop()
        
    }

    /**********************************
    * UDFs: User-Defined Functions
    ***********************************/

    // returns the length of given text
    private val stringLengthUDF = udf{txt:String => 
        txt.length.toDouble
    }

    // outputs 1.0 if given timestamp represents a weekday, 0.0 otherwise
    private val timestampIsWeekDayUDF = udf{ unixTimestamp:Long =>

        val date = new DateTime(unixTimestamp * 1000L)
            
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
    private val timestampIsWeekendUDF = udf{ unixTimestamp:Long =>

        val date = new DateTime(unixTimestamp * 1000L)

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
    private val timestampIsAMUDF = udf{ unixTimestamp:Long =>

        val elevenFiftyNine = new LocalTime(11,59,59)
        val date = new DateTime(unixTimestamp * 1000L)

        date.toLocalTime.compareTo(elevenFiftyNine) match{
            case -1 => 1.0
            case 1 => 0.0
            case _ => 0.0 // just to make sure nothing gets through
        }
    }

    // outputs 1.0 if given timestamp represents a time after noon (PM), 0.0 otherwise
    private val timestampIsPMUDF = udf{ unixTimestamp:Long =>

        val elevenFiftyNine = new LocalTime(11,59,59)
        val date = new DateTime(unixTimestamp * 1000L)

        date.toLocalTime.compareTo(elevenFiftyNine) match{
            case -1 => 0.0
            case 1 => 1.0
            case _ => 1.0 // just to make sure nothing gets through
        }
    }

    // given a pair of ints, return the first divided by the second
    private val getRatioUDF = udf{ pair: Seq[Long] =>

        if( pair(1) == 0 ) 0.0
        else ( pair(0).toDouble/pair(1).toDouble )
    }

    // all values in the dataframe need to be normalized so that they fall within 0.0 and 1.0
    // otherwise values with larger absolute values will dominate all others
    private def normalizerUDF(minValue:Double,maxValue:Double) = udf{ targetValue:Double =>
        if((maxValue - minValue) == 0.0) 0.0
        else (targetValue - minValue) / (maxValue - minValue)    
    }

}
