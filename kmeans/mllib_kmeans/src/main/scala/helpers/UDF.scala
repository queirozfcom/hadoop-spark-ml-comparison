package com.queirozf.clustering.helpers

import org.apache.spark.sql.functions._
import org.joda.time.{LocalTime, DateTime}

/**
 * Created by felipe on 13/09/15.
 */
object UDF {
  // returns the length of given text
  def stringLengthUDF = udf{ txt:String => txt.length.toDouble }

  // outputs 1.0 if given timestamp represents a weekday, 0.0 otherwise
  def timestampIsWeekDayUDF = udf{ unixTimestamp:Long =>

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
  def timestampIsWeekendUDF = udf{ unixTimestamp:Long =>

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
  def timestampIsAMUDF = udf{ unixTimestamp:Long =>

    val elevenFiftyNine = new LocalTime(11,59,59)
    val date = new DateTime(unixTimestamp * 1000L)

    date.toLocalTime.compareTo(elevenFiftyNine) match{
      case -1 => 1.0
      case 1 => 0.0
      case _ => 0.0 // just to make sure nothing gets through
    }
  }

  // outputs 1.0 if given timestamp represents a time after noon (PM), 0.0 otherwise
  def timestampIsPMUDF = udf{ unixTimestamp:Long =>

    val elevenFiftyNine = new LocalTime(11,59,59)
    val date = new DateTime(unixTimestamp * 1000L)

    date.toLocalTime.compareTo(elevenFiftyNine) match{
      case -1 => 0.0
      case 1 => 1.0
      case _ => 1.0 // just to make sure nothing gets through
    }
  }

  // given a pair of ints, return the first divided by the second
  def getRatioUDF = udf{ pair: Seq[Long] =>

    if( pair(1) == 0 ) 0.0
    else ( pair(0).toDouble/pair(1).toDouble )
  }

  // all values in the dataframe need to be normalized so that they fall within 0.0 and 1.0
  // otherwise values with larger absolute values will dominate all others
  def normalizerUDF(minValue:Double,maxValue:Double) = udf{ targetValue:Double =>
    if((maxValue - minValue) == 0.0) 0.0
    else (targetValue - minValue) / (maxValue - minValue)
  }
}
