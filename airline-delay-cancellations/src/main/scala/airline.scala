/*
About Dataset
Content

This dataset is similar to 2015 Flight Delays and Cancellations. This dataset aims to incorporate multi-year data from 2009 to 2018 to offer additional time series insights.

Acknowledgements

All data files are downloaded from OST website, which stores flights on-time performance from 1987 to present. 

https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018
*/

import scala.io.Source._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}

object App{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("App")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val sc = spark.sparkContext

    val schema = StructType(Array(
      StructField("FL_DATE",              DateType,     true),
      StructField("OP_CARRIER",           StringType,   true),
      StructField("OP_CARRIER_FL_NUM",    StringType,   true),
      StructField("ORIGIN",               StringType,   true),
      StructField("DEST",                 StringType,   true),
      StructField("CRS_DEP_TIME",         IntegerType,  true),
      StructField("DEP_TIME",             FloatType,    true),
      StructField("DEP_DELAY",            FloatType,    true),
      StructField("TAXI_OUT",             FloatType,    true),
      StructField("WHEELS_OFF",           FloatType,    true),
      StructField("WHEELS_ON",            FloatType,    true),
      StructField("TAXI_IN",              FloatType,    true),
      StructField("CRS_ARR_TIME",         IntegerType,  true),
      StructField("ARR_TIME",             FloatType,    true),
      StructField("ARR_DELAY",            FloatType,    true),
      StructField("CANCELLED",            FloatType,    true),
      StructField("CANCELLATION_CODE",    StringType,   true),
      StructField("DIVERTED",             StringType,   true),
      StructField("CRS_ELAPSED_TIME",     FloatType,    true),
      StructField("ACTUAL_ELAPSED_TIME",  FloatType,    true),
      StructField("AIR_TIME",             StringType,   true),
      StructField("DISTANCE",             StringType,   true),
      StructField("CARRIER_DELAY",        StringType,   true),
      StructField("WEATHER_DELAY",        StringType,   true),
      StructField("NAS_DELAY",            StringType,   true),
      StructField("SECURITY_DELAY",       StringType,   true),
      StructField("LATE_AIRCRAFT_DELAY",  StringType,   true),
      StructField("Unnamed",              StringType,   true),
    ))


    val csvData: Dataset[String] = sc.textFile("*.csv").toDS()
    val frame = spark.read.
                      option("header", true).
                      option("interSchema", true).
                      option("dateFormat", "yyyy-MM-dd").
                      schema(schema).
                      csv(csvData).
                      drop(col("Unnamed")).
                      filter(col("ARR_DELAY") > 0).
                      withColumn("DAY_WEEK", date_format(col("FL_DATE"), "EEEE")).
                      withColumn("MONTH", date_format(col("FL_DATE"), "MM")).
                      cache()

    val columns = frame.schema.fields.map(e => e.name)
    def flatstring(array: Array[Row], columns: Seq[String]): String = {
      return array.map(row => columns zip row.toSeq).
                  map(array => array.map(t => t._1 ++ s" = ${t._2}").mkString("\n")).
                  mkString("\n\n")
    }


    def concat(ac:Map[String, Float], v:Map[String, Float]):Map[String, Float] = {
      val k = v.keys.head
      return ac.updated(k, if(ac.contains(k)) {ac(k) + v(k)} else {v(k)})
    }

    // val five = flatstring(frame.rdd.take(5), columns)
    // println(five ++ "\n\n")

    val start1 = System.nanoTime
    val departure_delays = frame.groupBy("MONTH").agg(sum("DEP_DELAY"))
    departure_delays.show()
    val duration1 = (System.nanoTime - start1) / 1e9d
    
    // val rddstart1 = System.nanoTime
    // val rdd_departure_delays = frame.rdd.map(row => Map(
    //                               row.getAs[String]("MONTH") -> row.getAs[Float]("DEP_DELAY")
    //                             )).reduce(concat)

    // val rdd_departure_delays = frame.rdd.
    //                                 map(row => (
    //                                   row.getAs[String]("MONTH"), 
    //                                   row.getAs[Float]("DEP_DELAY")
    //                                 )).
    //                                 groupBy(t => t._1).
    //                                 aggregateByKey(0.0)(
    //                                   (ac, v) => ac + v.map(t => t._2).reduce((a, b) => a + b),
    //                                   (ac1, ac2) => ac1 + ac2
    //                                 )
    
    // println(s"${rdd_departure_delays.collect().map(_.toString).mkString(", ")}")
    // val rddduration1 = (System.nanoTime - rddstart1) / 1e9d
    // println(s"[RDD] Duration for departure delays computation = ${rddduration1} s")

    val start2 = System.nanoTime
    val arrival_delays = frame.groupBy("MONTH").agg(sum("ARR_DELAY"))
    arrival_delays.show()
    val duration2 = (System.nanoTime - start2) / 1e9d
    
 
    val start3 = System.nanoTime
    val delays_by_cities = frame.groupBy("ORIGIN").
                                agg(sum("ARR_DELAY").as("sum")).
                                orderBy("sum")
    delays_by_cities.show()
    val duration3 = (System.nanoTime - start3) / 1e9d

    val start4 = System.nanoTime
    val delays_by_airlines = frame.groupBy("OP_CARRIER").
                                  agg(sum("ARR_DELAY").as("sum")).
                                  orderBy("sum")
    delays_by_airlines.show()
    val duration4 = (System.nanoTime - start4) / 1e9d

    val start5 = System.nanoTime
    frame.groupBy("MONTH").agg(sum("DEP_DELAY")).show()
    frame.groupBy("MONTH").agg(sum("ARR_DELAY")).show()
    frame.groupBy("ORIGIN").
          agg(sum("ARR_DELAY").as("sum")).
          orderBy("sum").
          show()
    frame.groupBy("OP_CARRIER").
          agg(sum("ARR_DELAY").as("sum")).
          orderBy("sum").
          show()
    val duration5 = (System.nanoTime - start5) / 1e9d

    println(s"[DataFrame] Duration for departure delays computation = ${duration1} s")
    println(s"[DataFrame] Duration for arrival delays computation = ${duration2} s")
    println(s"[DataFrame] Duration for delays by cities computation = ${duration3} s")
    println(s"[DataFrame] Duration for delays by airlines computation = ${duration4} s")
    println(s"[DataFrame] Duration for all = ${duration5} s")
  }
}

