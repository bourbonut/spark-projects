import scala.io.Source._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, FloatType}
import org.apache.spark.sql.SparkSession

object App{

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("AppName")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val sc = spark.sparkContext

    // Read csv
    val csvData: Dataset[String] = sc.textFile("CLIWOC15.csv").toDS()
    // val schema = new StructType().add("Month", IntegerType).
    //                               add("Year", IntegerType).
    //                               add("ProbTair", FloatType).
    //                               add("Nationality", StringType).
    //                               add("VoyageFrom", StringType).
    //                               add("VoyageTo", StringType)
    // or :
    // val schema = StructType(
    //   Array(
    //     StructField("Month", IntegerType, true),
    //     StructField("Year", IntegerType, true),
    //     StructField("ProbTair", FloatType, true),
    //     StructField("Nationality", StringType, true),
    //     StructField("VoyageFrom", StringType, true),
    //     StructField("VoyageTo", StringType, true)
    //   )
    // )
    // var frame = spark.read.
    //                   option("header", true).
    //                   option("nullValue", "null").
    //                   schema(schema).
    //                   csv(csvData)
    // Not working because the size of the schema is not equal to the number of columns !

    val frame = spark.read.
                      option("header", true).
                      option("interSchema", true).
                      csv(csvData).
                      select(
                        col("Month").cast(IntegerType),
                        col("Year").cast(IntegerType),
                        col("ProbTair").cast(FloatType),
                        col("Nationality"),
                        col("VoyageFrom"),
                        col("VoyageTo")
                      ).
                      cache()

    // Make British unique
    val unique = frame.withColumn("Nationality", when($"Nationality" === "British ", "British").otherwise($"Nationality"))

    // Count the number of observations
    val observations = unique.count
    println(s"Number of observations = ${observations}")

    // Count the number of years
    val years_count = unique.select("Year").distinct().count
    println(s"Number of years = ${years_count}")

    // Oldest and newest years
    println("Oldest and newest years")
    unique.select(max("Year"), min("Year")).show()

    // Minimum and maximum number of observations of years
    val frame_year_count = unique.groupBy("Year").agg(count("Year").as("count")).orderBy("count")
    println(s"[Year, min(count)] = ${frame_year_count.first()}")
    println(s"[Year, max(count)] = ${frame_year_count.tail(1)(0)}")

    // Distinct departures places with distinct
    val start1 = System.nanoTime
    val fromdistinct = unique.select("VoyageFrom").rdd.distinct().count
    val duration_distinct = (System.nanoTime - start1) / 1e9d
    println(s"Duration of distinct = ${duration_distinct} (with result = ${fromdistinct})")

    // Distinct departures places with reduce
    val start2 = System.nanoTime
    val fromreduce = unique.select("VoyageFrom").rdd.map(a => Set(a(0).asInstanceOf[String])).reduce((ac, v) => ac ++ v).size
    val duration_reduce = (System.nanoTime - start2) / 1e9d
    println(s"Duration of reduce = ${duration_reduce} (with result = ${fromreduce})")

    // Top 10 most popular departure places
    val top_departure = unique.groupBy("VoyageFrom").agg(count("VoyageFrom").as("count")).orderBy("count")
    val names = top_departure.tail(10).map(obj => obj(0).toString).reduce((ac, v) => ac ++ ", " ++ v)
    println("Top 10 most popular departure places = " ++ names)

    // Top 10 most taken roads (A-B and B-A distinct)
    // u = unprocessed
    val str = (a: Any) => a.asInstanceOf[String]
    val values_u = unique.select("VoyageFrom", "VoyageTo").
                          rdd.
                          filter(a => (a(0) != a(1))).
                          map(a => str(a(0)) ++ "-" ++ str(a(1))).
                          groupBy(identity).
                          map{case (k, v) => (k, v.count(_ => true))}.
                          sortBy(-_._2).
                          take(10)
    val quick_format = (a : Array[(String, Int)]) => a.map(_._1).reduce((ac, v) => ac ++ ", " ++ v)
    println("Top 10 most taken roads (A-B and B-A distinct) = " ++ quick_format(values_u))

    // Top 10 most taken roads (A-B and B-A not distinct)
    // p = processed
    val order = (a: String, b:String) => if (a < b) (a ++ "-" ++ b) else (b ++ "-" ++ a)
    val values_p = unique.select("VoyageFrom", "VoyageTo").
                          rdd.
                          filter(a => (a(0) != a(1))).
                          map(a => order(str(a(0)), str(a(1)))).
                          groupBy(identity).map{case (k, v) => (k, v.count(_ => true))}.
                          sortBy(-_._2).
                          take(10)

    println("Top 10 most taken roads (A-B and B-A not distinct) = " ++ quick_format(values_p))

    // Get the hottest month
    val hottest_month = frame.select("Month", "Year", "ProbTair").
                                filter($"ProbTair".isNotNull).
                                toDF.
                                groupBy("Month", "Year").
                                agg(avg("ProbTair").as("avg")).
                                groupBy("Month").
                                agg(avg("avg").as("avg")).
                                orderBy("avg").
                                tail(1)

    println(s"The hottest month is ${hottest_month(0)(0)}")
  }
}
