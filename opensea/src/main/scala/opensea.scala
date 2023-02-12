/*
About Dataset

This dataset contains sales data from the OpenSea NFT marketplace and is 
comprised of all successful sales from January 2019 through December 2021.
Data was pulled from the Events endpoint through the official OpenSea API.

Total_price is the value of the sale in 'wei' 
where 1 ether = 1,000,000,000,000,000,000 wei.

The Category column is a combination of web scraped categories based on 
the OpenSea sales pages as well as some hand done classifications.
This dataset was used to complete the paper "Characterizing the OpenSea 
NFT Marketplace" which was published as part of the Companion Proceedings 
of the Web Conference 2022.
We would very much appreciate a reference if this data proves to be useful 
for any further publications.

Full Reference:
Bryan White, Aniket Mahanti, and Kalpdrum Passi. 2022. Characterizing
the OpenSea NFT Marketplace. In Companion Proceedings of the Web
Conference 2022 (WWW '22). Association for Computing Machinery, New
York, NY, USA, 488–496. https://doi.org/10.1145/3487553.3524629

https://www.kaggle.com/datasets/bryanw26/opensea-nft-sales-2019-2021
*/

import scala.io.Source._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}

object App{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("AppName")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val sc = spark.sparkContext


    // Complete schema which splits data structures into data substructures
    // Note : the code will crash with this schema
    // val schema = StructType(Array(
    //   StructField(    "sales_datetimes",          DateType,     true),
    //   StructField(    "id",                       IntegerType,  true),
    //   StructField(    "asset",                    StructType(Array(
    //     StructField(  "id",                       IntegerType,  true),
    //     StructField(  "name",                     StringType,   true),
    //     StructField(  "collection",               StructType(Array(
    //       StructField("name",                     StringType,   true),
    //       StructField("short_description",        StringType,   true),
    //     ))),                                  
    //     StructField(  "permalink",                StringType,   true),
    //     StructField(  "num_sales",                IntegerType,  true),
    //   ))),                                    
    //   StructField(    "total_price",              StringType,   true),
    //   StructField(    "payement_token",           StructType(Array(
    //     StructField(  "name",                     StringType,   true),
    //     StructField(  "usd_price",                DoubleType,    true),
    //   ))),                                    
    //   StructField(    "seller",                   StructType(Array(
    //     StructField(  "address",                  StringType,   true),
    //     StructField(  "username",                 StringType,   true),
    //   ))),
    //   StructField(    "winner_account_username",  StringType,   true),
    //   StructField(    "Category",                 StringType,   true),
    // ))

    // Easier with this schema (flat schema)
    val schema = StructType(Array(
      StructField("sales_datetimes",                    DateType,    true),
      StructField("id",                                 IntegerType, true),
      StructField("asset_id",                           IntegerType, true),
      StructField("asset_name",                         StringType,  true),
      StructField("asset_collection_name",              StringType,  true),
      StructField("asset_collection_short_description", StringType,  true),
      StructField("asset_permalink",                    StringType,  true),
      StructField("total_price",                        StringType,  true),
      StructField("payment_token_name",                 StringType,  true),
      StructField("payment_token_usd_price",            DoubleType,   true),
      StructField("asset_num_sales",                    IntegerType, true),
      StructField("seller_address",                     StringType,  true),
      StructField("seller_user.username",               StringType,  true),
      StructField("winner_account.username",            StringType,  true),
      StructField("Category",                           StringType,  true),
    ))

    // Convert `total_price` from 'wei' to 'ether'
    val wei_to_ether = udf((number:String) => {
      try {
        val num = number.replace(",", "")
        val nb = "0" * (19 - num.length) ++ num
        (nb.slice(0, 1) ++ "." ++ nb.slice(1, nb.length)).toDouble
      } catch {
        case e : Exception => 0.toDouble
      }
    })

    val csvData: Dataset[String] = sc.textFile("OpenSea_NFT_Sales_2019_2021.csv").toDS()
    val frame = spark.read.
                      option("header", true).
                      option("interSchema", true).
                      option("dateFormat", "yyyy-MM-dd HH:mm:ss").
                      schema(schema).
                      csv(csvData).
                      withColumn("total_price", wei_to_ether(col("total_price"))).
                      cache()

    println("Schema of the dataframe")
    println("=======================")
    frame.printSchema()

    def flatstring(array: Array[Row], columns: Seq[String]): String = {
      return array.map(row => columns zip row.toSeq).
                  map(array => array.map(t => t._1 ++ s" = ${t._2}").mkString("\n")).
                  mkString("\n\n")
    }

    val columns = frame.schema.fields.map(e => e.name)
    println("columns = " ++ columns.mkString(", "))

    val five = flatstring(frame.rdd.take(5), columns.toSeq)
    println(five ++ "\n\n")

    val cframe = frame.filter(!(
                            col("Category").contains("0x") || 
                            col("Category") === "Uncategorized"
                            ) && col("total_price") =!= 0 &&
                            col("payment_token_usd_price") =!= 0
                          ).
                          withColumn("ether2usd", col("total_price") * col("payment_token_usd_price"))
    val cfive = flatstring(cframe.rdd.take(5), columns.toSeq)
    println(cfive ++ "\n\n")

    val categories = cframe.select("Category").distinct()
    println(s"Number of categories = ${categories.count}\n")

    val prices_per_category = cframe.select("Category", "ether2usd").
            groupBy("Category").
            agg(
              avg("ether2usd").as("avg"),
              max("ether2usd").as("max"),
              min("ether2usd").as("min")).
            orderBy("avg").
            tail(5)

    val results_ppc = flatstring(prices_per_category, Seq("Category", "avg", "max", "min"))
    println("Categories with the best average price")
    println("======================================")
    println(results_ppc ++ "\n\n")

    val max_sale = cframe.rdd.map(row => row.getAs[Double]("ether2usd")).max()
    val max_sale_array = cframe.filter(col("ether2usd") === max_sale).rdd.take(1)
    val result_max_sale = flatstring(max_sale_array, columns ++ Array("ether2usd"))
    println(result_max_sale ++ "\n\n")
  }
}
