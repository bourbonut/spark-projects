import scala.io.Source._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window
import java.security.MessageDigest

object App{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("App")
      .config("spark.executors.memory", "20G")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val sc = spark.sparkContext

    val products_schema = StructType(Array(
      StructField("product_id",       StringType,   true), // BinaryType
      StructField("product_name",     StringType,   true),
      StructField("price",            StringType,   true), // BinaryType
    ))

    val sales_schema = StructType(Array(
      StructField("order_id",         StringType,   true), // BinaryType
      StructField("product_id",       StringType,   true), // BinaryType
      StructField("seller_id",        StringType,   true), // BinaryType
      StructField("date",             StringType,   true),
      StructField("num_pieces_sold",  StringType,   true), // BinaryType
      StructField("bill_raw_text",    StringType,   true),
    ))

    val sellers_schema = StructType(Array(
      StructField("seller_id",        StringType,   true), // BinaryType
      StructField("seller_name",      StringType,   true),
      StructField("daily_target",     StringType,   true), // BinaryType
    ))

    val products = spark.read.
                        option("header", true).
                        option("interSchema", true).
                        schema(products_schema).
                        parquet("products_parquet/*")

    val sales = spark.read.
                        option("header", true).
                        option("interSchema", true).
                        schema(sales_schema).
                        option("dateFormat", "yyyy-MM-dd").
                        parquet("sales_parquet/*")

    val sellers = spark.read.
                        option("header", true).
                        option("interSchema", true).
                        schema(sellers_schema).
                        parquet("sellers_parquet/*")

    def flatstring(array: Array[Row], columns: Seq[String]): String = {
      return array.map(row => columns zip row.toSeq).
                  map(array => array.map(t => t._1 ++ s" = ${t._2}").mkString("\n")).
                  mkString("\n\n")
    }
    
    def information(frame: DataFrame, name: String) = {
      println("Schema of `" ++ name ++ "`")
      frame.printSchema()
      val columns = frame.schema.fields.map(e => e.name)
      val one = flatstring(frame.rdd.take(1), columns)
      println("One element of `" ++ name ++ "`:\n\n" ++ one ++ "\n\n")
    }

    // Products information
    information(products, "products")

    // Sales information
    information(sales, "sales")

    // Sellers information
    information(sellers, "sellers")

    /*
    Find out how many orders, how many products and 
    how many sellers are in the data ?
    How many products have been sold at least once? 
    Which is the product contained in more orders?
    */
    def how_many(frame: DataFrame, name: String): String = {
      val count = frame.count.toString
      "Number of elements in `" ++ name ++ "`: " ++ count
    }
    println(how_many(products, "products"))
    println(how_many(sales, "sales"))
    println(how_many(sellers, "sellers"))

    println("\nProduct sold at least once :")
    sales.agg(countDistinct(col("product_id"))).show()

    /*
    How many distinct products have been sold in each day?
    */
    println("\nDistinct products sold in each day :")
    sales.groupBy("date").agg(countDistinct("product_id").as("count")).orderBy("count").show()

    /*
    What is the average revenue of the orders ?
    */
    println("\nAverage revenue of the orders :")
    sales.join(products, sales("product_id") === products("product_id"), "inner").
          agg(avg(products("price") * sales("num_pieces_sold"))).
          show()

    /*
    For each seller, what is the average % contribution of an order 
    to the seller's daily quota?
    # Example
    If Seller_0 with `quota=250` has 3 orders:
    Order 1: 10 products sold
    Order 2: 8 products sold
    Order 3: 7 products sold
    The average % contribution of orders to the seller's quota would be:
    Order 1: 10/250 = 0.04
    Order 2: 8/250 = 0.032
    Order 3: 7/250 = 0.028
    Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
    */
    
    println("\nAverage contribution to the seller's daily quota :")
    sales.join(sellers, sales("seller_id") === sellers("seller_id"), "inner").
          withColumn("ratio", sales("num_pieces_sold") / sellers("daily_target")).
          groupBy(sales("seller_id")).
          agg(avg("ratio")).
          show()

    /*
    Who are the second most selling and the least selling persons (sellers) 
    for each product? Who are those for product with `product_id = 0`
    */
    val products_and_sellers = sales.groupBy(sales("seller_id"), sales("product_id")).
          agg(sum("num_pieces_sold").alias("total"))

    val window_desc = Window.partitionBy("product_id").orderBy(desc("total"))
    val window_asc = Window.partitionBy("product_id").orderBy(asc("total"))

    val rank_table = products_and_sellers.withColumn("rank_asc", row_number.over(window_asc))
                                        .withColumn("rank_desc", row_number.over(window_desc))

    val single_seller = rank_table.where(col("rank_asc") === col("rank_desc")).
      select(
        col("product_id").alias("single_seller_product_id"),
        col("seller_id").alias("single_seller_seller_id"),
        lit("Only seller or multiple sellers with the same results").alias("type")
      )

    val second_seller = rank_table.where(col("rank_desc") === 2).
      select(
        col("product_id").alias("second_seller_product_id"),
        col("seller_id").alias("second_seller_seller_id"),
        lit("Second top seller").alias("type")
      )
    
    val least_seller = rank_table.where(col("rank_asc") === 1)
      .select(
        col("product_id"), 
        col("seller_id"),
        lit("Least Seller").alias("type")
      ).join(
        single_seller,
        (rank_table("seller_id") === single_seller("single_seller_seller_id")) &&
        (rank_table("product_id") === single_seller("single_seller_product_id")),
        "left_anti"
      ).join(
        second_seller,
        (rank_table("seller_id") === second_seller("second_seller_seller_id")) &&
        (rank_table("product_id") === second_seller("second_seller_product_id")),
        "left_anti"
      )

    // Union all the tables
    val union_table = least_seller.select(
        col("product_id"),
        col("seller_id"),
        col("type")
    ).union(second_seller.select(
        col("second_seller_product_id").alias("product_id"),
        col("second_seller_seller_id").alias("seller_id"),
        col("type")
    )).union(single_seller.select(
        col("single_seller_product_id").alias("product_id"),
        col("single_seller_seller_id").alias("seller_id"),
        col("type")
    ))
    // union_table.show()

    println("Which are the second top seller and least seller of product 0 ?")
    union_table.where(col("product_id") === 0).show()

    /*
    Create a new column called "hashed_bill" defined as follows:

      - if the order_id is even: apply MD5 hashing iteratively 
        to the bill_raw_text field, once for each 'A' (capital 'A') 
        present in the text. E.g. if the bill text is 'nbAAnllA', 
        you would apply hashing three times iteratively 
        (only if the order number is even)
      - if the order_id is odd: apply SHA256 hashing to the bill text

    Finally, check if there are any duplicate on the new column 
    */
  
    def encode(s:String): Array[Byte] = {
      return java.util.Base64.getEncoder.encode(s.getBytes())
    }

    def decode(bytes:Array[Byte]): String = {
      return new String(java.util.Base64.getDecoder.decode(bytes))
    }

    def unhash_ntimes(s:String, n:Int): String = {
      if (n == 0) {
        return s
      }
      else {
        return unhash_ntimes(
          // The following code doesn't work
          // decode(MessageDigest.getInstance("MD5").digest(s.getBytes)),
          String.format("%02x", 
            new java.math.BigInteger(1, 
              java.security.MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")
          ))),
          n - 1
        )
      }
    }

    def unhashed_md5(order_id: String, bill_text: String): String = {
      if (order_id.toInt % 2 == 0) {
        val count_capital_A = bill_text.count(_ == 'A')
        return unhash_ntimes(bill_text, count_capital_A)
      }
      println(bill_text)
      // return decode(MessageDigest.getInstance("SHA-256").digest(bill_text.getBytes))
      return String.format("%064x", 
        new java.math.BigInteger(1, 
          java.security.MessageDigest.getInstance("SHA-256").digest(bill_text.getBytes("UTF-8")
      ))) 
    }
  
    val unhash_udf = udf(unhashed_md5(_, _))
    println(sales.rdd.map(row => unhashed_md5(
                        row.getAs[String]("order_id"),
                        row.getAs[String]("bill_raw_text")
                      )).collect().mkString("\n"))
    // sales.withColumn("hashed_bill", unhash_udf(col("order_id"), col("bill_raw_text"))).
    //       groupBy(col("hashed_bill")).
    //       agg(count("*").alias("cnt")).
    //       where(col("cnt") > 1).
    //       show(5)
  }
}
