import scala.io.Source._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.{FloatType, StringType, DataType}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}

import org.apache.spark.sql.SparkSession


object App{

  def main(args: Array[String]):Unit = {
    // Set Spark parameters
    val spark = SparkSession.builder()
      .appName("AppName")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    val sc = spark.sparkContext

    // Download data
    val url: String = "https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv"
    val CONTI_FEATURES: Array[String] = Array("age", "fnlwgt", "capital-gain", "educational-num", "capital-loss", "hours-per-week")
    sc.addFile(url)

    var frame = spark.read.
                      option("header", true).
                      option("interSchema", true).
                      csv("file://" + SparkFiles.get("adult_data.csv")).
                      cache()

    // Visualization
    println("\nFrame columns of 'frame'")
    println("========================")
    frame.printSchema()

    println("See data")
    println("========")
    frame.show(5, truncate = false)

    def convertColumn(frame: DataFrame, names: Array[String], newType: DataType): DataFrame =
      return names.length match {
        case 0 => frame
        case _ => {
          val name = names(0)
          convertColumn(frame.withColumn(name, frame(name).cast(newType)), names.drop(1), newType)
        }
      }

    var frame_string = convertColumn(frame, CONTI_FEATURES, FloatType)

    println("\nFrame columns of 'frame_string'")
    println("===============================")
    frame.printSchema()

    println("Show 'age' and 'fnlwgt'")
    println("=======================")
    frame.select("age", "fnlwgt").show(5)

    println("Show education count")
    println("====================")
    frame.groupBy("education").count().sort(asc("count")).show()

    println("Describe 'frame'")
    println("================")
    frame.describe().show()


    println("Describe capital_gain in 'frame'")
    println("================================")
    frame.describe("capital-gain").show()

    // Preprocessing
    // Application of a transformation and add it in the DataFrame
    frame = frame.withColumn("age_square", col("age") * col("age"))

    // Checks if a group has an observation
    frame.where(frame("native-country") === "Holand-Netherlands").count()
    println("Check if every `native-country` has an observation")
    println("==================================================")
    frame.groupBy("native-country")
      .agg(Map("native-country" -> "count"))
      .sort(asc("count(native-country)"))
      .show()

    val frame_remove = frame.where(frame("native-country") =!= "Holand-Netherlands")

    // Pipeline of preprocessing
    val stringIndexer = new StringIndexer().setInputCol("workclass").
                                            setOutputCol("workclass_encoded")
    val pseudo_model = stringIndexer.fit(frame)
    val indexed = pseudo_model.transform(frame)
    val encoder = new OneHotEncoder().setInputCol("workclass_encoded").
                                      setOutputCol("workclassvec").
                                      setDropLast(false).
                                      fit(indexed)
    val encoded = encoder.transform(indexed)

    val CATE_FEATURES = Array(
      "workclass",
      "education",
      "marital-status",
      "occupation",
      "relationship",
      "race",
      "gender",
      "native-country"
    )

    val indexers = CATE_FEATURES.map(
      string => new StringIndexer().setInputCol(string).
                                    setOutputCol(string ++ "Index")
    )
    val encoders = CATE_FEATURES.map(
      string => new OneHotEncoder().setInputCol(string ++ "Index").
                                    setOutputCol(string ++ "classVec")
    ) 
    val input = CATE_FEATURES.map(string => string ++ "classVec") // ++ CONTI_FEATURES
    val vector_assembler = Array(
      new VectorAssembler().setInputCols(input).
                            setOutputCol("features")
    )

    val indexer_label = Array(
      new StringIndexer().setInputCol("income").
                          setOutputCol("labelIndex")
    )
    val encoder_label = Array(
      new OneHotEncoder().setInputCol("labelIndex").
                          setOutputCol("label")
    )

    // Creation of the Pipeline
    val pipeline = new Pipeline().setStages(
      indexers ++ encoders ++ vector_assembler ++ indexer_label ++ encoder_label
    )
    val pipelineModel = pipeline.fit(frame_remove)
    val model = pipelineModel.transform(frame_remove)

    // Classification
    val input_data = model.rdd.map(
      x => (
        x.getAs[SparseVector]("label").toDense(0),
        x.getAs[SparseVector]("features").toDense
      )
    )

    val frame_train = spark.createDataFrame(input_data).
                            toDF("label", "features")
    val splitted_data = frame_train.randomSplit(Array(0.8, 0.2), seed=1234)
    val train_data = splitted_data(0)
    val test_data = splitted_data(1)
    val lr = new LogisticRegression().setLabelCol("label").
                                      setFeaturesCol("features").
                                      setMaxIter(10).
                                      setRegParam(0.3)
    val linearModel = lr.fit(train_data)

    // Training and evaluation of the model
    val predictions = linearModel.transform(test_data)
    val selected = predictions.select("label", "prediction", "probability")
    selected.show(20)

    def accuracy(model: LogisticRegressionModel) = {
      val predictions = model.transform(test_data)
      val cm = predictions.select("label", "prediction")
      val acc = cm.filter(col("label") === cm("prediction")).count.asInstanceOf[Float] / cm.count.asInstanceOf[Float]
      println(s"Model accuracy : ${acc * 100}%")
    }

    accuracy(linearModel)
  }
}
