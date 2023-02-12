name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.15"

val spark_version ="3.3.1"

libraryDependencies ++= (
    Seq(
        "org.apache.spark" %% "spark-core" % spark_version,
        "org.apache.spark" %% "spark-sql" % spark_version,
        "org.apache.spark" %% "spark-mllib" % spark_version,
    )
)
