all:
	sbt package
	spark-submit --class App --master local[4] target/scala-2.12/simple-project_2.12-1.0.jar
