all:
	sbt package
	spark-submit --class App --master local[*] --num-executors 1 --driver-memory 4g target/scala-2.12/simple-project_2.12-1.0.jar
