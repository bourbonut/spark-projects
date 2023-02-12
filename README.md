# Spark Projects

There are currently three projects with Spark written in Scala where the goal is to manipulate the syntax of Scala and learn how Spark works :
- Full Machine Learning Process (read data, visualize data, manipulate data and build a pipeline to train a model)
- Oceans (read data and manipulate data to extract information)
- OpenSea NFT (read data and manipulate data through this large dataset)

## Dependencies

Exclusively Spark and Scala

To install Scala on Linux, you can run the following command from the [official site](https://www.scala-lang.org/download/):

```sh
curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup
```

Check the [official site](https://www.scala-lang.org/download/) for other OS.

To install Spark on Linux, there are several steps to follow :

1. Go on the [Spark Website](https://spark.apache.org/downloads.html)
2. Download the Apache zipped file :

```sh
wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
```

3. Extract the zipped file :

```sh
tar -xzf spark-3.3.1-bin-hadoop3.tgz
mv spark-3.3.1-bin-hadoop3 spark
```

Note : `spark` should be located in your home directory

4. Add paths to your `~/.bashrc`:

```bash
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$SPARK_HOME/bin
```

## Run projects

You can execute the code by running the command `make` in the directory of one of the projects.
