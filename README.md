# Example usage

## Example one with full pass partitioner and schema builder
`spark-submit --master local --class edu.ucr.cs.bdlab.Main ./target/scala-2.12/json-stream-assembly-0.1.jar "./input/*.json" "\$.products[*]" "fullPass" "price > 20"`

## Example one with speculating partitioner and efficient schema builder
`spark-submit --master local --class edu.ucr.cs.bdlab.Main ./target/scala-2.12/json-stream-assembly-0.1.jar "./input/*.json" "\$.products[*]" "speculation" "SQL_FILTER_HERE_OR_EMPTY_STRING"`


## Example usage in code
First, make sure the classes are importable by linking the JAR file.

```
val spark =
      SparkSession.builder().appName("spark-json-reader").getOrCreate()
val df = spark.read
      .format("edu.ucr.cs.bdlab.JsonSource") // name of the class or "dsJSON" if the class is registerd
      .option("jsonPath", jsonPath)
      .option("partitioningStrategy", "speculation") // or "fullPass"
      .option("schemaBuilder", "speculation") // or "fullPass" in the paper (it is optimistic vs. pessimistic, will be udpated here)
      .option("encoding", encoding) // optional: default is UTF8
      .option("hdfsPath", hdfsPath) // root path for HDFS
      .option("pushdown_option", pushdown_option) // the option to include pushDown "projection_filters" to push both
      .load("path_to_data.json")
```
Also, refer to the file Main.scala for a full example.

# Compilation
Just use `sbt assembly`
sbt version in this project: 1.4.3
This code was developed and tested for Spark 3.1, Java 1.8, Scala 2.12.
Refer to: `build.sbt` for all dependencies.

# Package structure and flow
* `Main.scala`: prints the schema and implements a simple counting program
* `JsonSource.scala`: the first class that is called when creating the data source, it has the `inferSchema` and `getTable` function.
    * `inferSchema`: calls methods in the file `SchemaInference.scala` based on the chosen schema inference type, it also creates the partitions by calling methods in the `Partitioning.scala` in case of `fullPass` inference.
* `JsonTable.scala`: it creates a new ScanBuilder using the provided or inferred schema. Also, the fullPass partitioning is executed here and the results are stored for later use. The method `createPartitions` in the file `JsonBatch.scala`. This method is called twice by Spark for some reason. Therefore, it is only used to load and return the pre-computed partitions.
* `JsonScanBuilder.scala`: in this file the schema is replaced with the required schema (only the columns used in projection and filtering), and it also loads the list of query filters.
* `JsonScan.scala`: in this instance it just creates a new `JsonBatch` object.
* `JsonBatch.scala`: the only method of relevance is the createPartitions method, but it only loads the already created partitions.
* `JsonPartitionReader.scala`: the initialization starts by shifting the start and end index for the case of speculative partitioning. Then, it initializes the syntax stack (corresponding to the depth in the file), and also sets the state of the DFA.
* `Parser.scala`: contains all the functions used for parsing and working with input files.
* `FilterProcessor.scala`: contains all the classes and predicates for buliding the expression tree and evaluating ghe filters.
