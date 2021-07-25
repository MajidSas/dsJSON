# Example usage

## Example one with full pass partitioner and schema builder
`spark-submit --master local --class edu.ucr.cs.bdlab.Main ./target/scala-2.12/json-stream-assembly-0.1.jar "./input/*.json" "\$.products[*]" "fullPass"`

## Example one with speculating partitioner and efficient schema builder
`spark-submit --master local --class edu.ucr.cs.bdlab.Main ./target/scala-2.12/json-stream-assembly-0.1.jar "./input/*.json" "\$.products[*]" "speculation"`

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

# Remaining components:
* Compiling filters in a JsonPath query
* Combining SQL filters with the JsonPath filters into a single expression tree
* Modify the parse() function to take into account the requiredSchema and the filtering expression.
* Implement serialization and de-serialization for the geometry type