# Environment setup

While we executed these experiments on a cluster. Here, I'll provide the instructions to setup the development environment, and the steps to be able to recompile the code and execute an example in a local environment.

## Setup Java

We compiled and tested the code usinng OpenJDK 1.8.

The output of `java -version` in the local development environemtn looks like this:
```
openjdk version "1.8.0_292"
OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_292-b10)
OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.292-b10, mixed mode)
```

The output in the cluster is:
```
java version "1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```

Also, make sure that the `$JAVA_HOME` environment variable is defined.

## Setup SBT

In Mac, I just use this command:

`brew install sbt`

You can follow instructions on the project website: `https://www.scala-sbt.org/`

The output of `sbt --version` is:
```
sbt version in this project: 1.5.8
sbt script version: 1.8.2
```

Having installed this part, it should be straight forward to compile the  code with this command:
```
sbt assembly
```

The compiler will produce this executable: `./target/scala-2.12/dsJSON-assembly-0.1.jar`

## Setup Spark Locally

On the cluster where we ran the experiments, we have this verison:
https://archive.apache.org/dist/spark/spark-3.1.2/

In a local environment, download this file: `spark-3.1.2-bin-hadoop3.2.tgz`

Download the file, and make sure to setup the envrionment variable `$SPARK_HOME` to point to this directory. Also, add $SPARK_HOME/bin to the path, so that spark commands can be found.

To test this setup, run this command `spark-submit --version` this should work showing the spark verison that was downloaded.

# Example local execution:

Download and decompress one of the datasets in:
https://drive.google.com/drive/folders/1aQURK9Vg1lMLx_drDIyWLIWm6kueHbpY

In a local environment, I recommend to download the smallest files, most other files are big for single machine execution.

Here is an example that counts the number of records in the products array in the BestBuy dataset:
Make sure the path to the `jar` and `.json` files are modified according to your environment. 

```
spark-submit --master local --conf spark.sql.files.maxPartitionBytes=1073741824 --conf spark.sql.files.minPartitionBytes=33554432 --class edu.ucr.cs.bdlab.Main ./target/scala-2.12/dsJSON-assembly-0.1.jar count local "./bestbuy_large_record.json" "$.products[*]" speculation ""
```

For the last experiment, the class `DataframeJoin` was used instead of `Main`.
# Scripts

The `experiments.py` is the script I use to run the experiments on the cluster. Although, I did some edits when I needed to make some edits when I needed to re-run some experiments, but all the experiments can be re-executed using this script with minor changes (e.g. uncommenting some lines, making sure paths are valid). This script is only for spark based code.

The other single machine tools are tested individually since they are only tested on one dataset. For example, in python it is simply `json.load` and then simply getting the size of the array or iterating it.

The file `output_extractor.py` extracts the required values from each log file produced by the `experiments.py` file. It extracts the total execution time and the exectuion time at each stage from the log files. The values it extracts I just copy them manually to the file `Charts.xlsx` which where the charts are produced.


The files `bestbuy-duplicate.py` and `bestbuy_converter.py` are used for pre-processing the BestBuy dataset depending on the experiment needs.

