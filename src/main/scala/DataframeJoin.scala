/*
 * Copyright ...
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucr.cs.bdlab
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataframeJoin {
  def main(args: Array[String]): Unit = {
//    val (count, hdfsPath, input, jsonPath, partitioningStrategy, sqlFilter) = (args(0), args(1), args(2), args(3), args(4), args(5))
//    val extraFields = if (args.length > 6 && args(6) == "extraFields" || args.length > 7 && args(7) == "extraFields") { true } else { false }
//    val keepIndex = if(args.length > 6 && args(6) == "keepIndex" || args.length > 7 && args(7) == "keepIndex" ) { true } else { false }
    val spark =
      SparkSession.builder().appName("dsJSON-join").getOrCreate()

    val pathGlobFilter = ""
    val recursive = false
//    val schemaBuilder = if(partitioningStrategy.equals("speculation")) {"start"} else { "fullPass" }
    val encoding = "UTF-8"
    val df_osm = JsonStream.load(
      "./all_objects.geojson",
//      "/Users/majid/Downloads/osm21_all_objects.json",
      pathGlobFilter,
      recursive,
//      "$.features[(@.geometry geometryWithin \"POLYGON ((-128.056641 24.926295, -59.589844 24.926295, -59.589844 49.781264, -128.056641 49.781264, -128.056641 24.926295))\")].geometry;$.features[(@.properties != null)].properties((@.tagsMap != null)).tagsMap((@.type StringStartsWith \"boundary\" && @.wikidata != null))",
      "$.features[(@.geometry geometryWithin \"POLYGON ((-128.056641 24.926295, -59.589844 24.926295, -59.589844 49.781264, -128.056641 49.781264, -128.056641 24.926295))\")].geometry;$.features[(@.properties != null)].properties((@.tagsMap != null)).tagsMap((@.wikidata != null && @.type StringStartsWith \"boundary\")).wikidata",
      "speculation",
      "fullPass",
      false,
      false,
      encoding,
      "hdfs://ec-hn.cs.ucr.edu:8040/"
    )

    df_osm.printSchema()
    df_osm.createOrReplaceTempView("osm")

//    df_osm.write.mode("overwrite").parquet("./datasets/joined_df.parquet")
//    println("OSM filtered count: " + spark.read.parquet("./datasets/joined_df.parquet").count().toString)

    val df_wiki = JsonStream.load(
            "./datasets/latest-all.json",
//            "/Users/majid/Documents/wikipedia_small.json",
      pathGlobFilter,
      recursive,
      "[*].id;[*].labels.en.value;[*].descriptions.en.value;[*].claims..mainsnak((@.datatype StringStartsWith \"wikibase-item\"))",
    "speculation",
      "speculation",
      false,
      false,
      encoding,
      "hdfs://ec-hn.cs.ucr.edu:8040/"
    )

    df_wiki.printSchema()
    df_wiki.createOrReplaceTempView("wiki")

    val joined_dfs = spark.sql("SELECT * FROM osm o, wiki w " +
      "WHERE o.properties.tagsMap.wikidata == w.id")
    joined_dfs.write.mode("overwrite").parquet("./datasets/joined_df.parquet")

  }
}