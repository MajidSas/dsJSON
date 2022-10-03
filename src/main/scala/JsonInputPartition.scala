/*
 * Copyright ....
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

import org.apache.spark.sql.connector.read.{InputPartition}
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
class JsonInputPartition(val path : String, val hdfsPath : String, val start : Long, val end : Long, val startLevel : Int, val dfaState : Int, val stateLevels : List[Int], val initialState : List[Char] = null, val id : Int = 0, val speculationAttribute:String="") extends InputPartition {

    // TODO: also add initial path (might be required in some cases for initialization [e.g. with multiple dscendent types])
    override def preferredLocations() : Array[String] = {
        var locations = Array[String]()
        val hadoopConf = if(hdfsPath == "local") {
            SparkContext.getOrCreate().hadoopConfiguration
        } else {
            val _conf = SparkContext.getOrCreate().hadoopConfiguration
            _conf.set("fs.defaultFS", hdfsPath)
            _conf
        }
        val fs = FileSystem.get(hadoopConf)
        val blockLocations = fs.getFileBlockLocations(new Path(path), start, end-start+1)
        var maxBlock = blockLocations(0)
        for(block <- blockLocations) {
            if(block.getLength() > maxBlock.getLength()) {
                locations = block.getHosts()
                maxBlock = block
            } else if(block.getLength() == maxBlock.getLength()) {
                locations = locations ++ block.getHosts();
            }
        }
        val loc : Array[String] = locations.map(s => s.toString)
        return loc
    }
}