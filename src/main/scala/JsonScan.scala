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


import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType


class JsonScan(val schema : StructType, val options :  JsonOptions) extends Scan {

    override def readSchema(): StructType = {
        schema
    }

    override def description(): String = {
        "dsJSON"
    }

    

    override def toBatch(): Batch = {
        
        return new JsonBatch(schema, options)
    }
}