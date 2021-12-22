/*
 * Copyright 2020 University of California, Riverside
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

package org.apache.spark.beast.sql

import org.apache.spark.sql.types._
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeArrayData
}
import org.apache.spark.sql.types._

class GeometryUDT() extends UserDefinedType[Geometry] {

  val geometryFactory: GeometryFactory = new GeometryFactory(
    new PrecisionModel(PrecisionModel.FLOATING),
    4326,
    CoordinateArraySequenceFactory.instance()
  );
  // the following two classes are not serializable
  var geoJsonReader : GeoJsonReader = null
  var geoJsonWriter : GeoJsonWriter = null
  var wktReader : WKTReader = null

  override def sqlType: DataType = {
      StringType
  }

  override def serialize(obj: Geometry): UTF8String = {
    if(geoJsonWriter == null)
    geoJsonWriter = new GeoJsonWriter(7)
    val result = geoJsonWriter.write(obj)
    return UTF8String.fromString(result)
  }

  override def deserialize(datum: Any): Geometry = {
    if(geoJsonReader == null)
    geoJsonReader = new GeoJsonReader(geometryFactory)
    val str = if(datum.isInstanceOf[UTF8String]) {datum.asInstanceOf[UTF8String].toString} else {datum.asInstanceOf[String]}
    val result : Geometry = geoJsonReader.read(str)
    return result
  }

  def deserializeWKT(datum: Any): Geometry = {
    if(wktReader == null)
    wktReader = new WKTReader(geometryFactory)
    val str = if(datum.isInstanceOf[UTF8String]) {datum.asInstanceOf[UTF8String].toString} else {datum.asInstanceOf[String]}
    val result : Geometry = wktReader.read(str)
    return result
  }
  override def pyUDT: String = "GeometryUDT"

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def equals(o: Any): Boolean = {
    o match {
      case v: GeometryUDT => true
      case _              => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[GeometryUDT].getName.hashCode()

  override def typeName: String = "geometry"

  override def asNullable: GeometryUDT = this

}