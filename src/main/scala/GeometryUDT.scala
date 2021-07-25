package org.apache.spark.beast.sql

import org.apache.spark.sql.types._

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeArrayData
}
import org.apache.spark.sql.types._

class GeometryUDT(isCollection: Boolean) extends UserDefinedType[Geometry] {

  val geometryFactory: GeometryFactory = new GeometryFactory(
    new PrecisionModel(PrecisionModel.FLOATING),
    4326,
    CoordinateArraySequenceFactory.instance()
  );

  override def sqlType: StructType = {
    if (isCollection) {
      StructType(
        Seq(
          StructField("type", StringType, nullable = false),
          StructField(
            "geometries",
            ArrayType(
              StructType(
                Seq(
                  StructField("type", StringType, nullable = false),
                  StructField("coordinates", StringType, nullable = true)
                )
              )
            ),
            nullable = true
          )
        )
      )
    } else {
      StructType(
        Seq(
          StructField("type", StringType, nullable = false),
          StructField("coordinates", StringType, nullable = true)
        )
      )
    }

  }

  override def serialize(obj: Geometry): InternalRow = {
    println("serializing")
    println(obj)
    val row = new GenericInternalRow(4)
    row
  }

  override def deserialize(datum: Any): Geometry = {
    datum match {
      case row: InternalRow =>
        println("deserializing")
        println(row)
        val geometryType: String = "point"
        // List<Coordinate> coordinates;
        // List<Geometry> parts;
        // Geometry jtsGeom;
        (new Point(
          (new Coordinate(0, 0)).asInstanceOf[CoordinateSequence],
          geometryFactory
        )).asInstanceOf[Geometry];
    }
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
