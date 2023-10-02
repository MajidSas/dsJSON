name := "SparkJsonReader"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("edu.ucr.cs.bdlab.sparkjsonreader")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq( "org.locationtech.jts" % "jts-core" % "1.18.1")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.12" % "3.1.2")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-sql_2.12" % "3.1.2")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-sql_2.12" % "3.1.2")
libraryDependencies ++= Seq( "com.jayway.jsonpath" % "json-path" % "2.6.0")

mainClass in assembly := Some("edu.ucr.cs.bdlab.sparkjsonreader.Main")
assemblyJarName in assembly := "spark_jayway.jar"
mergeStrategy in assembly := { 
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case _ => MergeStrategy.first 
}