name := "dsJSON"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("edu.ucr.cs.bdlab")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq( "org.locationtech.jts" % "jts-core" % "1.18.2")
libraryDependencies ++= Seq("org.locationtech.jts.io" % "jts-io-common" % "1.18.2")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.12" % "3.1.2")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-sql_2.12" % "3.1.2")

mainClass in assembly := Some("edu.ucr.cs.bdlab.Main")
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}