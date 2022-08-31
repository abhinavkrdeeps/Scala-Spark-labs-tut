name := "scala-labs"

version := "0.1"

scalaVersion := "2.13.0"

idePackagePrefix := Some("com.abhinav.practice.scala")

val sparkVersion = "3.0.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "io.delta" %% "delta-core" % "2.0.0"


