name := "nasa_http_requests"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion

)