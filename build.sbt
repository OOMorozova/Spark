name := "Spark"

version := "0.1"

scalaVersion := "2.13.8"


lazy val sparkVersion = "3.2.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val circeVersion = "0.14.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

//SettingKey[Option[String]]("ide-package-prefix") := Option("com.example")
