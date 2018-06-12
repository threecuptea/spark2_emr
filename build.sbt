name := "spark2_emr"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.0"
val sparkTestVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib"  % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % sparkTestVersion % "test"
)
        