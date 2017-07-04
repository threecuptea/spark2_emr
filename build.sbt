name := "spark2_emr"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-mllib"  % "2.1.1" % "provided"
)
        