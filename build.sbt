name := "Test"

version := "1.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.0.2"
)