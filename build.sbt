name := "Advanced Analytics with Spark"

version := "0.0.1"

scalaVersion := "2.11.8"

spName := "com.github.huajianmao/aaspark"
sparkVersion := "2.1.0"
sparkComponents ++= Seq("mllib", "sql")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  // "org.apache.spark" %% "spark-sql" % "2.1.0",
  // "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test",
  "org.scoverage" %% "scalac-scoverage-runtime" % "1.3.0" % "test",

  // "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1",
  "io.spray" %% "spray-json" % "1.3.3"
)

parallelExecution in Test := false

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

