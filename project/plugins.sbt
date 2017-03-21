resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
