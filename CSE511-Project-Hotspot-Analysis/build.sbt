import sbt.Keys.{libraryDependencies, scalaVersion, version}


lazy val root = (project in file(".")).
  settings(
    name := "CSE512-Hotspot-Analysis-Template",

    version := "0.1.0",

    scalaVersion := "2.12.17",

    organization  := "org.datasyslab",

    publishMavenStyle := true,

    mainClass := Some("cse512.Entrance")
  )

libraryDependencies ++= Seq(
  // Apache Spark dependencies updated to match Spark 3.4.4
  "org.apache.spark" %% "spark-core" % "3.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.4" % "provided",

  // Updated ScalaTest to a version compatible with Scala 2.12.17
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,

  // Updated Specs2 dependencies to versions compatible with Scala 2.12.17
  "org.specs2" %% "specs2-core" % "4.10.6" % Test,
  "org.specs2" %% "specs2-junit" % "4.10.6" % Test
)