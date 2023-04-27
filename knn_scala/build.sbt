ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "knn_scala",
    libraryDependencies ++= Seq(
      "org.locationtech.jts" % "jts-core" % "1.19.0",
      "org.postgresql" % "postgresql" % "42.6.0"
    )
  )
