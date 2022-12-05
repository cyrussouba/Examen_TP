ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "service_v1",
    idePackagePrefix := Some("suppression")

  )
