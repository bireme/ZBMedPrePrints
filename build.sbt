ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ppzbmedxml"
  )

val mongoVersion = "4.7.2"
val xmlVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,
  "org.scala-lang.modules" %% "scala-xml" % xmlVersion,
)
