ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ppzbmedxml",
    Test / parallelExecution := false,
    Test / fork := true
  )

val xmlVersion = "2.1.0"
val mongoVersion = "4.9.0"
val logback = "1.4.7"
val logging = "3.9.5"
val scalatest = "3.2.15"
val gsonVersion = "2.10.1"
val jsonOrgVersion = "20220924"


libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % xmlVersion,
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,
  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % logging,
  "org.scalatest" %% "scalatest" % scalatest % "test",
  "com.google.code.gson" % "gson" % gsonVersion,
)