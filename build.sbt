ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ppzbmedxml"
  )

val mongoVersion = "4.8.1"
val xmlVersion = "2.1.0"
val logging = "3.9.5"
val logback = "1.4.5"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % xmlVersion,
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion,
  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % logging,
)