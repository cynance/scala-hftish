name := "alpaca-scala-hft"

version := "0.1"

scalaVersion := "2.12.8"

val circeVersion = "0.10.0"


libraryDependencies ++= Seq(
  "com.github.oueasley" %% "alpaca-scala" % "0.2",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.typesafe.akka" %% "akka-http"   % "10.1.7",
  "com.typesafe.akka" %% "akka-stream" % "2.5.19", // or whatever the latest version is,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)