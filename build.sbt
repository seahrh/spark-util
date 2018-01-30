lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.sgcharts",
      scalaVersion := "2.11.12",
      version      := "1.0.0-SNAPSHOT"
    )),
    name := "spark-util",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % versions.scalatest % Test,
      "org.apache.spark" %% "spark-sql" % versions.spark % "provided",
      "ch.qos.logback" % "logback-classic" % versions.logback,
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % versions.scalaLogging
    )
  )
lazy val versions = new {
  val scalatest = "3.0.5"
  val spark = "2.2.1"
  val logback = "1.2.3"
  val scalaLogging = "2.1.2"
}
wartremoverErrors ++= Warts.allBut(
  Wart.ToString,
  Wart.Throw,
  Wart.DefaultArguments,
  Wart.Return,
  Wart.TraversableOps,
  Wart.ImplicitParameter,
  Wart.NonUnitStatements,
  Wart.Var
)
