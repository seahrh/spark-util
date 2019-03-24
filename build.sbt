lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.sgcharts",
      scalaVersion := "2.11.12",
      version      := "0.2.0"
    )),
    name := "spark-util",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % versions.scalatest % Test,
      "org.apache.spark" %% "spark-sql" % versions.spark % Provided,
      "org.apache.spark" %% "spark-hive" % versions.spark % Test,
      "ch.qos.logback" % "logback-classic" % versions.logback,
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % versions.scalaLogging,
      "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase % Test
    )
  )
lazy val versions = new {
  val scalatest = "3.0.5"
  val spark = "2.2.1"
  val logback = "1.2.3"
  val scalaLogging = "2.1.2"
  val sparkTestingBase = "2.2.0_0.8.0"
}
wartremoverErrors ++= Warts.allBut(
  Wart.ToString,
  Wart.Throw,
  Wart.DefaultArguments,
  Wart.Return,
  Wart.TraversableOps,
  Wart.ImplicitParameter,
  Wart.NonUnitStatements,
  Wart.Var,
  Wart.Overloading,
  Wart.MutableDataStructures,
  Wart.Nothing // in sc.parallelize
)
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
