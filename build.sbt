import sbt.url

lazy val user = "seahrh"
lazy val artifact = "spark-util"
lazy val thisBuildSettings = Seq(
  organization := "com.sgcharts",
  scalaVersion := "2.11.12",
  version := "0.4.0"
)
lazy val publishSettings = Seq(
  homepage := Option(url(s"https://github.com/$user/$artifact")),
  licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  useGpg := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Option("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Option("releases" at nexus + "service/local/staging/deploy/maven2")
    }
  },
  scmInfo := Some(
    ScmInfo(
      browseUrl = url(s"https://github.com/$user/$artifact"),
      connection = s"scm:git:git@github.com:$user/$artifact.git"
    )
  ),
  developers := List(
    Developer(
      id = user,
      name = "Seah Ru Hong",
      email = "admin@sgcharts.com",
      url = url("https://www.sgcharts.com"))
  ),
  description := "Utility for common use cases and bug workarounds for Apache Spark 2"
)
lazy val versions = new {
  val scalatest = "3.0.7"
  val spark = "2.4.0"
  val sparkTestingBase = "2.4.0_0.11.0"
}
lazy val dependencies = Seq(
  "org.scalatest" %% "scalatest" % versions.scalatest % Test,
  "org.apache.spark" %% "spark-sql" % versions.spark % Provided,
  "org.apache.spark" %% "spark-hive" % versions.spark % Test,
  "org.apache.spark" %% "spark-mllib" % versions.spark % Provided,
  "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase % Test
)
lazy val root = (project in file(".")).
  settings(
    inThisBuild(thisBuildSettings),
    name := artifact,
    libraryDependencies ++= dependencies
  )
  .settings(publishSettings: _*)
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
  Wart.Nothing, // in sc.parallelize
  Wart.Equals, // else != is disabled
  Wart.While
)
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
