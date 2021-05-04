import sbt.Keys.{libraryDependencies, parallelExecution, scalaVersion}
import sbt.file

name := "flink_benchmark"

version := "0.1"

scalaVersion := "2.11.11"

val flinkVersion = "1.9.1"

lazy val flinkbenchmark = project
  .in(file("."))
  .settings(
    libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
    libraryDependencies +="org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
    javaOptions ++= Seq("-XX:+CMSClassUnloadingEnabled", "-Xm2G", "-Xms2G", "-Xss16M"),
  )

assembly / fullClasspath := (Compile / fullClasspath).value