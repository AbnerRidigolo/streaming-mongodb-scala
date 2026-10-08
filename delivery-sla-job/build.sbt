// Delivery SLA job: Spark Structured Streaming in Scala (typed Datasets and
// flatMapGroupsWithState). Spark itself is "provided": the runtime image
// (apache/spark:3.4.1) ships it; the assembly JAR carries everything else.

ThisBuild / scalaVersion := "2.12.18" // Spark 3.4.1 is built for Scala 2.12
ThisBuild / organization := "br.com.olist"

val sparkVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "delivery-sla-job",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "io.delta" %% "delta-core" % "2.4.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1",
      // The connector asks for [5.1.1,5.1.99); pin it for reproducible builds.
      "org.mongodb" % "mongodb-driver-sync" % "5.1.4",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint"),
    // Spark tests in a forked JVM: own heap, no sbt classloader surprises.
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xmx2g", "-Dspark.ui.enabled=false"),
    Test / parallelExecution := false,
    // The producers' .avsc files are the single source of truth for the input
    // schemas; tests encode records with them.
    Test / unmanagedResourceDirectories += baseDirectory.value / ".." / "producers" / "schemas",
    assembly / assemblyJarName := "delivery-sla-job.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      // Keep every DataSourceRegister (kafka, delta, mongodb) and the like.
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
  )
