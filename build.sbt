ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  "Artima Maven Repository" at "https://repo.artima.com/releases",
  Resolver.mavenLocal
)

name := "money-enrichment"

version := "0.1-SNAPSHOT"

organization := "org.kindofdev"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.12.1"

addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.10")

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-avro" % flinkVersion,
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVersion,
  // testing
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" %% "flink-runtime" % flinkVersion % Test classifier "tests",
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion % Test classifier "tests",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Test classifier "tests",
  // queryable state
  "org.apache.flink" % "flink-core" % "1.12.0",
  "org.apache.flink" % "flink-queryable-state-client-java" % "1.12.0"
)

val avroDependencies = Seq(
  "org.apache.avro" % "avro" % "1.8.2",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4",
)

val jsonDependencies = Seq(
  "com.typesafe.play" %% "play-json" % "2.8.1",
  "org.julienrf" %% "play-json-derived-codecs" % "4.0.0",
)

val logDependencies = Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.7",
  "org.apache.logging.log4j" % "log4j-core" % "2.7",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.7"
)

libraryDependencies += "org.typelevel" %% "cats-core" % "2.3.1"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "com.vitorsvieira" %% "scala-iso" % "0.1.2"

val testDependencies = Seq(
  "org.scalamock" %% "scalamock" % "4.4.0" % Test,
  "org.scalactic" %% "scalactic" % "3.2.2",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test"
)


lazy val root = (project in file(".")).settings(
  libraryDependencies ++= flinkDependencies ++ avroDependencies ++ jsonDependencies ++ logDependencies ++ testDependencies
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

assembly / mainClass := Some("org.kindofdev.MoneyEnrichmentApp")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
