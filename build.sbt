name := "kafka-streams-demo"

version := "0.1"

scalaVersion := "2.13.1"

avroScalaCustomTypes in Compile := {
  avrohugger.format.Standard.defaultTypes.copy(
    enum = avrohugger.types.ScalaCaseObjectEnum
  )
}

sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue

val thirdPartyRepos = Seq(
  "confluent-release" at "https://packages.confluent.io/maven/"
)
resolvers := (thirdPartyRepos ++: resolvers.value)

val confluentAvroVersion = "5.3.2"
val kafkaVersion = "2.4.0"

val testDependencies = Seq(
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion,

)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.0",
  "io.confluent" % "kafka-avro-serializer" % confluentAvroVersion,
  "io.confluent" % "kafka-streams-avro-serde" % confluentAvroVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.4",
) ++ testDependencies.map(_ % Test)

dependencyOverrides in ThisBuild ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion
)