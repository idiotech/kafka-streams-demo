name := "kafka-streams-demo"

scalaVersion := "2.13.1"

val thirdPartyRepos = Seq(
  "confluent-release" at "https://packages.confluent.io/maven/",
  "jitpack" at "https://jitpack.io"
)
resolvers in ThisBuild := (thirdPartyRepos ++ resolvers.value)

val confluentAvroVersion = "5.4.2"
val kafkaVersion = "2.4.1"

val testDependencies = Seq(
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion,
)

libraryDependencies in ThisBuild ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.0",
  "com.github.idiotech" % "kafka-streams-schema-helper" % "34f653cdba",
) ++ testDependencies.map(_ % Test)

def project(projectName: String) = Project(projectName, new File(projectName)).settings(
  name := projectName,
  version := "0.1"
)

val hackagram = project("hackagram")