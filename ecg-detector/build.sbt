
//scalaVersion := "2.12.3"
scalaVersion := "2.11.8"

name := "ecg-detector"
organization := "fxo"
version := "1.0"

//libraryDependencies += "org.typelevel" %% "cats" % "0.9.0"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0" % "provided"

//https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0" % "test"
// Test seem to not work!!
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"

// https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/bigquery-connector
libraryDependencies += "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.3-hadoop2"


resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "datastax"  at "https://dl.bintray.com/spark-packages/maven/",
  Resolver.sonatypeRepo("public")
)

