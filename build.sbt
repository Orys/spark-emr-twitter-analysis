name := "spark-emr-twitter-analysis"

version := "0.1"

scalaVersion := "2.11.12"


val sparkVersion = "2.4.5"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4",
  // "edu.stanford.nlp" % "stanford-corenlp" % "3.4" classifier "models",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models-english",
  "edu.stanford.nlp" % "stanford-parser" % "3.4"

  // logging
  //"org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  //"org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  //"org.postgresql" % "postgresql" % postgresVersion

)
