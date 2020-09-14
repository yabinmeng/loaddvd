lazy val root = (project in file(".")).
  settings(
    name := "loaddvd",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.example.loaddvd")
  )

resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"
val dseVersion = "6.8.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",

  // NOTE:
  // - for "spark-cassandra-connector" version 2.4.x and below, it does NOT have all
  //       DSE specific features as provided in "dse-spark-dependencies".
  // - for "spark-cassandra-connector" version 2.5.x, it does have all
  //       DSE specific features as provided in "dse-spark-dependencies".
  //"com.datastax.dse" % "dse-spark-dependencies" % dseVersion % "provided",
  //"com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1" % "provided",

  "com.typesafe" % "config" % "1.4.0",
  "org.postgresql" % "postgresql" % "42.2.16"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}