lazy val root = (project in file(".")).
  settings(
    name := "loaddvd",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.example.loaddvd")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2" % "provided",
  "com.typesafe" % "config" % "1.4.0",
  "org.postgresql" % "postgresql" % "42.2.16"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}