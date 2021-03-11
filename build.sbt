name := "laba02-05"

version := "0.1"

scalaVersion := "2.11.12"

lazy val scriptClasspath = Seq("*")
//val json4sNative = "org.json4s" %% "json4s-native" % "{latestVersion}"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.7.0-M6"
libraryDependencies += "org.apache.spark" %%  "spark-core" % "2.4.6"
libraryDependencies += "org.apache.spark" %%  "spark-sql" % "2.4.6"
libraryDependencies += "org.apache.spark" %%  "spark-mllib" % "2.4.6"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"