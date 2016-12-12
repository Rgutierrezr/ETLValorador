name := "ETLValorador"
version := "1.0"
scalaVersion := "2.10.4"

exportJars := true
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.1"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.9.0.1"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test"
libraryDependencies += "com.ibm.informix" % "jdbc" % "4.10.7.20160517"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.3"
libraryDependencies +=  "org.apache.spark" % "spark-streaming_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"
// new
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.4"
libraryDependencies += "com.typesafe.play" % "play-json_2.10" % "2.4.8"




