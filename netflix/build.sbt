name := "netflix"

version := "0.0.1"

scalaVersion := "2.12.12"

libraryDependencies +="org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies +="org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies +="com.typesafe" % "config" % "1.4.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.22"

