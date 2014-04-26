import AssemblyKeys._

organization := "cc.nlplab"

name := "page-rank"

version := "0.1-cdh4"

scalaVersion := "2.10.4"


resolvers += "repo.codahale.com" at "http://repo.codahale.com"


// libraryDependencies += "com.codahale" % "logula_2.9.1" % "2.1.3"
seq(bintrayResolverSettings:_*)


libraryDependencies += "ooyala.scamr" % "scamr_2.10" % "0.3.1-cdh4"

libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.0.1"

// libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies <+= (version) {
    case v if v.contains("cdh3") => "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided"
  case v if v.contains("cdh4") => "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.4.0" % "provided"
  case v if v.contains("cdh5") => "org.apache.hadoop" % "hadoop-client" % "2.2.0-cdh5.0.0-beta-1" % "provided"
}

javaOptions ++= Seq("-XX:MaxPermSize=1024m", "-Xmx2048m")

seq(sbtassembly.Plugin.assemblySettings: _*)

org.scalastyle.sbt.ScalastylePlugin.Settings


