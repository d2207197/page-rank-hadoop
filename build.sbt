import AssemblyKeys._

organization := "cc.nlplab"

name := "page-rank"

version := "0.1-cdh3"

scalaVersion := "2.10.4"


resolvers += "repo.codahale.com" at "http://repo.codahale.com"


// libraryDependencies += "com.codahale" % "logula_2.9.1" % "2.1.3"
seq(bintrayResolverSettings:_*)


// libraryDependencies += "ooyala.scamr" % "scamr_2.10" % "0.3.1.1-cdh3"
libraryDependencies += "ooyala.scamr" % "scamr_2.10" % "0.3.2-cdh3-SNAPSHOT"

// libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.0.1"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.94.18"



libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"


libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.2"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2"



// libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

// libraryDependencies <+= (version) {
//     case v if v.contains("cdh3") => "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided"
//   case v if v.contains("cdh4") => "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.4.0" % "provided"
//   case v if v.contains("cdh5") => "org.apache.hadoop" % "hadoop-client" % "2.2.0-cdh5.0.0-beta-1" % "provided"
// }

javaOptions ++= Seq("-XX:MaxPermSize=1024m", "-Xmx2048m")

seq(sbtassembly.Plugin.assemblySettings: _*)

org.scalastyle.sbt.ScalastylePlugin.Settings



net.virtualvoid.sbt.graph.Plugin.graphSettings


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
    case PathList( "META-INF", "maven", "joda-time", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
    case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.first
    case x => old(x)
  }
}

// excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
//   cp filter {_.data.getName == "jsp-api-2.1-6.1.14.jar"}
// }
 

