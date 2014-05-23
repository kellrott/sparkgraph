import AssemblyKeys._ // put this at the top of the file

organization := "sparkgraph"

name := "sparkgraph-gremlin"

version := "0.1"

scalaVersion := "2.10.0"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.0.0",
	"org.apache.spark" %% "spark-graphx" % "1.0.0",
	"com.tinkerpop.blueprints" % "blueprints-core" % "2.4.0",
	"com.tinkerpop.blueprints" % "blueprints-test" % "2.4.0" % "test",
	"com.tinkerpop" % "pipes" % "2.4.0",
	"com.tinkerpop.gremlin" % "gremlin-java" % "2.4.0",
	//"com.twitter" % "parquet-avro" % "1.3.2",
	"com.twitter" % "parquet-avro" % "1.4.3",
	"org.apache.avro" % "avro" % "1.7.4",
	"com.tinkerpop.gremlin" % "gremlin-test" % "2.4.0" % "test",
	"org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
	"com.novocode" % "junit-interface" % "0.9" % "test"
)	

resolvers ++= Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

assemblySettings

assembleArtifact in packageScala := false

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "w3c", xs @ _*) => MergeStrategy.first
    case "about.html"     => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "log4j.properties"     => MergeStrategy.concat
    //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}


test in assembly := {}


