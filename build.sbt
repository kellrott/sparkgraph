import AssemblyKeys._ // put this at the top of the file

name := "SparkGremlin"

version := "1.0"

scalaVersion := "2.9.3"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "0.8.0-incubating",
	"com.tinkerpop.blueprints" % "blueprints-core" % "2.4.0",
	"com.tinkerpop.blueprints" % "blueprints-test" % "2.4.0" % "test",
	"com.novocode" % "junit-interface" % "0.9" % "test"
)	



resolvers ++= Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)


ivyXML := <dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518"><artifact name="javax.servlet" type="orbit" ext="jar"/></dependency>


assemblySettings


assembleArtifact in packageScala := false

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "w3c", xs @ _*) => MergeStrategy.first
    case "about.html"     => MergeStrategy.discard
    case "log4j.properties"     => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

test in assembly := {}


