scalaVersion := "2.11.7"

resolvers ++= Seq(
  "repo1" at "http://repo1.maven.org/maven2/",
  "jboss-repo" at "http://repository.jboss.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.6.4",
  "org.slf4j" % "slf4j-log4j12" % "1.6.4"
)

libraryDependencies +=
  "org.apache.zookeeper" % "zookeeper" % "3.3.4"