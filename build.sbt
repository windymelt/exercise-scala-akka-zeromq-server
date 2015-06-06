name := "Exercise Akka ZeroMQ"

version := "0.1-SNAPSHOT"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.2.5",
    "com.typesafe.akka" %% "akka-zeromq" % "2.2.5"
)

