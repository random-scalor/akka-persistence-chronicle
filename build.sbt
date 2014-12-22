/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
import MultiJvmKeys.MultiJvm
import scalariform.formatter.preferences._

organization := "com.carrotgarden.akka"
name := "akka-persistence-chronicle"

scalaVersion := "2.11.5"
crossScalaVersions := Seq("2.10.4", "2.11.5")

val javaVersion = "1.6"
val akkaVersion = "2.4-SNAPSHOT"
val chroncleMapVersion = "2.1.3"
val chroncleQueueVersion = "3.4.1"

//

resolvers += "TypeSafe Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/"
resolvers += "SonaType Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "net.openhft" % "chronicle" % chroncleQueueVersion % "compile"
libraryDependencies += "net.openhft" % "chronicle-map" % chroncleMapVersion % "compile"
libraryDependencies += "com.thoughtworks.xstream" % "xstream" % "1.4.7"
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion % "compile" exclude("org.iq80.leveldb","leveldb") exclude("org.fusesource.leveldbjni","leveldbjni-all")

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion % "compile,optional"
libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % akkaVersion % "compile,optional"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion  % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental-tck" % akkaVersion % "test"

libraryDependencies += "org.scala-lang.modules" %% "scala-pickling" % "0.10.0-M2" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"

libraryDependencies += "junit" % "junit" % "4.12" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.3" % "test"
libraryDependencies += "org.specs2" %% "specs2-core" % "2.4.15" % "test"
libraryDependencies += "org.specs2" %% "specs2-junit" % "2.4.15" % "test"

libraryDependencies += "commons-io" % "commons-io"  % "2.4" % "test" 

//

multiJvmSettings

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(PreserveDanglingCloseParenthesis, true)
  
javaOptions in MultiJvm := Seq(
    "-Xms256M",
    "-Xmx256M"
)
    
scalacOptions in compile ++= Seq(
    "-encoding", "UTF-8",
    "-source", javaVersion, 
    "-target", javaVersion, 
    "-target:jvm-" + javaVersion,
    "-unchecked",
    "-deprecation",
    "-language:_",
    "-Xlint"
)

scalacOptions in doc ++= Seq(
    "-encoding", "UTF-8",
    "-source", javaVersion,
    "-groups",
    "-implicits",
    s"-external-urls:scala=http://www.scala-lang.org/api/${scalaVersion.value}/,akka=http://doc.akka.io/api/akka/${akkaVersion}/"        
) 

autoAPIMappings := true
    
parallelExecution in Test := false

compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test)

executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
  case (testResults, multiNodeResults)  =>
    val overall =
      if (testResults.overall.id < multiNodeResults.overall.id)
        multiNodeResults.overall
      else
        testResults.overall
    Tests.Output(overall,
      testResults.events ++ multiNodeResults.events,
      testResults.summaries ++ multiNodeResults.summaries)
}
    
pomExtra := (
    <url>https://github.com/carrot-garden/akka-persistence-chronicle</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/carrot-garden/akka-persistence-chronicle</url>
      <connection>scm:git:git@github.com:carrot-garden/akka-persistence-chronicle.git</connection>
    </scm>
    <developers>
      <developer>
        <id>andrei-pozolotin</id>
        <name>Andrei Pozolotin</name>
        <url>https://github.com/andrei-pozolotin</url>
      </developer>
    </developers>)


publishTo <<= version { (value: String) =>
    val sonatype = "https://oss.sonatype.org/"
    if (value.contains("-SNAPSHOT")) 
        Some("snapshots" at sonatype + "content/repositories/snapshots")
    else 
        Some("releases" at sonatype + "service/local/staging/deploy/maven2")
}

val root = Project("root", file("."))
    .configs(MultiJvm)
    .settings(Defaults.itSettings:_*)
