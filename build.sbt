name := "cs434-project"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.1"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided

scalacOptions += "-Xasync"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)
