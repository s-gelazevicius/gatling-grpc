// the enterprisePackage task is confused
ThisBuild / Gatling / publishArtifact := false
ThisBuild / GatlingIt / publishArtifact := false

val commonSettings = Seq(
  organization := "com.github.phisgr",
  scalaVersion := "2.13.16",
  crossPaths := false,
)

val gatlingVersion = "3.14.3"
val gatlingCore = "io.gatling" % "gatling-core" % gatlingVersion

val grpcVersion = "1.66.0"

lazy val root = (project in file("."))
  .enablePlugins(GatlingPlugin)
  .settings(commonSettings *)
  .settings(
    name := "gatling-grpc",
    version := "0.18.0",
    inConfig(Test)(sbtprotoc.ProtocPlugin.protobufConfigSettings),
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value,
      PB.gens.java -> (Test / sourceManaged).value,
    ),
    scalacOptions ++= Seq(
      "-language:existentials",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-Xlint",
      "-opt:l:method",
    ),
    libraryDependencies ++= Seq(
      "io.grpc" % "grpc-netty" % grpcVersion,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      gatlingCore,
      "com.github.phisgr" % "gatling-ext" % "0.5.0",
      "io.gatling.highcharts" % "gatling-charts-highcharts" % gatlingVersion % "test",
      "io.gatling" % "gatling-test-framework" % gatlingVersion % "test",
      "org.scalatest" %% "scalatest" % "3.2.12" % "test",
    ),
  )
  .dependsOn(macroSub % "compile-internal")

lazy val macroSub = (project in file("macro"))
  .settings(commonSettings *)
  .settings(
    name := "macro",
    libraryDependencies ++= Seq(
      gatlingCore,
    ),
    scalacOptions ++= Seq(
      "-language:experimental.macros",
    ),
  )