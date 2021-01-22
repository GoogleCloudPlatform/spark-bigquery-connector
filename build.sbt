/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
lazy val scala211Version = "2.11.12"
lazy val scala212Version = "2.12.10"
lazy val sparkVersion = "2.4.0"
lazy val grpcVersion = "1.30.2"
// should match the dependency from grpc-netty
lazy val nettyVersion = "4.1.51.Final"
// should match the dependency in grpc-netty
lazy val nettyTcnativeVersion = "2.0.34.Final"

lazy val commonSettings = Seq(
  organization := "com.google.cloud.spark",
  version := "0.18.2-SNAPSHOT",
  scalaVersion := scala211Version,
  crossScalaVersions := Seq(scala211Version, scala212Version)
)

// scalastyle:off
// For https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/72
// Based on
// https://github.com/sbt/sbt-assembly/#q-despite-the-concerned-friends-i-still-want-publish-fat-jars-what-advice-do-you-have
// scalastyle:on
lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings, skip in publish := true)
  .aggregate(connector, fatJar, published)

lazy val commonTestDependencies = Seq(
  "io.grpc" % "grpc-alts" % grpcVersion exclude("io.grpc", "grpc-netty-shaded"),
  "io.grpc" % "grpc-netty" % grpcVersion,
  "com.google.api" % "gax-grpc" % "1.60.0" exclude("io.grpc", "grpc-netty-shaded"),
  "com.google.guava" % "guava" % "30.0-jre",

  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "org.mockito" %% "mockito-scala-scalatest" % "1.10.0" % "test",
  "junit" % "junit" % "4.13" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "com.google.truth" % "truth" % "1.1" % "test",
  "com.google.truth.extensions" % "truth-java8-extension" % "1.1" % "test"
)

lazy val connector = (project in file("connector"))
  .enablePlugins(BuildInfoPlugin)
  .configs(ITest)
  .settings(
    commonSettings,
    publishSettings,
    name := "spark-bigquery",
    unmanagedSourceDirectories in Compile += baseDirectory.value /
      "third_party/apache-spark/src/main/java",
    inConfig(ITest)(Defaults.testTasks),
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in ITest := Seq(Tests.Filter(itFilter)),
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.google.cloud.spark.bigquery",
    resourceGenerators in Compile += Def.task {
      val file = (resourceManaged in Compile).value / "spark-bigquery-connector.properties"
      IO.write(file, s"scala.version=${scalaVersion.value}\n")
      Seq(file)
    }.taskValue,
    libraryDependencies ++= (commonTestDependencies ++ Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.slf4j" % "slf4j-api" % "1.7.16" % "provided",
      "aopalliance" % "aopalliance" % "1.0" % "provided",
      "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13" % "provided",
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13" % "provided",
      "org.apache.arrow" % "arrow-vector" % "0.16.0" exclude("org.slf4j", "slf4j-api"),
      "com.google.inject" % "guice" % "4.2.3",

      // Keep com.google.cloud dependencies in sync
      "com.google.cloud" % "google-cloud-bigquery" % "1.123.2",
      "com.google.cloud" % "google-cloud-bigquerystorage" % "1.6.0"
        exclude("io.grpc", "grpc-netty-shaded"),
      // Keep in sync with com.google.cloud
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.3",
      "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.11.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.3",

      // Netty, with a version supporting Java 11
      "io.netty" % "netty-all" % nettyVersion % "provided",
      "io.netty" % "netty-buffer" % nettyVersion,
      "io.netty" % "netty-common" % nettyVersion,
      // scalastyle:off
      // See https://github.com/grpc/grpc-java/blob/master/SECURITY.md#tls-with-netty-tcnative-on-boringssl
      // scalastyle:on
      "io.netty" % "netty-tcnative-boringssl-static" % nettyTcnativeVersion,

      // runtime
      // scalastyle:off
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.0.0" % "runtime" classifier("shaded")
        exclude("com.google.cloud.bigdataoss", "util-hadoop"),
      // scalastyle:on
      // test

      "org.apache.spark" %% "spark-avro" % sparkVersion % "test"
      ))
      .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*)),
    dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.16" % "provided"
  )

lazy val fatJar = project
  .enablePlugins(AssemblyPlugin)
  .settings(
    commonSettings,
    skip in publish := true,
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyShadeRules in assembly := (
      notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
        ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
      ).map(_.inAll),

    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("/public-suffix-list.txt") => MergeStrategy.filterDistinctLines
      case "module-info.class" => MergeStrategy.discard
      case PathList(ps@_*) if ps.last.endsWith(".properties") => MergeStrategy.filterDistinctLines
      case PathList(ps@_*) if ps.last.endsWith(".proto") => MergeStrategy.discard
      // Relocate netty-tcnative.so. This is necessary even though gRPC shades it, because we shade
      // gRPC.
      case PathList("META-INF", "native", f) if f.contains("netty_tcnative") =>
        RelocationMergeStrategy(path =>
          path.replace("native/lib", s"native/lib${relocationPrefix.replace('.', '_')}_"))

      // Relocate GRPC service registries
      case PathList("META-INF", "services", _) => ServiceResourceMergeStrategy(renamed,
        relocationPrefix)
      case x => (assemblyMergeStrategy in assembly).value(x)
    }
  )
  .dependsOn(connector)

val publishVerified = taskKey[Seq[String]]("Published signed artifact after acceptance test")
lazy val published = project
  .configs(AcceptanceTest)
  .settings(
    commonSettings,
    publishSettings,
    name := "spark-bigquery-with-dependencies",
    packageBin in Compile := (assembly in(fatJar, Compile)).value,
    test in AcceptanceTest := (test in AcceptanceTest).dependsOn(packageBin in Compile).value,
    // publishSigned in SbtPgp := publishSigned.dependsOn(test in AcceptanceTest).value,
    inConfig(AcceptanceTest)(Defaults.testTasks),
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in AcceptanceTest := Seq(Tests.Filter(acceptanceFilter)),
    publishVerified := { Seq(
      (packageBin in Compile).value.toString,
      (test in AcceptanceTest).value.toString
    ) },
    libraryDependencies ++= (commonTestDependencies.map(
      dependency => dependency.withConfigurations(Some("test"))) ++ Seq(
      "com.google.cloud" % "google-cloud-dataproc" % "1.0.0" % "test",
      "com.google.cloud" % "google-cloud-storage" % "1.109.1" % "test"
    ))
      .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*))

  )

lazy val myPackage = "com.google.cloud.spark.bigquery"
lazy val relocationPrefix = s"$myPackage.repackaged"

// Exclude dependencies already on Spark's Classpath
val excludedOrgs = Seq(
  // All use commons-cli:1.4
  "commons-cli",
  // Not a runtime dependency
  "com.google.auto.value",
  // All use jsr305:3.0.0
  "com.google.code.findbugs",
  "javax.annotation",
  // Spark Uses 2.9.9 google-cloud-core uses 2.9.2
  "com.sun.jdmk",
  "com.sun.jmx",
  "javax.activation",
  "javax.jms",
  "javax.mail"
)

lazy val renamed = Seq(
  "avro.shaded",
  "com.fasterxml",
  "com.google",
  "com.thoughtworks.paranamer",
  "com.typesafe",
  "io.grpc",
  "io.netty",
  "io.opencensus",
  "org.apache.arrow",
  "io.perfmark",
  "org.apache.commons",
  "org.apache.http",
  "org.checkerframework",
  "org.codehaus.mojo",
  "org.conscrypt",
  "org.json",
  "org.threeten",
  "org.tukaani.xz",
  "org.xerial.snappy")
lazy val notRenamed = Seq(myPackage)

// Default IntegrationTest config uses separate test directory, build files
lazy val ITest = config("it") extend Test
// Run scalastyle automatically
(test in Test) := ((test in Test) dependsOn scalastyle.in(Test).toTask("")).value
parallelExecution in ITest := false

// Default IntegrationTest config uses separate test directory, build files
lazy val AcceptanceTest = config("acceptance") extend Test
parallelExecution in AcceptanceTest := false

def unitFilter(name: String): Boolean =
  (name.endsWith("Suite") || name.endsWith("Test")) && !itFilter(name) && !acceptanceFilter(name)

def itFilter(name: String): Boolean = name endsWith "ITSuite"

def acceptanceFilter(name: String): Boolean = name endsWith "AcceptanceTest"

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector")),
  scmInfo := Some(ScmInfo(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector"),
    "git@github.com:GoogleCloudPlatform/spark-bigquery-connector.git")),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  publishMavenStyle := true,
  pomExtra :=
    <developers>
      <developer>
        <organization>Google LLC</organization>
        <organizationUrl>http://www.google.com</organizationUrl>
      </developer>
    </developers>,

  publishTo := Some(if (version.value.trim.endsWith("SNAPSHOT")) {
    Opts.resolver.sonatypeSnapshots
  } else {
    Opts.resolver.sonatypeStaging
  })
)
