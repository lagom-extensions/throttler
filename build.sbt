name := "lagom-throttler"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  lagomScaladslApi         % "provided",
  lagomScaladslPersistence % "provided",
  lagomScaladslCluster     % "provided",
  "org.apache.commons"     % "commons-collections4" % "4.0",
  "org.scalatest"          %% "scalatest" % "3.0.5" % Test
)

libraryDependencies ++= Seq(compilerPlugin("com.github.ghik" %% "silencer-plugin" % "1.3.0"), "com.github.ghik" %% "silencer-lib" % "1.3.0" % Provided)
scalacOptions += "-P:silencer:pathFilters=target/.*"
scalacOptions += s"-P:silencer:sourceRoots=${baseDirectory.value.getCanonicalPath}"

organization := "com.github.lagom-extensions"
homepage := Some(url("https://github.com/lagom-extensions/throttler"))
scmInfo := Some(ScmInfo(url("https://github.com/lagom-extensions/throttler"), "git@github.com:lagom-extensions/throttler.git"))
developers := List(Developer("kuzkdmy", "Dmitriy Kuzkin", "mail@dmitriy.kuzkin@gmail.com", url("https://github.com/kuzkdmy")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

publishTo := Some(
  if (isSnapshot.value) Opts.resolver.sonatypeSnapshots
  else Opts.resolver.sonatypeStaging
)
