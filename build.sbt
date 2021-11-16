name := "md_data_client_library"

version := "0.1"

scalaVersion := "2.12.12"
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
resolvers += "HERE Repo" at "https://repo.platform.here.com/artifactory/open-location-platform/"
libraryDependencies ++= Seq(
  "com.here.platform.data.client" %% "data-engine" % "1.11.21",
  "com.typesafe" % "config" % "1.4.0"
)