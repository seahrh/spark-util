// The plugin is a solution to sign artifacts. It works with the GPG command line tool.
//addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
addSbtPlugin("io.crashbox" % "sbt-gpg" % "0.2.0")

// automate the staging -> release workflow of Nexus
// the sbt-sonatype plugin can also be used to publish to other non-sonatype repositories
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.0")

// integrate with the release process
//addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")
