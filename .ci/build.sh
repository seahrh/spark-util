#!/bin/bash
set -o errexit
set -o nounset
set -o verbose

sbt \
    clean \
    test \
    scripted

# Automatic publishing for tags that have the pattern like `1.2.3`
if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+.*$ ]]; then
    # Setup gpg keys
    gpg --keyserver keyserver.ubuntu.com --recv-keys "C8DBA8065F261033D78A3AD9B2BFA790AECB1FF8"
    openssl aes-256-cbc \
	    -K "$encrypted_c5bb60cb91b9_key" \
	    -iv "$encrypted_c5bb60cb91b9_iv" \
	    -in .ci/sec.gpg.enc \
	    -out sec.gpg \
	    -d
    gpg --import sec.gpg

    # Setup publishing
    cat <<-EOF > sonatype.sbt
	credentials in Global += Credentials(
	    "GnuPG Key ID",
	    "gpg",
	    "C8DBA8065F261033D78A3AD9B2BFA790AECB1FF8",
	    "ignored"
	)
	credentials in Global += Credentials(
	    "Sonatype Nexus Repository Manager",
	    "oss.sonatype.org",
	    "$SONATYPE_USERNAME",
	    "$SONATYPE_PASSWORD"
	)
	EOF

    # publish release
    sbt publish
fi