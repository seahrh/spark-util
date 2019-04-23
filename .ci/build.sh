#!/bin/bash
set -o errexit
set -o nounset
set -o verbose

sbt \
    clean \
    test

# Automatic publishing for tags that have the release pattern like `1.2.3`
if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+.*$ ]]; then

    echo "Setup gpg keys"
    gpg --keyserver keyserver.ubuntu.com --recv-keys "$GPG_KEY_ID"
    openssl aes-256-cbc \
	    -K "$encrypted_c5bb60cb91b9_key" \
	    -iv "$encrypted_c5bb60cb91b9_iv" \
	    -in .ci/sec.gpg.enc \
	    -out sec.gpg \
	    -d
    gpg --import sec.gpg

    echo "Setup publishing"
    cat <<-EOF > sonatype.sbt
	credentials in Global += Credentials(
	    "GnuPG Key ID",
	    "gpg",
	    "$GPG_KEY_ID",
	    "ignored"
	)
	credentials in Global += Credentials(
	    "Sonatype Nexus Repository Manager",
	    "oss.sonatype.org",
	    "$SONATYPE_USERNAME",
	    "$SONATYPE_PASSWORD"
	)
	EOF

    echo "Publish release"
    sbt publish sonatypeRelease
fi
