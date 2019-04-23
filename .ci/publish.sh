#!/bin/bash
set -o errexit
set -o nounset
set -o verbose

echo "Setup gpg keys"

gpg --keyserver keyserver.ubuntu.com --recv-keys "$GPG_KEY_ID"
openssl aes-256-cbc \
    -K "$encrypted_c5bb60cb91b9_key" \
    -iv "$encrypted_c5bb60cb91b9_iv" \
    -in .ci/sec.gpg.enc \
    -out sec.gpg \
    -d
gpg --import sec.gpg

echo "Publish release"

sbt publish
