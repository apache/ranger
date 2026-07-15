#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Generates throwaway test keystores for TestRangerDefaultHostnameVerifier.
set -euo pipefail

OUT_DIR="${1:?Usage: generate-test-keystores.sh <output-dir>}"
PASS="changeit"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

rm -f ca.jks ca.pem *.jks *.csr *.pem 2>/dev/null || true

# --- CA ---
keytool -genkeypair -alias fakeCA -keyalg RSA -keysize 2048 -validity 30 -dname "CN=Ranger Test CA" -ext bc:c=ca:true -keystore ca.jks -storepass "$PASS" -keypass "$PASS" -noprompt
keytool -exportcert -alias fakeCA -keystore ca.jks -storepass "$PASS" -rfc -file ca.pem

# --- helper: issue a leaf cert with given CN/SAN into its own keystore ---
issue_cert() {
  local alias="$1" keystore="$2" dname="$3" san="$4"
  keytool -genkeypair -alias "$alias" -keyalg RSA -keysize 2048 -validity 30 -dname "$dname" -keystore "$keystore" -storepass "$PASS" -keypass "$PASS" -noprompt
  keytool -certreq -alias "$alias" -keystore "$keystore" -storepass "$PASS" -file "$alias.csr"
  keytool -gencert -alias fakeCA -keystore ca.jks -storepass "$PASS" -infile "$alias.csr" -outfile "$alias.pem" -ext "$san" -validity 30
  keytool -importcert -alias fakeCA -keystore "$keystore" -storepass "$PASS" -file ca.pem -noprompt
  keytool -importcert -alias "$alias" -keystore "$keystore" -storepass "$PASS" -file "$alias.pem" -noprompt
}

issue_cert serverkey attacker-cert.jks  "CN=attacker.internal"    "san=dns:attacker.internal"
issue_cert serverkey localhost-cert.jks "CN=localhost"            "san=dns:localhost"
issue_cert serverkey wildcard-cert.jks  "CN=*.example.test"       "san=dns:*.example.test"

echo "Generated attacker-cert.jks, localhost-cert.jks, wildcard-cert.jks in $OUT_DIR"
