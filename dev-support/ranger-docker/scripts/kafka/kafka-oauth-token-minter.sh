#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dev-only stand-in for a platform token issuer (e.g. the kubelet writing projected
# service-account tokens). Mints unsigned (alg=none) JWTs for the Ranger Kafka clients
# into a volume shared with their containers and rewrites them in place on a loop, so
# the SASL/OAUTHBEARER login handler's re-read-on-refresh path is exercised. The broker
# accepts them through Kafka's OAuthBearerUnsecuredValidatorCallbackHandler; never use
# this outside the docker dev setup.

set -u

TOKEN_DIR="${KAFKA_OAUTH_TOKEN_DIR:-/etc/kafka-oauth}"
TOKEN_TTL="${KAFKA_OAUTH_TOKEN_TTL_SECONDS:-600}"
REFRESH="${KAFKA_OAUTH_TOKEN_REFRESH_SECONDS:-120}"
SUBJECTS="${KAFKA_OAUTH_TOKEN_SUBJECTS:-rangerauditserver rangertagsync}"
ISSUER="${KAFKA_OAUTH_TOKEN_ISSUER:-ranger-docker}"

b64url() {
  base64 -w0 | tr '+/' '-_' | tr -d '='
}

mint_token() {
  local subject="$1"
  local file="${TOKEN_DIR}/${subject}.token"
  local now exp header payload

  now=$(date +%s)
  exp=$((now + TOKEN_TTL))
  header=$(printf '{"alg":"none","typ":"JWT"}' | b64url)
  payload=$(printf '{"iss":"%s","sub":"%s","aud":"kafka","iat":%d,"exp":%d}' "${ISSUER}" "${subject}" "${now}" "${exp}" | b64url)

  # same atomic replace kubelet does: write a temp file, then rename over the old token
  printf '%s.%s.' "${header}" "${payload}" > "${file}.tmp"
  chmod 644 "${file}.tmp"
  mv -f "${file}.tmp" "${file}"
}

mkdir -p "${TOKEN_DIR}"
chmod 755 "${TOKEN_DIR}"

echo "[INFO] kafka-oauth-token-minter: writing tokens for [${SUBJECTS}] to ${TOKEN_DIR} (ttl=${TOKEN_TTL}s, refresh every ${REFRESH}s)"

while true; do
  for subject in ${SUBJECTS}; do
    mint_token "${subject}"
  done

  sleep "${REFRESH}"
done
