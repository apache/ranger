#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

RANGER_URL="${RANGER_URL:-http://localhost:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASS="${RANGER_PASS:-rangerR0cks!}"
OZONE_SVC_DEF_ID="${OZONE_SVC_DEF_ID:-201}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

login() {
  local cookie
  cookie="$(mktemp)"
  curl -sf -c "${cookie}" -X POST "${RANGER_URL}/login" \
    -d "username=${RANGER_USER}&password=${RANGER_PASS}" >/dev/null \
    || fail "Ranger Admin login failed at ${RANGER_URL}"
  echo "${cookie}"
}

has_action_matches() {
  python3 - "$1" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
conds = data.get("policyConditions") or []
print("true" if any(c.get("name") == "action-matches" for c in conds) else "false")
PY
}

option_enabled() {
  python3 - "$1" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
opts = data.get("options") or {}
print(opts.get("enableOzoneActionPolicy", "missing"))
PY
}

echo "=== Ozone action-policy feature flag E2E (${RANGER_URL}) ==="

curl -sf "${RANGER_URL}/" >/dev/null 2>&1 || fail "Ranger Admin not reachable at ${RANGER_URL}"

COOKIE="$(login)"
TMP="$(mktemp)"
trap 'rm -f "${TMP}" "${COOKIE:-}"' EXIT

curl -sf -b "${COOKIE}" "${RANGER_URL}/service/plugins/definitions/${OZONE_SVC_DEF_ID}" > "${TMP}" \
  || fail "failed to fetch ozone service-def id ${OZONE_SVC_DEF_ID}"

ENABLED="$(option_enabled "${TMP}")"
HAS_ACTION="$(has_action_matches "${TMP}")"

echo "enableOzoneActionPolicy option: ${ENABLED}"
echo "action-matches in policyConditions: ${HAS_ACTION}"

if [[ "${ENABLED}" == "false" && "${HAS_ACTION}" == "false" ]]; then
  pass "flag OFF — service-def hides action-matches"
elif [[ "${ENABLED}" == "true" && "${HAS_ACTION}" == "true" ]]; then
  pass "flag ON — service-def includes action-matches"
else
  fail "unexpected combination: enableOzoneActionPolicy=${ENABLED}, action-matches=${HAS_ACTION}"
fi

if docker ps --format '{{.Names}}' | grep -qx 'ozone-om'; then
  OZONE_REALM="${OZONE_KERBEROS_REALM:-EXAMPLE.COM}"
  if docker exec ozone-om test -f /etc/security/keytabs/ozone.service.keytab; then
    docker exec ozone-om bash -lc "kinit -kt /etc/security/keytabs/ozone.service.keytab ozone/om.rangernw@${OZONE_REALM} 2>/dev/null || true"
    if docker exec ozone-om timeout 20 /opt/hadoop/bin/ozone sh volume list 2>&1 | head -5; then
      pass "ozone CLI volume list succeeded"
    else
      fail "ozone volume list failed"
    fi
  else
    echo "WARN: ozone.service.keytab missing; skipping CLI volume list"
  fi
else
  echo "WARN: ozone-om container not running; skipping CLI check"
fi

echo "=== E2E checks completed ==="
