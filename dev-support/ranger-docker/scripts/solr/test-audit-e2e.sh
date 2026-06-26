#!/usr/bin/env bash
# End-to-end audit verification for Ranger Docker (Solr plugin + Admin UI)
set -euo pipefail

SOLR_HOST="${SOLR_HOST:-ranger-solr.rangernw}"
SOLR_BASE="http://${SOLR_HOST}:8983"
REPO="${REPO:-dev_solr}"
ADMIN_URL="${ADMIN_URL:-http://localhost:6080}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS:-rangerR0cks!}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "=== 1. Solr plugin: policy download ==="
SOLR_LOG=$(docker logs ranger-solr 2>&1 || true)
echo "${SOLR_LOG}" | grep -Fq "PolicyRefresher(serviceName=dev_solr): found updated version" || fail "PolicyRefresher did not download policies"
echo "${SOLR_LOG}" | grep -Fq "policy evaluators" || fail "No policy evaluators in log"
pass "Policy download"

echo "=== 2. Solr: ranger_audits core loaded ==="
echo "${SOLR_LOG}" | grep -Fq "Creating SolrCore 'ranger_audits'" || fail "ranger_audits core not created"
pass "Audit core loaded"

echo "=== 3. Plugin audit WRITE (testuser1 -> /admin/cores) ==="
before=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')

docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_u
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/testuser1.keytab testuser1/${SOLR_HOST}@EXAMPLE.COM
  curl -s -o /dev/null --negotiate -u : '${SOLR_BASE}/solr/admin/cores'
"
sleep 18
after=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')

[ "${after}" -gt "${before}" ] || fail "Audit count did not increase (${before} -> ${after})"
pass "Plugin audit write (${before} -> ${after})"

echo "=== 4. Solr audit READ (HTTP SPNEGO) ==="
docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -sf --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=*:*&rows=1&wt=json' >/dev/null
" || fail "HTTP read of ranger_audits failed"
pass "Direct Solr read"

echo "=== 5. Ranger Admin UI audit READ (SolrMgr / xaudit API) ==="
docker exec ranger bash -c "grep -A1 'ranger.audit.solr.urls' /opt/ranger/admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml" | grep -q ranger-solr.rangernw || fail "Admin audit URL not FQDN"
resp=$(curl -sf -u "${ADMIN_USER}:${ADMIN_PASS}" "${ADMIN_URL}/service/xaudit/access_audit?pageSize=2&startIndex=0") || fail "xaudit API request failed"
echo "${resp}" | grep -q '"totalCount"' || fail "No totalCount in response"
echo "${resp}" | grep -q 'msgDesc' && echo "${resp}" | grep -q '"statusCode":1' && fail "API returned error: ${resp}"
pass "Ranger Admin audit API (UI uses same path)"

echo ""
echo "=== ALL CHECKS PASSED ==="
