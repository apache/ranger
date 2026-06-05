#!/usr/bin/env bash
# Verify Hive plugin writes authorization audits to Solr ranger_audits core
set -euo pipefail

SOLR_HOST="${SOLR_HOST:-ranger-solr.rangernw}"
SOLR_BASE="http://${SOLR_HOST}:8983"
REPO="${REPO:-dev_hive}"
HIVE_HOST="${HIVE_HOST:-ranger-hive.rangernw}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "=== 1. Hive + plugin healthy ==="
docker exec ranger-hive bash -c 'ps aux | grep org.apache.hive.service.server.HiveServer2 | grep -v grep' | grep -q HiveServer2 || fail "HiveServer2 not running"
docker exec ranger-hive grep -A1 'xasecure.audit.destination.solr</name>' /opt/hive/conf/ranger-hive-audit.xml | grep -q '<value>true</value>' || fail "Solr audit not enabled in ranger-hive-audit.xml"
docker exec ranger-hive test -d /var/log/hive/audit/solr/spool || fail "Solr audit spool dir missing"
pass "Hive stack up (Solr audit enabled, spool dir present)"

echo "=== 2. Solr ranger_audits core reachable ==="
docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -sf --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json' >/dev/null
" || fail "Cannot query ranger_audits"
pass "Solr audit core reachable"

echo "=== 3. Baseline audit count (repo=${REPO}) ==="
before=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "Before: ${before}"

echo "=== 4. Hive access (testuser1) to generate audit ==="
set +e
docker exec ranger-hive bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_u
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/testuser1.keytab testuser1/${HIVE_HOST}@EXAMPLE.COM
  klist
  /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;principal=hive/${HIVE_HOST}@EXAMPLE.COM' -e 'show databases;' 2>&1
  /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;principal=hive/${HIVE_HOST}@EXAMPLE.COM' -e 'show tables in default;' 2>&1
"
set -e

echo "Waiting 30s for Solr audit flush..."
sleep 30

after=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "After: ${after}"

[ "${after}" -gt "${before}" ] || fail "Hive audit count did not increase (${before} -> ${after})"
pass "Hive audit write to Solr (${before} -> ${after})"

echo ""
echo "=== ALL HIVE->SOLR AUDIT CHECKS PASSED ==="
