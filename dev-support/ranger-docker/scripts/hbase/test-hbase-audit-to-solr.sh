#!/usr/bin/env bash
# Verify HBase plugin writes authorization audits to Solr ranger_audits core
set -euo pipefail

SOLR_HOST="${SOLR_HOST:-ranger-solr.rangernw}"
SOLR_BASE="http://${SOLR_HOST}:8983"
REPO="${REPO:-dev_hbase}"
HBASE_HOST="${HBASE_HOST:-ranger-hbase.rangernw}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "=== 1. HBase + plugin healthy ==="
docker exec ranger-hbase bash -c 'ps aux | grep org.apache.hadoop.hbase.master.HMaster | grep -v grep' | grep -q HMaster || fail "HMaster not running"
docker exec ranger-hbase grep -A1 'xasecure.audit.destination.solr</name>' /opt/hbase/conf/ranger-hbase-audit.xml | grep -q '<value>true</value>' || fail "Solr audit not enabled in ranger-hbase-audit.xml"
docker exec ranger-hbase test -d /var/log/hadoop/hbase/audit/solr/spool || fail "Solr audit spool dir missing"
pass "HBase stack up (Solr audit enabled, spool dir present)"

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

echo "=== 4. HBase access (testuser1) to generate audit ==="
set +e
docker exec ranger-hbase bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_u
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/testuser1.keytab testuser1/${HBASE_HOST}@EXAMPLE.COM
  klist
  TABLE=test_ranger_audit_\$(date +%s)
  echo \"create '\${TABLE}', 'cf'\" | /opt/hbase/bin/hbase shell -n 2>&1 || true
  echo 'list' | /opt/hbase/bin/hbase shell -n 2>&1 || true
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

[ "${after}" -gt "${before}" ] || fail "HBase audit count did not increase (${before} -> ${after})"
pass "HBase audit write to Solr (${before} -> ${after})"

echo ""
echo "=== ALL HBASE->SOLR AUDIT CHECKS PASSED ==="
