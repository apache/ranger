#!/usr/bin/env bash
# Verify HDFS plugin writes authorization audits to Solr ranger_audits core
set -euo pipefail

SOLR_HOST="${SOLR_HOST:-ranger-solr.rangernw}"
SOLR_BASE="http://${SOLR_HOST}:8983"
REPO="${REPO:-dev_hdfs}"
HADOOP_HOST="${HADOOP_HOST:-ranger-hadoop.rangernw}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "=== 1. Hadoop + HDFS plugin healthy ==="
docker exec ranger-hadoop bash -c 'ps aux | grep org.apache.hadoop.hdfs.server.namenode.NameNode | grep -v grep' | grep -q NameNode || fail "NameNode not running"
docker exec ranger-hadoop grep -A1 'xasecure.audit.destination.solr</name>' /opt/hadoop/etc/hadoop/ranger-hdfs-audit.xml | grep -q '<value>true</value>' || fail "Solr audit not enabled in ranger-hdfs-audit.xml"
docker exec ranger-hadoop test -d /var/log/hadoop/hdfs/audit/solr/spool || fail "Solr audit spool dir missing"
pass "HDFS stack up (Solr audit enabled, spool dir present)"

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

echo "=== 4. HDFS access (testuser1) to generate audit ==="
docker exec ranger-hadoop bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_u
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/testuser1.keytab testuser1/${HADOOP_HOST}@EXAMPLE.COM
  klist
  /opt/hadoop/bin/hdfs dfs -ls /
  /opt/hadoop/bin/hdfs dfs -ls /tmp
  /opt/hadoop/bin/hdfs dfs -stat '%n' /user/testuser1 2>/dev/null || /opt/hadoop/bin/hdfs dfs -ls /user
"

echo "Waiting 25s for Solr audit flush..."
sleep 25

after=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "After: ${after}"

[ "${after}" -gt "${before}" ] || fail "HDFS audit count did not increase (${before} -> ${after})"
pass "HDFS audit write to Solr (${before} -> ${after})"

echo ""
echo "=== ALL HDFS->SOLR AUDIT CHECKS PASSED ==="
