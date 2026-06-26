#!/usr/bin/env bash
# Verify Kafka plugin writes authorization audits to Solr ranger_audits core
set -euo pipefail

SOLR_HOST="${SOLR_HOST:-ranger-solr.rangernw}"
SOLR_BASE="http://${SOLR_HOST}:8983"
REPO="${REPO:-dev_kafka}"
KAFKA_HOST="${KAFKA_HOST:-ranger-kafka.rangernw}"

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

echo "=== 1. Kafka + plugin healthy ==="
docker exec ranger-kafka bash -c 'ps aux | grep kafka.Kafka | grep -v grep' | grep -q Kafka || fail "Kafka broker not running"
docker exec ranger-kafka grep -A1 'xasecure.audit.destination.solr</name>' /opt/kafka/config/ranger-kafka-audit.xml | grep -q '<value>true</value>' || fail "Solr audit not enabled"
docker exec ranger-kafka test -d /var/log/kafka/audit/solr/spool || fail "Solr audit spool dir missing"
pass "Kafka stack up (Solr audit enabled)"

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

echo "=== 4. Kafka access (testuser1) to generate audit ==="
# Authorization may deny the operation; denied attempts still produce Ranger audits.
docker exec ranger-kafka bash -c "
  set +e
  cat > /tmp/kafka-client-jaas.conf <<'EOF'
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  keyTab=\"/etc/keytabs/testuser1.keytab\"
  principal=\"testuser1/${KAFKA_HOST}@EXAMPLE.COM\";
};
EOF
  cat > /tmp/client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
EOF
  export KRB5CCNAME=FILE:/tmp/cc_u
  kdestroy -q 2>/dev/null || true
  kinit -kt /etc/keytabs/testuser1.keytab testuser1/${KAFKA_HOST}@EXAMPLE.COM
  export KAFKA_OPTS=\"-Djava.security.auth.login.config=/tmp/kafka-client-jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf\"
  topic=\"ranger-audit-test-\$(date +%s)\"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_HOST}:9092 --create --topic \"\${topic}\" --partitions 1 --replication-factor 1 --command-config /tmp/client.properties 2>&1 || true
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server ${KAFKA_HOST}:9092 --entity-type topics --entity-name \"\${topic}\" --describe --command-config /tmp/client.properties 2>&1 || true
  true
"

echo "Waiting 25s for Solr audit flush..."
sleep 25

after=$(docker exec ranger-solr bash -c "
  export KRB5CCNAME=FILE:/tmp/cc_h
  kinit -kt /etc/keytabs/HTTP.keytab HTTP/${SOLR_HOST}@EXAMPLE.COM
  curl -s --negotiate -u : '${SOLR_BASE}/solr/ranger_audits/select?q=repo:${REPO}&rows=0&wt=json'
" | grep -o '"numFound":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "After: ${after}"

[ "${after}" -gt "${before}" ] || fail "Kafka audit count did not increase (${before} -> ${after})"
pass "Kafka audit write to Solr (${before} -> ${after})"

echo ""
echo "=== ALL KAFKA->SOLR AUDIT CHECKS PASSED ==="
