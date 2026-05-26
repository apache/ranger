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

#
# Creates the ranger_audits Kafka topic with the correct partition count.
# Idempotent: skips creation if the topic already exists.
#
# Usage: ./scripts/kafka/create-ranger-audit-topic.sh
#
# Environment variables:
#   TOPIC_NAME   - Topic name (default: ranger_audits)
#   PARTITIONS   - Number of partitions (default: 10)
#   REPLICATION  - Replication factor (default: 1)
#   KAFKA_CONTAINER - Kafka container name (default: ranger-kafka)
#

set -euo pipefail

TOPIC_NAME="${TOPIC_NAME:-ranger_audits}"
PARTITIONS="${PARTITIONS:-10}"
REPLICATION="${REPLICATION:-1}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ranger-kafka}"

BOOTSTRAP_SERVER="ranger-kafka.rangernw:9092"
KEYTAB="/etc/keytabs/kafka.keytab"
PRINCIPAL="kafka/ranger-kafka.rangernw@EXAMPLE.COM"

echo "Kafka topic    : ${TOPIC_NAME}"
echo "Partitions     : ${PARTITIONS}"
echo "Replication    : ${REPLICATION}"
echo "Container      : ${KAFKA_CONTAINER}"

if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "ERROR: Container '${KAFKA_CONTAINER}' is not running" >&2
  exit 1
fi

SASL_CONFIG="security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"${KEYTAB}\" principal=\"${PRINCIPAL}\";"

docker exec "${KAFKA_CONTAINER}" bash -c "
  kinit -kt ${KEYTAB} ${PRINCIPAL} 2>/dev/null

  /opt/kafka/bin/kafka-topics.sh --create \
    --if-not-exists \
    --topic '${TOPIC_NAME}' \
    --partitions ${PARTITIONS} \
    --replication-factor ${REPLICATION} \
    --bootstrap-server ${BOOTSTRAP_SERVER} \
    --command-config <(echo '${SASL_CONFIG}')
" 2>&1 | grep -v "WARN.*TGT renewal"

echo ""
echo "Topic '${TOPIC_NAME}' is ready."
