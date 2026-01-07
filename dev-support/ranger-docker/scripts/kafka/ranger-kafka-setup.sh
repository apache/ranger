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

cat <<EOF > /etc/ssh/ssh_config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  ${RANGER_SCRIPTS}/wait_for_keytab.sh kafka.keytab
fi

cp ${RANGER_SCRIPTS}/core-site.xml          ${KAFKA_HOME}/config/
cp ${RANGER_SCRIPTS}/kafka-server-jaas.conf ${KAFKA_HOME}/config/

chown -R kafka:hadoop /opt/kafka/

cd ${RANGER_HOME}/ranger-kafka-plugin
./enable-kafka-plugin.sh

sed -i 's/localhost:2181/ranger-zk.rangernw:2181/' ${KAFKA_HOME}/config/server.properties

if [ "${KERBEROS_ENABLED}" == "true" ]; then
  echo "Configuring Kafka with Kerberos (SASL_PLAINTEXT)"
  cat <<EOF >> ${KAFKA_HOME}/config/server.properties

    # Enable SASL/GSSAPI mechanism
    sasl.enabled.mechanisms=GSSAPI
    sasl.mechanism.inter.broker.protocol=GSSAPI
    security.inter.broker.protocol=SASL_PLAINTEXT

    # Listener configuration
    listeners=SASL_PLAINTEXT://:9092
    advertised.listeners=SASL_PLAINTEXT://ranger-kafka.rangernw:9092

    # JAAS configuration for Kerberos
    listener.name.sasl_plaintext.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
    useKeyTab=true \
    storeKey=true \
    keyTab="/etc/keytabs/kafka.keytab" \
    principal="kafka/ranger-kafka.rangernw@EXAMPLE.COM";

    # Kerberos service name
    sasl.kerberos.service.name=kafka

    # Ranger authorization
    authorizer.class.name=org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer
EOF
else
  echo "Configuring Kafka with PLAINTEXT (no Kerberos)"
  cat <<EOF >> ${KAFKA_HOME}/config/server.properties
    # Listener configuration (PLAINTEXT - no authentication)
    listeners=PLAINTEXT://:9092
    advertised.listeners=PLAINTEXT://ranger-kafka.rangernw:9092

    # Ranger authorization
    # disabling Ranger Plugin for now.
    # authorizer.class.name=org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer
EOF
fi
