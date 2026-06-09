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

SOLR_INSTALL_DIR=/opt/solr

export RANGER_SCRIPTS=/home/ranger/scripts

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  ${RANGER_SCRIPTS}/wait_for_keytab.sh HTTP.keytab
  ${RANGER_SCRIPTS}/wait_for_keytab.sh solr.keytab
  ${RANGER_SCRIPTS}/wait_for_testusers_keytab.sh

  # Use KDC-generated keytabs from the volume mount (/etc/keytabs); do not copy into
  # /var/solr/data or they go stale after KDC restart (Checksum failed on login).
  JAAS_CONFIG="-Djava.security.auth.login.config=/var/solr/data/jaas.conf"
  JAAS_APPNAME="-Dsolr.kerberos.jaas.appname=Client"
  KRB5_CONF="-Djava.security.krb5.conf=/etc/krb5.conf"
  KERBEROS_KEYTAB="-Dsolr.kerberos.keytab=/etc/keytabs/HTTP.keytab"
  KERBEROS_PRINCIPAL="-Dsolr.kerberos.principal=HTTP/ranger-solr.rangernw@EXAMPLE.COM"
  COOKIE_DOMAIN="-Dsolr.kerberos.cookie.domain=ranger-solr"
  KERBEROS_NAME_RULES="-Dsolr.kerberos.name.rules=RULE:[2:\$1/\$2@\$0]([ndj]n/.*@EXAMPLE\.COM)s/.*/hdfs/\
RULE:[2:\$1/\$2@\$0]([rn]m/.*@EXAMPLE\.COM)s/.*/yarn/\
RULE:[2:\$1/\$2@\$0](jhs/.*@EXAMPLE\.COM)s/.*/mapred/\
DEFAULT"

  export SOLR_AUTHENTICATION_OPTS="${JAAS_CONFIG} ${JAAS_APPNAME} ${KRB5_CONF} ${KERBEROS_KEYTAB} ${KERBEROS_PRINCIPAL} ${COOKIE_DOMAIN} ${KERBEROS_NAME_RULES}"
  export SOLR_AUTH_TYPE=kerberos
  export HADOOP_CONF_DIR=/opt/solr/server/resources
  export SOLR_AUTHENTICATION_OPTS="${SOLR_AUTHENTICATION_OPTS} -Dhadoop.security.authentication=kerberos"
  export SOLR_OPTS="${SOLR_OPTS} ${SOLR_AUTHENTICATION_OPTS}"
fi

# Ranger policy cache, keytabs (Solr 9 SecurityManager; allowPaths used when SM is enabled)
export SOLR_OPTS="${SOLR_OPTS} -Dsolr.allowPaths=/etc/ranger,/etc/keytabs,/var/solr/data"
export SOLR_SECURITY_MANAGER_ENABLED="${SOLR_SECURITY_MANAGER_ENABLED:-false}"
# Solr 9.4+: KerberosPlugin lives in the hadoop-auth module
export SOLR_MODULES="${SOLR_MODULES:+$SOLR_MODULES,}hadoop-auth"

if [ ! -e ${SOLR_INSTALL_DIR}/.setupDone ]
then
  POLICY_CACHE_DIR=/etc/ranger/dev_solr/policycache
  mkdir -p "${POLICY_CACHE_DIR}"
  chmod a+rx /etc/ranger /etc/ranger/dev_solr "${POLICY_CACHE_DIR}" 2>/dev/null || true

  cd /opt/ranger/ranger-solr-plugin
  ./enable-solr-plugin.sh

  touch "${SOLR_INSTALL_DIR}"/.setupDone
fi

# conf/ and core.properties are bind-mounted; bootstrap if solr-precreate left an empty file
RANGER_AUDITS_CORE_DIR=/var/solr/data/ranger_audits
if [ -d "${RANGER_AUDITS_CORE_DIR}/conf" ]; then
  if [ ! -s "${RANGER_AUDITS_CORE_DIR}/core.properties" ]; then
    printf '%s\n' 'name=ranger_audits' 'config=solrconfig.xml' 'schema=managed-schema' 'dataDir=data' > "${RANGER_AUDITS_CORE_DIR}/core.properties"
    mkdir -p "${RANGER_AUDITS_CORE_DIR}/data"
  fi
  chown -R solr:solr "${RANGER_AUDITS_CORE_DIR}" 2>/dev/null || true
fi

export PATH="/opt/solr/bin:/opt/solr/docker/scripts:/opt/solr/prometheus-exporter/bin:${PATH}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/solr/server/resources}"
su -p -c "export PATH=${PATH} HADOOP_CONF_DIR=${HADOOP_CONF_DIR} SOLR_SECURITY_MANAGER_ENABLED=${SOLR_SECURITY_MANAGER_ENABLED:-false} SOLR_OPTS=\"${SOLR_OPTS}\" SOLR_AUTHENTICATION_OPTS=\"${SOLR_AUTHENTICATION_OPTS}\" && /opt/solr/docker/scripts/docker-entrypoint.sh $*" solr
