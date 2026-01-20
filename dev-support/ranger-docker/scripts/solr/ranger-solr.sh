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

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  /home/ranger/scripts/wait_for_keytab.sh HTTP.keytab
  /home/ranger/scripts/wait_for_keytab.sh solr.keytab
  /home/ranger/scripts/wait_for_testusers_keytab.sh

  JAAS_CONFIG="-Djava.security.auth.login.config=/opt/solr/server/etc/jaas.conf"
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
fi

if [ ! -e ${SOLR_INSTALL_DIR}/.setupDone ]
then
  cd /opt/ranger/ranger-solr-plugin
  ./enable-solr-plugin.sh

  cp /home/ranger/scripts/core-site.xml /opt/solr/server/resources/

  touch "${SOLR_INSTALL_DIR}"/.setupDone
fi

su -p -c "export PATH=${PATH} && /opt/docker-solr/scripts/docker-entrypoint.sh $*" solr
