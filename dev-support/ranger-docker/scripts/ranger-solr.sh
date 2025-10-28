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

if [ ! -e ${SOLR_INSTALL_DIR}/.setupDone ]
then
  if [ "${KERBEROS_ENABLED}" == "true" ]
  then
    KEYTABS_DIR=/opt/solr/keytabs

    /home/ranger/scripts/create_principal_and_keytab.sh HTTP ${KEYTABS_DIR} solr
  fi

  touch "${SOLR_INSTALL_DIR}"/.setupDone
fi

if [ "${KERBEROS_ENABLED}" == "true" ]
then
  export SOLR_AUTH_TYPE=kerberos
  export SOLR_AUTHENTICATION_OPTS="-Djava.security.auth.login.config=/opt/solr/server/etc/jaas.conf -Dsolr.kerberos.jaas.appname=Client -Djava.security.krb5.conf=/etc/krb5.conf -Dsolr.kerberos.keytab=/opt/solr/keytabs/HTTP.keytab -Dsolr.kerberos.principal=HTTP/ranger-solr.rangernw@EXAMPLE.COM -Dsolr.kerberos.cookie.domain=ranger-solr -Dsolr.kerberos.name.rules=RULE:[2:\$1/\$2@\$0]([ndj]n/.*@EXAMPLE\.COM)s/.*/hdfs/\
RULE:[2:\$1/\$2@\$0]([rn]m/.*@EXAMPLE\.COM)s/.*/yarn/\
RULE:[2:\$1/\$2@\$0](jhs/.*@EXAMPLE\.COM)s/.*/mapred/\
DEFAULT"
fi

su -p -c "export PATH=${PATH} && /opt/docker-solr/scripts/docker-entrypoint.sh $*" solr
