# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

FROM openjdk:8-jdk-alpine

RUN mkdir -p /opt/apache/ranger-admin

RUN apk --no-cache add python bash java-postgresql-jdbc bc shadow procps curl && \
    apk --no-cache update && \
    apk --no-cache upgrade && \
    groupadd -r ranger -g 6080 && \
    useradd --no-log-init -r -g ranger -u 6080 -d /opt/apache/ranger-admin ranger

COPY target/ranger-3.0.0-SNAPSHOT-admin.tar.gz /ranger-3.0.0-SNAPSHOT-admin.tar.gz
COPY target/ranger-3.0.0-SNAPSHOT-atlas-plugin.tar.gz /ranger-3.0.0-SNAPSHOT-atlas-plugin.tar.gz
RUN tar -xzf /ranger-3.0.0-SNAPSHOT-admin.tar.gz -C /opt/apache/ranger-admin --strip-components=1

WORKDIR /opt/apache/ranger-admin
RUN chmod +x /opt/apache/ranger-admin/ews/ranger-admin-services.sh

RUN wget https://atlan-public.s3-eu-west-1.amazonaws.com/ranger-plugins/ranger-heka-plugin-1.0-SNAPSHOT.jar && \
    mkdir /opt/apache/ranger-admin/ews/webapp/WEB-INF/classes/ranger-plugins/heka/ && \
    mv ./ranger-heka-plugin-1.0-SNAPSHOT.jar /opt/apache/ranger-admin/ews/webapp/WEB-INF/classes/ranger-plugins/heka/

EXPOSE 6080

ENTRYPOINT ["/bin/bash", "-c", "/opt/apache/ranger-admin/setup.sh && ranger-admin start && tail -F ews/logs/*.log"]