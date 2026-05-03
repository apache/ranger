/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.audit.server;

public class AuditServerConstants {
    private AuditServerConstants() {}

    public static final String AUDIT_SERVER_APPNAME                          = "ranger-audit";
    public static final String PROP_PREFIX_AUDIT_SERVER                      = "ranger.audit.ingestor.";
    public static final String PROP_PREFIX_AUDIT_SERVER_SERVICE              = PROP_PREFIX_AUDIT_SERVER + "service.";
    public static final String PROP_AUTH_TO_LOCAL                            = PROP_PREFIX_AUDIT_SERVER + "auth.to.local";
    public static final String PROP_SUFFIX_ALLOWED_USERS                     = ".allowed.users";
    public static final String PROP_AUDIT_SERVICE_PRINCIPAL                  = "service.kerberos.principal";
    public static final String PROP_AUDIT_SERVICE_KEYTAB                     = "service.kerberos.keytab";

    /**************************************
     AUDIT-SERVER INGESTOR Configuration
     **************************************/
    // kafka configuration for audit ingestor
    public static final String JAAS_KRB5_MODULE                              = "com.sun.security.auth.module.Krb5LoginModule required";
    public static final String JAAS_USE_KEYTAB                               = "useKeyTab=true";
    public static final String JAAS_KEYTAB                                   = "keyTab=\"";
    public static final String JAAS_STOKE_KEY                                = "storeKey=true";
    public static final String JAAS_SERVICE_NAME                             = "serviceName=kafka";
    public static final String JAAS_USER_TICKET_CACHE                        = "useTicketCache=false";
    public static final String JAAS_PRINCIPAL                                = "principal=\"";
    public static final String PROP_KAFKA_PROP_PREFIX                        = "xasecure.audit.destination.kafka";
    public static final String PROP_BOOTSTRAP_SERVERS                        = "kafka.bootstrap.servers";
    public static final String PROP_TOPIC_NAME                               = "kafka.topic.name";
    public static final String PROP_SECURITY_PROTOCOL                        = "kafka.security.protocol";
    public static final String PROP_SASL_MECHANISM                           = "kafka.sasl.mechanism";
    public static final String PROP_KAFKA_TOPIC_INIT_MAX_RETRIES             = "kafka.topic.init.max.retries";
    public static final String PROP_KAFKA_TOPIC_INIT_RETRY_DELAY_MS          = "kafka.topic.init.retry.delay.ms";
    public static final String PROP_REQ_TIMEOUT_MS                           = "kafka.request.timeout.ms";
    public static final String PROP_CONN_MAX_IDEAL_MS                        = "kafka.connections.max.idle.ms";
    public static final String PROP_SASL_JAAS_CONFIG                         = "sasl.jaas.config";
    public static final String PROP_SASL_KERBEROS_SERVICE_NAME               = "sasl.kerberos.service.name";

    // kafka topic - ranger_audits configuration
    public static final String PROP_TOPIC_PARTITIONS                         = "kafka.topic.partitions";
    public static final String PROP_PARTITIONER_CLASS                        = "kafka.partitioner.class";
    public static final String PROP_CONFIGURED_PLUGINS                       = "kafka.configured.plugins";
    public static final String PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN   = "kafka.topic.partitions.per.configured.plugin";
    public static final String PROP_PLUGIN_PARTITION_OVERRIDE_PREFIX         = "kafka.plugin.partition.overrides.";
    public static final String PROP_BUFFER_PARTITIONS                        = "kafka.topic.partitions.buffer";
    public static final String PROP_REPLICATION_FACTOR                       = "kafka.replication.factor";

    // Kafka Topic defaults
    public static final String DEFAULT_TOPIC                                 = "ranger_audits";
    public static final String DEFAULT_SASL_MECHANISM                        = "PLAIN";
    public static final String DEFAULT_SECURITY_PROTOCOL                     = "PLAINTEXT";
    public static final String DEFAULT_SERVICE_NAME                          = "kafka";
    public static final String PROP_SECURITY_PROTOCOL_VALUE                  = "SASL";

    // kafka Offset commit strategies
    public static final String PROP_OFFSET_COMMIT_STRATEGY_MANUAL            = "manual";
    public static final String PROP_OFFSET_COMMIT_STRATEGY_BATCH             = "batch";
    public static final String DEFAULT_OFFSET_COMMIT_STRATEGY                = PROP_OFFSET_COMMIT_STRATEGY_BATCH;
    public static final long   DEFAULT_OFFSET_COMMIT_INTERVAL_MS             = 30000; // 30 seconds
    public static final int    DEFAULT_MAX_POLL_RECORDS                      = 500;   // Kafka default batch size

    // Default configured plugins: each gets allocated partitions from the topic
    public static final String DEFAULT_PARTITIONER_CLASS                     = "org.apache.ranger.audit.producer.kafka.AuditPartitioner";
    public static final String DEFAULT_CONFIGURED_PLUGINS                    = "hdfs,yarn,knox,hiveServer2,hiveMetastore,kafka,hbaseRegional,hbaseMaster,solr,trino,ozone,kudu,nifi";
    public static final short  DEFAULT_REPLICATION_FACTOR                    = 3;
    public static final int    DEFAULT_TOPIC_PARTITIONS                      = 10;
    public static final int    DEFAULT_PARTITIONS_PER_CONFIGURED_PLUGIN      = 3;
    public static final int    DEFAULT_BUFFER_PARTITIONS                     = 9;

    // Hadoop security configuration for UGI
    public static final String PROP_HADOOP_AUTHENTICATION_TYPE               = "hadoop.security.authentication";
    public static final String PROP_HADOOP_AUTH_TYPE_KERBEROS                = "kerberos";
    public static final String PROP_HADOOP_KERBEROS_NAME_RULES               = "hadoop.security.auth_to_local";

    /**************************************
     AUDIT-SERVER DISPATCHER Configuration
     **************************************/
    // kafka configuration for the audit-server dispatcher
    public static final String DEFAULT_PARTITION_ASSIGNMENT_STRATEGY         = "org.apache.kafka.clients.consumer.RangeAssignor";
    public static final String PROP_DISPATCHER_PREFIX                        = "ranger.audit.dispatcher";
    public static final String PROP_DISPATCHER_THREAD_COUNT                  = "thread.count";
    public static final String PROP_DISPATCHER_OFFSET_COMMIT_STRATEGY        = "offset.commit.strategy";
    public static final String PROP_DISPATCHER_OFFSET_COMMIT_INTERVAL        = "offset.commit.interval.ms";
    public static final String PROP_DISPATCHER_MAX_POLL_RECORDS              = "max.poll.records";
    public static final String PROP_DISPATCHER_SESSION_TIMEOUT_MS            = "session.timeout.ms";
    public static final String PROP_DISPATCHER_MAX_POLL_INTERVAL_MS          = "max.poll.interval.ms";
    public static final String PROP_DISPATCHER_HEARTBEAT_INTERVAL_MS         = "heartbeat.interval.ms";
    public static final String PROP_DISPATCHER_PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";
    public static final String PROP_DISPATCHER_TYPE                          = "ranger.audit.dispatcher.type";
    public static final String PROP_DISPATCHER_CLASS                         = "ranger.audit.dispatcher.class";

    // Kafka dispatcher rebalancing timeouts (for subscribe mode)
    public static final int    DEFAULT_SESSION_TIMEOUT_MS                    = 60000;  // 60 seconds - failure detection
    public static final int    DEFAULT_MAX_POLL_INTERVAL_MS                  = 300000; // 5 minutes - max processing time
    public static final int    DEFAULT_HEARTBEAT_INTERVAL_MS                 = 10000;  // 10 seconds - heartbeat frequency
}
