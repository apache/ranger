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

    public static final String AUDIT_SERVER_APPNAME                        = "ranger-audit";
    public static final String AUDIT_SERVER_PROP_PREFIX                    = "ranger.audit.service.";
    public static final String JAAS_KRB5_MODULE                            = "com.sun.security.auth.module.Krb5LoginModule required";
    public static final String JAAS_USE_KEYTAB                             = "useKeyTab=true";
    public static final String JAAS_KEYTAB                                 = "keyTab=\"";
    public static final String JAAS_STOKE_KEY                              = "storeKey=true";
    public static final String JAAS_SERVICE_NAME                           = "serviceName=kafka";
    public static final String JAAS_USER_TICKET_CACHE                      = "useTicketCache=false";
    public static final String JAAS_PRINCIPAL                              = "principal=\"";
    public static final String PROP_BOOTSTRAP_SERVERS                      = "bootstrap.servers";
    public static final String PROP_TOPIC_NAME                             = "topic.name";
    public static final String PROP_SECURITY_PROTOCOL                      = "security.protocol";
    public static final String PROP_SASL_MECHANISM                         = "sasl.mechanism";
    public static final String PROP_SASL_JAAS_CONFIG                       = "sasl.jaas.config";
    public static final String PROP_SASL_KERBEROS_SERVICE_NAME             = "sasl.kerberos.service.name";
    public static final String PROP_REQ_TIMEOUT_MS                         = "request.timeout.ms";
    public static final String PROP_CONN_MAX_IDEAL_MS                      = "connections.max.idle.ms";
    public static final String PROP_SOLR_DEST_PREFIX                       = "solr";
    public static final String PROP_HDFS_DEST_PREFIX                       = "hdfs";
    public static final String PROP_CONSUMER_THREAD_COUNT                  = "consumer.thread.count";
    public static final String PROP_CONSUMER_OFFSET_COMMIT_STRATEGY        = "consumer.offset.commit.strategy";
    public static final String PROP_CONSUMER_OFFSET_COMMIT_INTERVAL        = "consumer.offset.commit.interval.ms";
    public static final String PROP_CONSUMER_MAX_POLL_RECORDS              = "consumer.max.poll.records";
    public static final String PROP_CONSUMER_SESSION_TIMEOUT_MS            = "consumer.session.timeout.ms";
    public static final String PROP_CONSUMER_MAX_POLL_INTERVAL_MS          = "consumer.max.poll.interval.ms";
    public static final String PROP_CONSUMER_HEARTBEAT_INTERVAL_MS         = "consumer.heartbeat.interval.ms";
    public static final String PROP_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "consumer.partition.assignment.strategy";
    public static final String PROP_AUDIT_SERVICE_PRINCIPAL                = "kerberos.principal";
    public static final String PROP_AUDIT_SERVICE_KEYTAB                   = "kerberos.keytab";
    public static final String PROP_KAFKA_STARTUP_MAX_RETRIES              = "kafka.startup.max.retries";
    public static final String PROP_KAFKA_STARTUP_RETRY_DELAY_MS           = "kafka.startup.retry.delay.ms";

    // ranger_audits topic partition management
    public static final String PROP_CONFIGURED_APP_IDS                  = "configured.appIds";
    public static final String PROP_REPLICATION_FACTOR                  = "replication.factor";
    public static final String PROP_PARTITION_OVERRIDE_PREFIX           = "partitioner.override";
    public static final String PROP_MIN_PARTITIONS_THRESHOLD            = "min.partitions.threshold";
    public static final String DEFAULT_CONFIGURED_APP_IDS               = "hdfs,hiveServer2,hiveMetaStore,hbaseMaster,hbaseRegional,knox,kms,kafka,solr,impala";
    public static final int    DEFAULT_PARTITIONS_PER_APP_ID            = 2;
    public static final int    DEFAULT_MIN_PARTITIONS_THRESHOLD         = 20;
    public static final short  DEFAULT_REPLICATION_FACTOR               = 3;



    // Hadoop security configuration
    public static final String PROP_HADOOP_AUTHENTICATION_TYPE          = "hadoop.security.authentication";
    public static final String PROP_HADOOP_AUTH_TYPE_KERBEROS           = "kerberos";
    public static final String PROP_HADOOP_KERBEROS_NAME_RULES          = "hadoop.security.auth_to_local";

    // Kafka Topic and defaults
    public static final String DEFAULT_TOPIC                            = "ranger_audits";
    public static final String DEFAULT_SASL_MECHANISM                   = "PLAIN";
    public static final String DEFAULT_SECURITY_PROTOCOL                = "PLAINTEXT";
    public static final String DEFAULT_SERVICE_NAME                     = "kafka";
    public static final String DEFAULT_RANGER_AUDIT_HDFS_CONSUMER_GROUP = "ranger_audit_hdfs_consumer_group";
    public static final String DEFAULT_RANGER_AUDIT_SOLR_CONSUMER_GROUP = "ranger_audit_solr_consumer_group";
    public static final String PROP_SECURITY_PROTOCOL_VALUE             = "SASL";

    // Offset commit strategies
    public static final String PROP_OFFSET_COMMIT_STRATEGY_MANUAL       = "manual";
    public static final String PROP_OFFSET_COMMIT_STRATEGY_BATCH        = "batch";
    public static final String DEFAULT_OFFSET_COMMIT_STRATEGY           = PROP_OFFSET_COMMIT_STRATEGY_BATCH;
    public static final long   DEFAULT_OFFSET_COMMIT_INTERVAL_MS        = 30000; // 30 seconds
    public static final int    DEFAULT_MAX_POLL_RECORDS                 = 500;   // Kafka default batch size

    // Kafka consumer rebalancing timeouts (for subscribe mode)
    public static final int    DEFAULT_SESSION_TIMEOUT_MS               = 60000;  // 60 seconds - failure detection
    public static final int    DEFAULT_MAX_POLL_INTERVAL_MS             = 300000; // 5 minutes - max processing time
    public static final int    DEFAULT_HEARTBEAT_INTERVAL_MS            = 10000;  // 10 seconds - heartbeat frequency

    // Kafka consumer partition assignment strategy
    public static final String DEFAULT_PARTITION_ASSIGNMENT_STRATEGY    = "org.apache.kafka.clients.consumer.RangeAssignor";
}
