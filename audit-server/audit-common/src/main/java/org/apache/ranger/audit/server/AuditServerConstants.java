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

    // Dynamic partition plan (Kafka compacted registry topic)
    public static final String PROP_PARTITION_PLAN_TOPIC                     = "kafka.partition.plan.topic";
    public static final String PROP_PARTITION_PLAN_REFRESH_INTERVAL_MS       = "kafka.partition.plan.refresh.interval.ms";
    public static final String PROP_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS  = "kafka.partition.plan.consumer.poll.timeout.ms";
    public static final String PROP_PARTITION_PLAN_DYNAMIC_ENABLED           = "kafka.partition.plan.dynamic.enabled";
    public static final String DEFAULT_PARTITION_PLAN_TOPIC                  = "ranger_audit_partition_plan";
    public static final int    DEFAULT_PARTITION_PLAN_REFRESH_INTERVAL_MS    = 30000;
    public static final int    DEFAULT_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS = 500;
    public static final int    PARTITION_PLAN_TOPIC_PARTITION_COUNT          = 1;
    public static final String KAFKA_TOPIC_CLEANUP_POLICY_COMPACT            = "compact";

    // Kafka producer tuning (ranger.audit.ingestor.kafka.producer.*)
    public static final String PROP_KAFKA_PRODUCER_PREFIX                    = "kafka.producer.";
    public static final String PROP_PRODUCER_BATCH_SIZE                        = "batch.size";
    public static final String PROP_PRODUCER_LINGER_MS                         = "linger.ms";
    public static final String PROP_PRODUCER_BUFFER_MEMORY                     = "buffer.memory";
    public static final String PROP_PRODUCER_COMPRESSION_TYPE                  = "compression.type";
    public static final String PROP_PRODUCER_DELIVERY_TIMEOUT_MS               = "delivery.timeout.ms";
    public static final String PROP_PRODUCER_MAX_REQUEST_SIZE                  = "max.request.size";
    public static final String PROP_PRODUCER_MAX_BLOCK_MS                      = "max.block.ms";
    public static final String PROP_PRODUCER_BATCH_SEND_TIMEOUT_MS             = "batch.send.timeout.ms";

    // Optional topic-level configs applied at topic create (Kafka Admin)
    public static final String PROP_TOPIC_RETENTION_MS                         = "kafka.topic.retention.ms";
    public static final String PROP_TOPIC_COMPRESSION_TYPE                     = "kafka.topic.compression.type";
    public static final String PROP_TOPIC_MIN_INSYNC_REPLICAS                  = "kafka.topic.min.insync.replicas";

    // Kafka producer defaults (high-volume audit JSON; idempotent + acks=all)
    public static final int    DEFAULT_PRODUCER_BATCH_SIZE                    = 131072;    // 128 KiB
    public static final int    DEFAULT_PRODUCER_LINGER_MS                      = 20;
    public static final long   DEFAULT_PRODUCER_BUFFER_MEMORY                  = 134217728L; // 128 MiB
    public static final String DEFAULT_PRODUCER_COMPRESSION_TYPE               = "lz4";
    public static final int    DEFAULT_PRODUCER_DELIVERY_TIMEOUT_MS            = 120000;
    public static final int    DEFAULT_PRODUCER_MAX_REQUEST_SIZE                 = 1048576;   // 1 MiB
    public static final int    DEFAULT_PRODUCER_MAX_BLOCK_MS                     = 60000;
    public static final int    DEFAULT_PRODUCER_BATCH_SEND_TIMEOUT_MS          = 30000;
    public static final int    DEFAULT_PRODUCER_REQUEST_TIMEOUT_MS             = 60000;

    // Optional topic defaults when properties are set (production guidance; RF must allow min ISR)
    public static final String DEFAULT_TOPIC_RETENTION_MS                      = "604800000"; // 7 days
    public static final String DEFAULT_TOPIC_COMPRESSION_TYPE                    = "lz4";
    public static final String DEFAULT_TOPIC_MIN_INSYNC_REPLICAS                 = "2";

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

    // Empty by default: operators opt in via XML (static) or REST (dynamic). See ranger-audit-ingestor-site.xml.
    public static final String DEFAULT_PARTITIONER_CLASS                     = "org.apache.ranger.audit.producer.kafka.AuditPartitioner";
    public static final String DEFAULT_CONFIGURED_PLUGINS                    = "";
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
    public static final String DEFAULT_PARTITION_ASSIGNMENT_STRATEGY         = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor";
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
