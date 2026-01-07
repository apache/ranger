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

package org.apache.ranger.audit.destination.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Interface for Ranger Kafka consumers that consume audit events from Kafka
 * and forward them to various destinations like Solr, HDFS, etc.
 */
public interface AuditConsumer extends Runnable {
    /**
     * Initialize the consumer with the given properties and property prefix.
     * This method should set up the destination handler and configure the Kafka consumer.
     *
     * @param props Configuration properties
     * @param propPrefix Property prefix for consumer-specific configuration
     * @throws Exception if initialization fails
     */
    void init(Properties props, String propPrefix) throws Exception;

    /**
     * Start consuming messages from Kafka topic and process them.
     * This method should implement the main consumption loop.
     * Implementation should handle subscription to topics and polling for messages.
     */
    @Override
    void run();

    /**
     * Get the underlying Kafka consumer instance.
     *
     * @return KafkaConsumer instance used by this consumer
     */
    KafkaConsumer<String, String> getKafkaConsumer();

    /**
     * Process a single audit message received from Kafka.
     * Implementation should handle the specific logic for forwarding
     * the message to the appropriate destination (Solr, HDFS, etc.).
     *
     * @param audit The audit message in JSON format
     * @throws Exception if message processing fails
     */
    void processMessage(String audit) throws Exception;

    /**
     * Get the topic name this consumer is subscribed to.
     *
     * @return Kafka topic name
     */
    String getTopicName();

    /**
     * Shutdown the consumer gracefully.
     * This method should clean up resources and close connections.
     */
    void shutdown();
}
