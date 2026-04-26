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

package org.apache.ranger.audit.dispatcher.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Interface for Ranger Kafka dispatchers that consume audit events from Kafka
 * and forward them to various destinations like Solr, HDFS, etc.
 */
public interface AuditDispatcher extends Runnable {
    /**
     * Start consuming messages from Kafka topic and process them.
     * This method should implement the main consumption loop.
     * Implementation should handle subscription to topics and polling for messages.
     */
    @Override
    void run();

    /**
     * Shutdown the dispatcher gracefully.
     * This method should clean up resources and close connections.
     */
    void shutdown();

    /**
     * Get the underlying Kafka dispatcher instance.
     *
     * @return KafkaConsumer instance used by this dispatcher
     */
    KafkaConsumer<String, String> getDispatcher();

    /**
     * Get the topic name this dispatcher is subscribed to.
     *
     * @return Kafka topic name
     */
    String getTopicName();
}
