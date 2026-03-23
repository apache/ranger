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

package org.apache.ranger.audit.consumer.kafka;

import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing audit consumer factories and instances.
 * Supports dynamic consumer registration and creation based on configuration.
 */
public class AuditConsumerRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(AuditConsumerRegistry.class);

    private static final AuditConsumerRegistry      auditConsumerRegistry = new AuditConsumerRegistry();
    private final Map<String, AuditConsumerFactory> consumerFactories     = new ConcurrentHashMap<>();
    private final Map<String, AuditConsumer>        activeConsumers       = new ConcurrentHashMap<>();

    private AuditConsumerRegistry() {
        LOG.debug("AuditConsumerRegistry instance created");
    }

    public static AuditConsumerRegistry getInstance() {
        return auditConsumerRegistry;
    }

    /**
     * Register a consumer factory for a specific destination type.
     * This method can be called early in the application lifecycle (static initializers)
     * to register consumers before the AuditMessageQueue is initialized.
     *
     * @param destinationType The destination type identifier (e.g., "solr", "hdfs", "elasticsearch")
     * @param factory The factory that creates consumer instances
     */
    public void registerFactory(String destinationType, AuditConsumerFactory factory) {
        if (destinationType == null || destinationType.trim().isEmpty()) {
            LOG.warn("Attempted to register factory with null or empty destination type");
            return;
        }

        if (factory == null) {
            LOG.warn("Attempted to register null factory for destination type: {}", destinationType);
            return;
        }

        AuditConsumerFactory existing = consumerFactories.put(destinationType, factory);
        if (existing != null) {
            LOG.warn("Replaced existing factory for destination type: {}", destinationType);
        } else {
            LOG.info("Registered consumer factory for destination type: {}", destinationType);
        }
    }

    /**
     * Create consumer instances for all enabled destinations based on configuration.
     *
     * @param props Configuration properties
     * @param propPrefix Property prefix for Kafka configuration
     * @return List of created consumer instances
     */
    public List<AuditConsumer> createConsumers(Properties props, String propPrefix) {
        LOG.info("==> AuditConsumerRegistry.createConsumers()");

        List<AuditConsumer> consumers = new ArrayList<>();

        for (Map.Entry<String, AuditConsumerFactory> entry : consumerFactories.entrySet()) {
            String               destinationType = entry.getKey();
            AuditConsumerFactory factory         = entry.getValue();
            String               destPropPrefix  = AuditProviderFactory.AUDIT_DEST_BASE + "." + destinationType;
            boolean              isEnabled       = MiscUtil.getBooleanProperty(props, destPropPrefix, false);

            if (isEnabled) {
                try {
                    LOG.info("Creating consumer for enabled destination: {}", destinationType);
                    AuditConsumer consumer = factory.createConsumer(props, propPrefix);

                    if (consumer != null) {
                        consumers.add(consumer);
                        activeConsumers.put(destinationType, consumer);
                        LOG.info("Successfully created consumer for destination: {}", destinationType);
                    } else {
                        LOG.warn("Factory returned null consumer for destination: {}", destinationType);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to create consumer for destination: {}", destinationType, e);
                }
            } else {
                LOG.debug("Destination '{}' is disabled (property: {} = false)", destinationType, destPropPrefix);
            }
        }

        LOG.info("<== AuditConsumerRegistry.createConsumers(): Created {} consumers out of {} registered factories", consumers.size(), consumerFactories.size());

        return consumers;
    }

    public Collection<AuditConsumer> getActiveConsumers() {
        return activeConsumers.values();
    }

    public Collection<String> getRegisteredDestinationTypes() {
        return consumerFactories.keySet();
    }

    /**
     * Clear all active consumer references.
     * Called during shutdown after consumers have been stopped.
     */
    public void clearActiveConsumers() {
        LOG.debug("Clearing {} active consumer references", activeConsumers.size());
        activeConsumers.clear();
    }

    public int getFactoryCount() {
        return consumerFactories.size();
    }
}
