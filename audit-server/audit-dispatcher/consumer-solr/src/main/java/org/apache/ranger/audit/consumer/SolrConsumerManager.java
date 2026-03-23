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

package org.apache.ranger.audit.consumer;

import org.apache.ranger.audit.consumer.kafka.AuditConsumer;
import org.apache.ranger.audit.consumer.kafka.AuditConsumerRegistry;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.server.SolrConsumerConfig;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Spring component that manages the lifecycle of Solr consumer threads.
 * This manager:
 * - Initializes the consumer registry
 * - Creates Solr consumer instances
 * - Starts consumer threads
 * - Handles graceful shutdown
 */
@Component
public class SolrConsumerManager {
    private static final Logger LOG = LoggerFactory.getLogger(SolrConsumerManager.class);

    private final AuditConsumerRegistry consumerRegistry = AuditConsumerRegistry.getInstance();
    private final List<AuditConsumer>   consumers        = new ArrayList<>();
    private final List<Thread>          consumerThreads  = new ArrayList<>();

    @PostConstruct
    public void init() {
        LOG.info("==> SolrConsumerManager.init()");

        try {
            SolrConsumerConfig config = SolrConsumerConfig.getInstance();
            Properties props = config.getProperties();

            if (props == null) {
                LOG.error("Configuration properties are null");
                throw new RuntimeException("Failed to load configuration");
            }

            // Initialize and register Solr Consumer
            initializeConsumerClasses(props, AuditServerConstants.PROP_KAFKA_PROP_PREFIX);

            // Create consumers from registry
            List<AuditConsumer> createdConsumers = consumerRegistry.createConsumers(props, AuditServerConstants.PROP_KAFKA_PROP_PREFIX);
            consumers.addAll(createdConsumers);

            if (consumers.isEmpty()) {
                LOG.warn("No consumers were created! Verify that xasecure.audit.destination.solr=true");
            } else {
                LOG.info("Created {} Solr consumer(s)", consumers.size());

                // Start consumer threads
                startConsumers();
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize SolrConsumerManager", e);
            throw new RuntimeException("Failed to initialize SolrConsumerManager", e);
        }

        LOG.info("<== SolrConsumerManager.init() - {} consumer thread(s) started", consumerThreads.size());
    }

    private void initializeConsumerClasses(Properties props, String propPrefix) {
        LOG.info("==> SolrConsumerManager.initializeConsumerClasses()");

        // Get consumer classes from configuration
        String clsStr = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_CLASSES,
                "org.apache.ranger.audit.consumer.kafka.AuditSolrConsumer");

        String[] solrConsumerClasses = clsStr.split(",");

        LOG.info("Initializing {} consumer class(es)", solrConsumerClasses.length);

        for (String solrConsumerClassName : solrConsumerClasses) {
            solrConsumerClassName = solrConsumerClassName.trim();

            if (solrConsumerClassName.isEmpty()) {
                continue;
            }

            try {
                Class<?> consumerClass = Class.forName(solrConsumerClassName);
                LOG.info("Successfully initialized consumer class: {}", consumerClass.getName());
            } catch (ClassNotFoundException e) {
                LOG.error("Consumer class not found: {}. Ensure the class is on the classpath.", solrConsumerClassName, e);
            } catch (Exception e) {
                LOG.error("Error initializing consumer class: {}", solrConsumerClassName, e);
            }
        }

        LOG.info("Registered consumer factories: {}", consumerRegistry.getRegisteredDestinationTypes());
        LOG.info("<== SolrConsumerManager.initializeConsumerClasses()");
    }

    /**
     * Start all consumer threads
     */
    private void startConsumers() {
        LOG.info("==> SolrConsumerManager.startConsumers()");

        logSolrConsumerStartup();

        for (AuditConsumer consumer : consumers) {
            try {
                String consumerName = consumer.getClass().getSimpleName();
                Thread consumerThread = new Thread(consumer, consumerName);
                consumerThread.setDaemon(true);
                consumerThread.start();
                consumerThreads.add(consumerThread);

                LOG.info("Started {} thread [Thread-ID: {}, Thread-Name: '{}']",
                        consumerName, consumerThread.getId(), consumerThread.getName());
            } catch (Exception e) {
                LOG.error("Error starting consumer: {}", consumer.getClass().getSimpleName(), e);
            }
        }

        LOG.info("<== SolrConsumerManager.startConsumers() - {} thread(s) started", consumerThreads.size());
    }

    /**
     * Log startup banner with consumer information
     */
    private void logSolrConsumerStartup() {
        LOG.info("################## SOLR CONSUMER SERVICE STARTUP ######################");

        if (consumers.isEmpty()) {
            LOG.warn("WARNING: No Solr consumers are enabled!");
            LOG.warn("Verify: xasecure.audit.destination.solr=true in configuration");
        } else {
            AuditServerLogFormatter.LogBuilder builder = AuditServerLogFormatter.builder("Solr Consumer Status");

            for (AuditConsumer consumer : consumers) {
                String consumerType = consumer.getClass().getSimpleName();
                builder.add(consumerType, "ENABLED");
                builder.add("Topic", consumer.getTopicName());
            }

            builder.logInfo(LOG);
            LOG.info("Starting {} Solr consumer thread(s)...", consumers.size());
        }
        LOG.info("########################################################################");
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("==> SolrConsumerManager.shutdown()");

        // Shutdown all consumers
        for (AuditConsumer consumer : consumers) {
            try {
                LOG.info("Shutting down consumer: {}", consumer.getClass().getSimpleName());
                consumer.shutdown();
                LOG.info("Consumer shutdown completed: {}", consumer.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.error("Error shutting down consumer: {}", consumer.getClass().getSimpleName(), e);
            }
        }

        // Wait for threads to terminate
        for (Thread thread : consumerThreads) {
            if (thread.isAlive()) {
                try {
                    LOG.info("Waiting for thread to terminate: {}", thread.getName());
                    thread.join(10000); // Wait up to 10 seconds
                    if (thread.isAlive()) {
                        LOG.warn("Thread did not terminate within 10 seconds: {}", thread.getName());
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for thread to terminate: {}", thread.getName(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        consumers.clear();
        consumerThreads.clear();
        consumerRegistry.clearActiveConsumers();

        LOG.info("<== SolrConsumerManager.shutdown() - All Solr consumers stopped");
    }
}
