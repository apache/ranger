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

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.server.EmbeddedServer;
import org.apache.ranger.audit.server.SolrConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for Audit Consumer Solr Service.
 * This service consumes audit events from Kafka and indexes them into Solr
 * It uses embedded Tomcat server for lifecycle management and health check endpoints.
 */
public class SolrConsumerApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SolrConsumerApplication.class);

    private static final String APP_NAME      = "ranger-audit-consumer-solr";
    private static final String CONFIG_PREFIX = "ranger.audit.consumer.solr.service.";

    private SolrConsumerApplication() {
    }

    public static void main(String[] args) {
        LOG.info("==========================================================================");
        LOG.info("==> Starting Ranger Audit Consumer Solr Service");
        LOG.info("==========================================================================");

        try {
            // Load configuration
            Configuration config = SolrConsumerConfig.getInstance();

            LOG.info("Configuration loaded successfully");

            // Create and start embedded server
            // The server will load Spring context which initializes SolrConsumerManager
            // which then creates and starts the Solr consumer threads
            EmbeddedServer server = new EmbeddedServer(config, APP_NAME, CONFIG_PREFIX);
            server.start();

            LOG.info("==> Ranger Audit Consumer Solr Service Started Successfully");
        } catch (Exception e) {
            LOG.error("Failed to start Ranger Audit Consumer Solr Service", e);
            System.exit(1);
        }
    }
}
