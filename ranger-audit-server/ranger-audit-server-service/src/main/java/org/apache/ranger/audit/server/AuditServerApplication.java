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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for Ranger Audit Server Service.
 *
 * This service receives audit events from Ranger plugins via REST API
 * and produces them to Kafka for consumption by downstream services.
 * It uses embedded Tomcat server for the REST API and health check endpoints.
 */
public class AuditServerApplication {
    private static final Logger LOG = LoggerFactory.getLogger(AuditServerApplication.class);

    private static final String APP_NAME      = "ranger-audit-server";
    private static final String CONFIG_PREFIX = "ranger.audit.service.";

    private AuditServerApplication() {
    }

    public static void main(String[] args) {
        LOG.info("==========================================================================");
        LOG.info("==> Starting Ranger Audit Server Service");
        LOG.info("==========================================================================");

        try {
            Configuration config = AuditServerConfig.getInstance();

            LOG.info("Configuration loaded successfully");

            EmbeddedServer server = new EmbeddedServer(config, APP_NAME, CONFIG_PREFIX);
            server.start();

            LOG.info("==> Ranger Audit Server Service Started Successfully");
        } catch (Exception e) {
            LOG.error("Failed to start Ranger Audit Server Service", e);
            System.exit(1);
        }
    }
}
