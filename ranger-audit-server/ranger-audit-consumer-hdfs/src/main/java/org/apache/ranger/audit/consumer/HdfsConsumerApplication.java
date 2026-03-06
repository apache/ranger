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
import org.apache.ranger.audit.server.HdfsConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for Audit Consumer HDFS Service.
 * This service consumes audit events from Kafka and writes them to HDFS.
 * It uses embedded Tomcat server for lifecycle management and health check endpoints.
 */
public class HdfsConsumerApplication {
    private static final Logger LOG           = LoggerFactory.getLogger(HdfsConsumerApplication.class);
    private static final String APP_NAME      = "ranger-audit-consumer-hdfs";
    private static final String CONFIG_PREFIX = "ranger.audit.consumer.hdfs.service.";

    private HdfsConsumerApplication() {
    }

    public static void main(String[] args) {
        LOG.info("==========================================================================");
        LOG.info("==> Starting Ranger Audit Consumer HDFS Service");
        LOG.info("==========================================================================");

        try {
            // Load configuration (includes core-site.xml, hdfs-site.xml, and ranger-audit-consumer-hdfs-site.xml)
            Configuration config = HdfsConsumerConfig.getInstance();

            EmbeddedServer server = new EmbeddedServer(config, APP_NAME, CONFIG_PREFIX);
            server.start();

            LOG.info("<== Ranger Audit Consumer HDFS Service Started Successfully");
        } catch (Exception e) {
            LOG.error("<== Failed to start Ranger Audit Consumer HDFS Service", e);
            System.exit(1);
        }
    }
}
