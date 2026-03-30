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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for Solr Dispatcher Service.
 * Loads Solr dispatcher-specific configuration files.
 */
public class SolrDispatcherConfig extends AuditConfig {
    private static final    Logger               LOG                     = LoggerFactory.getLogger(SolrDispatcherConfig.class);
    private static final    String               COMMON_CONFIG_FILE_PATH = "ranger-audit-dispatcher-site.xml";
    private static final    String               SOLR_CONFIG_FILE_PATH   = "ranger-audit-dispatcher-solr-site.xml";
    private static volatile SolrDispatcherConfig sInstance;

    private SolrDispatcherConfig() {
        super();
        addSolrDispatcherResources();
    }

    public static SolrDispatcherConfig getInstance() {
        SolrDispatcherConfig ret = SolrDispatcherConfig.sInstance;

        if (ret == null) {
            synchronized (SolrDispatcherConfig.class) {
                ret = SolrDispatcherConfig.sInstance;

                if (ret == null) {
                    ret = new SolrDispatcherConfig();
                    SolrDispatcherConfig.sInstance = ret;
                }
            }
        }

        return ret;
    }

    private boolean addSolrDispatcherResources() {
        LOG.debug("==> SolrConsumerConfig.addSolrDispatcherResources()");

        boolean ret = true;

        // Load common configuration
        if (!addAuditResource(COMMON_CONFIG_FILE_PATH, true)) {
            LOG.error("Could not load required common configuration: {}", COMMON_CONFIG_FILE_PATH);
            ret = false;
        }

        // Load Solr-specific configuration
        if (!addAuditResource(SOLR_CONFIG_FILE_PATH, true)) {
            LOG.error("Could not load required Solr configuration: {}", SOLR_CONFIG_FILE_PATH);
            ret = false;
        }

        LOG.debug("<== SolrConsumerConfig.addSolrDispatcherResources(), result={}", ret);

        return ret;
    }
}
