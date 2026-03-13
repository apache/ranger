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
 * Configuration class for Solr Consumer Service.
 * Loads Solr consumer-specific configuration files.
 */
public class SolrConsumerConfig extends AuditConfig {
    private static final    Logger             LOG                   = LoggerFactory.getLogger(SolrConsumerConfig.class);
    private static final    String             CONFIG_FILE_PATH      = "conf/ranger-audit-consumer-solr-site.xml";
    private static volatile SolrConsumerConfig sInstance;

    private SolrConsumerConfig() {
        super();
        addSolrConsumerResources();
    }

    public static SolrConsumerConfig getInstance() {
        SolrConsumerConfig ret = SolrConsumerConfig.sInstance;

        if (ret == null) {
            synchronized (SolrConsumerConfig.class) {
                ret = SolrConsumerConfig.sInstance;

                if (ret == null) {
                    ret = new SolrConsumerConfig();
                    SolrConsumerConfig.sInstance = ret;
                }
            }
        }

        return ret;
    }

    private boolean addSolrConsumerResources() {
        LOG.debug("==> SolrConsumerConfig.addSolrConsumerResources()");

        boolean ret = true;

        // Load ranger-audit-consumer-solr-site.xml
        if (!addAuditResource(CONFIG_FILE_PATH, true)) {
            LOG.error("Could not load required configuration: {}", CONFIG_FILE_PATH);
            ret = false;
        }

        LOG.debug("<== SolrConsumerConfig.addSolrConsumerResources(), result={}", ret);

        return ret;
    }
}
