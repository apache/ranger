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
 * Loads HDFS consumer-specific configuration files including Hadoop configs.
 */
public class HdfsConsumerConfig extends AuditConfig {
    private static final    Logger             LOG                   = LoggerFactory.getLogger(HdfsConsumerConfig.class);
    private static final    String             CONFIG_FILE_PATH      = "conf/ranger-audit-consumer-hdfs-site.xml";
    private static final    String             CORE_SITE_FILE_PATH   = "conf/core-site.xml";
    private static final    String             HDFS_SITE_FILE_PATH   = "conf/hdfs-site.xml";
    private static volatile HdfsConsumerConfig sInstance;

    private HdfsConsumerConfig() {
        super();
        addHdfsConsumerResources();
    }

    public static HdfsConsumerConfig getInstance() {
        HdfsConsumerConfig ret = HdfsConsumerConfig.sInstance;

        if (ret == null) {
            synchronized (HdfsConsumerConfig.class) {
                ret = HdfsConsumerConfig.sInstance;

                if (ret == null) {
                    ret = new HdfsConsumerConfig();
                    HdfsConsumerConfig.sInstance = ret;
                }
            }
        }

        return ret;
    }

    private boolean addHdfsConsumerResources() {
        LOG.debug("==> HdfsConsumerConfig.addHdfsConsumerResources()");

        boolean ret = true;

        if (!addAuditResource(CORE_SITE_FILE_PATH, false)) {
            LOG.warn("Could not load required configuration: {}", CORE_SITE_FILE_PATH);
            ret = false;
        }

        if (!addAuditResource(HDFS_SITE_FILE_PATH, true)) {
            LOG.error("Could not load required configuration: {}", HDFS_SITE_FILE_PATH);
            ret = false;
        }

        if (!addAuditResource(CONFIG_FILE_PATH, true)) {
            LOG.error("Could not load required configuration: {}", CONFIG_FILE_PATH);
            ret = false;
        }

        LOG.debug("<== HdfsConsumerConfig.addHdfsConsumerResources(), result={}", ret);

        return ret;
    }

    /**
     * Get Hadoop configuration properties (from core-site.xml and hdfs-site.xml) with a specific prefix.
     * @param prefix The prefix to add to each property name ("xasecure.audit.destination.hdfs.config.")
     * @return Properties from core-site.xml and hdfs-site.xml with the specified prefix
     */
    public java.util.Properties getHadoopPropertiesWithPrefix(String prefix) {
        LOG.debug("==> HdfsConsumerConfig.getHadoopPropertiesWithPrefix(prefix={})", prefix);

        java.util.Properties prefixedProps = new java.util.Properties();
        int propsAdded = 0;

        try {
            // Load core-site.xml separately to get pure Hadoop security properties
            org.apache.hadoop.conf.Configuration coreSite = new org.apache.hadoop.conf.Configuration(false);
            coreSite.addResource(CORE_SITE_FILE_PATH);

            for (java.util.Map.Entry<String, String> entry : coreSite) {
                String propName  = entry.getKey();
                String propValue = entry.getValue();

                if (propValue != null && !propValue.trim().isEmpty()) {
                    prefixedProps.setProperty(prefix + propName, propValue);
                    LOG.trace("Added from core-site.xml: {} = {}", propName, propValue);
                    propsAdded++;
                }
            }

            // Load hdfs-site.xml separately to get pure HDFS client properties
            org.apache.hadoop.conf.Configuration hdfsSite = new org.apache.hadoop.conf.Configuration(false);
            hdfsSite.addResource(HDFS_SITE_FILE_PATH);

            for (java.util.Map.Entry<String, String> entry : hdfsSite) {
                String propName  = entry.getKey();
                String propValue = entry.getValue();

                if (propValue != null && !propValue.trim().isEmpty()) {
                    prefixedProps.setProperty(prefix + propName, propValue);
                    LOG.trace("Added from hdfs-site.xml: {} = {}", propName, propValue);
                    propsAdded++;
                }
            }

            LOG.debug("<== HdfsConsumerConfig.getHadoopPropertiesWithPrefix(): Added {} Hadoop properties with prefix '{}'", propsAdded, prefix);
        } catch (Exception e) {
            LOG.error("Failed to load Hadoop properties from {} and {}", CORE_SITE_FILE_PATH, HDFS_SITE_FILE_PATH, e);
        }

        return prefixedProps;
    }

    /**
     * Get core-site.xml Configuration for UGI initialization.
     * @return Configuration loaded from core-site.xml
     */
    public org.apache.hadoop.conf.Configuration getCoreSiteConfiguration() {
        LOG.debug("==> HdfsConsumerConfig.getCoreSiteConfiguration()");

        org.apache.hadoop.conf.Configuration coreSite = new org.apache.hadoop.conf.Configuration(false);
        coreSite.addResource(CORE_SITE_FILE_PATH);

        LOG.debug("<== HdfsConsumerConfig.getCoreSiteConfiguration(): authentication={}", coreSite.get("hadoop.security.authentication"));

        return coreSite;
    }
}
