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

package org.apache.ranger.server.tomcat;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration helper for prefixed Hadoop Configuration (e.g. ranger.{service}.*).
 */
public final class EmbeddedServerConfigUtil {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServerConfigUtil.class);

    private final Configuration configuration;
    private final String        configPrefix;

    public EmbeddedServerConfigUtil(Configuration configuration, String configPrefix) {
        if (Objects.isNull(configuration) || Objects.isNull(configPrefix)) {
            throw new IllegalArgumentException("configuration and configPrefix must not be null");
        }

        this.configuration = configuration;
        this.configPrefix  = configPrefix;
    }

    public String getConfigPrefix() {
        return configPrefix;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getConfig(String key, String defaultValue) {
        String ret = getConfig(key);

        if (ret == null) {
            ret = defaultValue;
        }

        return ret;
    }

    public int getIntConfig(String key, int defaultValue) {
        int    ret    = defaultValue;
        String retStr = getConfig(key);

        try {
            if (retStr != null) {
                ret = Integer.parseInt(retStr);
            }
        } catch (Exception err) {
            LOG.error("{} can't be parsed to int. Reason: {}", retStr, err);
        }

        return ret;
    }

    public long getLongConfig(String key, long defaultValue) {
        long   ret    = defaultValue;
        String retStr = getConfig(key);

        try {
            if (retStr != null) {
                ret = Long.parseLong(retStr);
            }
        } catch (Exception err) {
            LOG.error("{} can't be parsed to long. Reason: {}", retStr, err);
        }

        return ret;
    }

    public boolean getBooleanConfig(String key, boolean defaultValue) {
        boolean ret    = defaultValue;
        String  retStr = getConfig(key);

        try {
            if (retStr != null) {
                ret = Boolean.parseBoolean(retStr);
            }
        } catch (Exception err) {
            LOG.error("{} can't be parsed to boolean. Reason: {}", retStr, err);
        }

        return ret;
    }

    public String getConfig(String key) {
        String propertyWithPrefix = configPrefix + key;
        String value              = configuration.get(propertyWithPrefix);

        if (value == null) {
            value = System.getProperty(propertyWithPrefix);
        }

        if (value == null) {
            value = configuration.get(key);
        }

        if (value == null) {
            value = System.getProperty(key);
        }

        return value;
    }

    public Iterator<Map.Entry<String, String>> iterator() {
        return configuration.iterator();
    }
}
