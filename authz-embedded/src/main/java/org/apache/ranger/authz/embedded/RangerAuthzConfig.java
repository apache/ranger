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

package org.apache.ranger.authz.embedded;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class RangerAuthzConfig {
    public static final String PROP_PREFIX_INIT_SERVICES = "ranger.authz.init.services";
    public static final String PROP_PREFIX_DEFAULT       = "ranger.authz.default.";
    public static final String PROP_PREFIX_AUDIT         = "ranger.authz.audit.";
    public static final String PROP_PREFIX_SERVICE       = "ranger.authz.service.";
    public static final String PROP_PREFIX_SERVICE_TYPE  = "ranger.authz.servicetype.";

    private final Properties properties;

    public RangerAuthzConfig(Properties properties) {
        this.properties = properties;
    }

    public String[] getInitServices() {
        String initServices = properties.getProperty(PROP_PREFIX_INIT_SERVICES);

        if (StringUtils.isBlank(initServices)) {
            return new String[0];
        }

        return initServices.split(",");
    }

    public Properties getAuditProperties() {
        Properties ret = new Properties();

        for (String propName : properties.stringPropertyNames()) {
            if (propName.startsWith(PROP_PREFIX_AUDIT)) {
                String propValue      = properties.getProperty(propName);
                String propSuffix     = propName.substring(PROP_PREFIX_AUDIT.length());
                String pluginPropName = "xasecure.audit." + propSuffix;

                ret.setProperty(pluginPropName, propValue);
            }
        }

        return ret;
    }

    public Properties getServiceProperties(String serviceName, String serviceType) {
        Properties ret = new Properties();

        if (StringUtils.isBlank(serviceType)) {
            serviceType = getServiceTypeForService(serviceName);
        }

        String pluginPropPrefix = "ranger.plugin." + serviceType + ".";

        // collect default properties
        for (String propName : properties.stringPropertyNames()) {
            if (propName.startsWith(PROP_PREFIX_DEFAULT)) {
                String propValue      = properties.getProperty(propName);
                String propSuffix     = propName.substring(PROP_PREFIX_DEFAULT.length());
                String pluginPropName = pluginPropPrefix + propSuffix;

                ret.setProperty(pluginPropName, propValue);
            }
        }

        // collect service-type level properties
        if (StringUtils.isNotBlank(serviceType)) {
            String svcTypePropPrefix = PROP_PREFIX_SERVICE_TYPE + serviceType + ".";

            for (String propName : properties.stringPropertyNames()) {
                if (propName.startsWith(svcTypePropPrefix)) {
                    String propValue      = properties.getProperty(propName);
                    String propSuffix     = propName.substring(svcTypePropPrefix.length());
                    String pluginPropName = pluginPropPrefix + propSuffix;

                    ret.setProperty(pluginPropName, propValue);
                }
            }
        }

        // collect service-level properties
        String svcPropPrefix = PROP_PREFIX_SERVICE + serviceName + ".";

        for (String propName : properties.stringPropertyNames()) {
            if (propName.startsWith(svcPropPrefix)) {
                String propValue      = properties.getProperty(propName);
                String propSuffix     = propName.substring(svcPropPrefix.length());
                String pluginPropName = pluginPropPrefix + propSuffix;

                ret.setProperty(pluginPropName, propValue);
            }
        }

        return ret;
    }

    public String getServiceTypeForService(String serviceName) {
        return properties.getProperty(PROP_PREFIX_SERVICE + serviceName + ".servicetype");
    }

    public String getDefaultServiceNameForServiceType(String serviceType) {
        return properties.getProperty(PROP_PREFIX_SERVICE_TYPE + serviceType + ".default.service");
    }

    /*
    private void collectPluginProperties(String prefix, Properties serviceProps) {
        for (String propName : properties.stringPropertyNames()) {
            if (propName.startsWith(prefix)) {
                String propValue = properties.getProperty(propName);

                String pluginPropName = propName.substring(prefix.length());

                serviceProps.setProperty(pluginPropName, propValue);
            }
        }
    }
     */
}
