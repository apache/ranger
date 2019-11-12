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

package org.apache.ranger.authorization.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URL;

public class RangerPluginConfig extends RangerConfiguration {
    private static final Logger LOG = Logger.getLogger(RangerPluginConfig.class);

    public RangerPluginConfig(String serviceType) {
        super();

        addResourcesForServiceType(serviceType);
    }


    private void addResourcesForServiceType(String serviceType) {
        String auditCfg    = "ranger-" + serviceType + "-audit.xml";
        String securityCfg = "ranger-" + serviceType + "-security.xml";
        String sslCfg 	   = "ranger-" + serviceType + "-policymgr-ssl.xml";

        if (!addResourceIfReadable(auditCfg)) {
            addAuditResource(serviceType);
        }

        if (!addResourceIfReadable(securityCfg)) {
            addSecurityResource(serviceType);
        }

        if (!addResourceIfReadable(sslCfg)) {
            addSslConfigResource(serviceType);
        }
    }

    private void  addSecurityResource(String serviceType) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> addSecurityResource(Service Type: " + serviceType );
        }

        Configuration rangerConf = RangerLegacyConfigBuilder.getSecurityConfig(serviceType);

        if (rangerConf != null ) {
            addResource(rangerConf);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unable to add the Security Config for " + serviceType + ". Plugin won't be enabled!");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<= addSecurityResource(Service Type: " + serviceType );
        }
    }

    private void  addAuditResource(String serviceType) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addAuditResource(Service Type: " + serviceType );
        }

        try {
            URL url = RangerLegacyConfigBuilder.getAuditConfig(serviceType);

            if (url != null) {
                addResource(url);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("==> addAuditResource() URL" + url.getPath());
                }
            }

        } catch (Throwable t) {
            LOG.warn("Unable to find Audit Config for "  + serviceType + " Auditing not enabled !" );

            if(LOG.isDebugEnabled()) {
                LOG.debug("Unable to find Audit Config for "  + serviceType + " Auditing not enabled !" + t);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addAuditResource(Service Type: " + serviceType + ")");
        }
    }

    private void addSslConfigResource(String serviceType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addSslConfigResource(Service Type: " + serviceType);
        }

        try {
            String sslConfigFile = this.get(RangerLegacyConfigBuilder.getPropertyName(RangerConfigConstants.RANGER_PLUGIN_REST_SSL_CONFIG_FILE, serviceType));

            URL url = getSSLConfigResource(sslConfigFile);
            if (url != null) {
                addResource(url);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSL config file URL:" + url.getPath());
                }
            }
        } catch (Throwable t) {
            LOG.warn(" Unable to find SSL Configs");

            if (LOG.isDebugEnabled()) {
                LOG.debug(" Unable to find SSL Configs");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addSslConfigResource(Service Type: " + serviceType + ")");
        }
    }

    private URL getSSLConfigResource(String fileName) throws Throwable {
        URL ret = null;

        try {
            if (fileName != null) {
                File f = new File(fileName);
                if (f.exists() && f.canRead()) {
                    ret = f.toURI().toURL();
                }
            }
        } catch (Throwable t) {
            LOG.error("Unable to read SSL configuration file:" + fileName);

            throw t;
        }

        return ret;
    }
}
