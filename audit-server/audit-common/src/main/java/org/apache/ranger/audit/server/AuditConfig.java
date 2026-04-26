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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * Base configuration class for Ranger Audit Server services.
 * Can be extended by specific services to load their custom configuration files.
 */
public class AuditConfig extends Configuration {
    private static final Logger LOG = LoggerFactory.getLogger(AuditConfig.class);

    public AuditConfig() {
        super();
    }

    public Properties getProperties() {
        return this.getProps();
    }

    /**
     * Add a resource file to the configuration.
     * Subclasses can override to load their specific config files.
     *
     * @param resourcePath Path to the resource file (e.g., "conf/ranger-audit-ingestor-site.xml")
     * @param required Whether this resource is required
     * @return true if resource was loaded successfully or is optional, false otherwise
     */
    public boolean addAuditResource(String resourcePath, boolean required) {
        LOG.debug("==> addAuditResource(path={}, required={})", resourcePath, required);

        boolean success = addResourceIfReadable(resourcePath);

        if (success) {
            LOG.info("Successfully loaded configuration: {}", resourcePath);
        } else if (required) {
            LOG.error("Failed to load required configuration: {}", resourcePath);
        } else {
            LOG.warn("Failed to load optional configuration: {}", resourcePath);
        }

        LOG.debug("<== addAuditResource(path={}, required={}), result={}", resourcePath, required, success);

        return success || !required;
    }

    public boolean addResourceIfReadable(String aResourceName) {
        LOG.debug("==> addResourceIfReadable({})", aResourceName);

        boolean ret  = false;
        URL fUrl = getFileLocation(aResourceName);

        if (fUrl != null) {
            LOG.debug("addResourceIfReadable({}): resource file is {}", aResourceName, fUrl);

            try {
                addResource(fUrl);

                ret = true;
            } catch (Exception e) {
                LOG.error("Unable to load the resource name [{}]. Ignoring the resource:{}", aResourceName, fUrl);

                LOG.debug("Resource loading failed for {}", fUrl, e);
            }
        } else {
            LOG.debug("addResourceIfReadable({}): couldn't find resource file location", aResourceName);
        }

        LOG.debug("<== addResourceIfReadable({}), result={}", aResourceName, ret);

        return ret;
    }

    private URL getFileLocation(String fileName) {
        URL lurl = null;

        if (!StringUtils.isEmpty(fileName)) {
            lurl = AuditConfig.class.getClassLoader().getResource(fileName);

            if (lurl == null) {
                lurl = AuditConfig.class.getClassLoader().getResource("/" + fileName);
            }

            if (lurl == null) {
                File f = new File(fileName);
                if (f.exists()) {
                    try {
                        lurl = f.toURI().toURL();
                    } catch (MalformedURLException e) {
                        LOG.error("Unable to load the resource name [{}]. Ignoring the resource:{}", fileName, f.getPath());
                    }
                } else {
                    LOG.debug("Conf file path {} does not exists", fileName);
                }
            }
        }

        return lurl;
    }
}
