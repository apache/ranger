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

package org.apache.ranger.audit.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Used to authenticate and execute actions when Kerberos is enabled and a keytab is being used.
 */
public class KerberosJAASConfigUser extends AbstractKerberosUser {
    private static final Logger LOG = LoggerFactory.getLogger(KerberosJAASConfigUser.class);

    private static final String JAAS_USE_KEYTAB = "useKeyTab";

    private final String        configName;
    private final Configuration config;

    public KerberosJAASConfigUser(final String configName, final Configuration config) {
        this.configName = configName;
        this.config     = config;
    }

    @Override
    public String getPrincipal() {
        String                  ret     = null;
        AppConfigurationEntry[] entries = config.getAppConfigurationEntry(configName);

        if (entries != null) {
            for (AppConfigurationEntry entry : entries) {
                if (entry.getOptions().containsKey(InMemoryJAASConfiguration.JAAS_PRINCIPAL_PROP)) {
                    ret = (String) entry.getOptions().get(InMemoryJAASConfiguration.JAAS_PRINCIPAL_PROP);

                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Solr/Kafka outbound JAAS clients (audit dispatcher, plugin Solr
     * destination) use {@code useKeyTab=true}. Opt those principals into
     * in-place keytab relogin so shipped {@code useTicketCache=true} does not
     * fail at TGT renewal with {@code "No key to store"}.
     */
    @Override
    protected boolean useKeytabRelogin() {
        return isJaasOptionTrue(JAAS_USE_KEYTAB);
    }

    private boolean isJaasOptionTrue(String optionName) {
        AppConfigurationEntry[] entries = config.getAppConfigurationEntry(configName);

        if (entries == null) {
            return false;
        }

        for (AppConfigurationEntry entry : entries) {
            Object value = entry.getOptions().get(optionName);

            if (value != null && "true".equalsIgnoreCase(value.toString())) {
                return true;
            }
        }

        return false;
    }

    @Override
    protected LoginContext createLoginContext(Subject subject) throws LoginException {
        LOG.debug("==> KerberosJAASConfigUser.createLoginContext()");

        LOG.debug("<== KerberosJAASConfigUser.createLoginContext(), Subject: {}", subject);

        return new LoginContext(configName, subject, null, config);
    }
}
