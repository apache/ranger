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

package org.apache.ranger.audit.destination;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpStatus;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.authn.DefaultJwtProvider;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class RangerAuditServerDestination extends AuditDestination {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuditServerDestination.class);

    public static final String PROP_URL                    = "url";
    public static final String PROP_SSL_CONFIG_FILE        = "ssl.config.file";
    public static final String PROP_AUTHN_TYPE             = "authn.type";
    public static final String PROP_AUTHN_BASIC_USERNAME   = "authn.basic.username";
    public static final String PROP_AUTHN_BASIC_PASSWORD   = "authn.basic.password";
    public static final String PROP_AUTHN_JWT_ENV          = "authn.jwt.env";
    public static final String PROP_AUTHN_JWT_FILE         = "authn.jwt.file";
    public static final String PROP_CLIENT_CONN_TIMEOUT_MS = "connection.timeout.ms";
    public static final String PROP_CLIENT_READ_TIMEOUT_MS = "read.timeout.ms";
    public static final String PROP_MAX_RETRY_ATTEMPTS     = "max.retry.attempts";
    public static final String PROP_RETRY_INTERVAL_MS      = "retry.interval.ms";

    public static final String REST_RELATIVE_PATH_POST  = "/api/audit/access";
    public static final String QUERY_PARAM_SERVICE_NAME = "serviceName";
    public static final String QUERY_PARAM_APP_ID       = "appId";

    // Authentication types
    public static final String AUTH_TYPE_KERBEROS = "kerberos";
    public static final String AUTH_TYPE_BASIC    = "basic";
    public static final String AUTH_TYPE_JWT      = "jwt";

    private RangerRESTClient restClient;

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("==> RangerAuditServerDestination:init()");

        super.init(props, propPrefix);

        String url               = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_URL);
        String sslConfigFileName = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SSL_CONFIG_FILE);
        int    connTimeoutMs     = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_CLIENT_CONN_TIMEOUT_MS, 120000);
        int    readTimeoutMs     = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_CLIENT_READ_TIMEOUT_MS, 30000);
        int    maxRetryAttempts  = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_MAX_RETRY_ATTEMPTS, 3);
        int    retryIntervalMs   = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_RETRY_INTERVAL_MS, 1000);
        String authType          = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUTHN_TYPE);

        LOG.info("Audit destination authentication type: {}", authType);

        Configuration config = createRESTClientConfiguration(props, propPrefix, authType);

        this.restClient = new RangerRESTClient(url, sslConfigFileName, config);

        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
            this.restClient.setJwtProvider(new DefaultJwtProvider("ranger.plugin.policy.rest.client", config));
        }

        this.restClient.setRestClientConnTimeOutMs(connTimeoutMs);
        this.restClient.setRestClientReadTimeOutMs(readTimeoutMs);
        this.restClient.setMaxRetryAttempts(maxRetryAttempts);
        this.restClient.setRetryIntervalMs(retryIntervalMs);

        LOG.info("<== RangerAuditServerDestination:init()");
    }

    @Override
    public void stop() {
        LOG.info("==> RangerAuditServerDestination.stop() called..");

        logStatus();

        RangerRESTClient restClient = this.restClient;

        if (restClient != null) {
            restClient.resetClient();

            this.restClient = null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.ranger.audit.provider.AuditProvider#flush()
     */
    @Override
    public void flush() {
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        boolean ret = false;

        try {
            logStatusIfRequired();

            addTotalCount(events.size());

            RangerRESTClient restClient = this.restClient;

            if (restClient != null) {
                ret = logAsBatch(events, restClient);
            } else {
                LOG.error("REST client is not initialized. Cannot send audit events");

                addDeferredCount(events.size());
            }
        } catch (Throwable t) {
            logError("Error sending audit to Audit Server", t);

            addDeferredCount(events.size());
        }

        return ret;
    }

    private boolean logAsBatch(Collection<AuditEventBase> events, RangerRESTClient restClient) {
        int totalEvents = events.size();

        LOG.debug("==> logAsBatch(): sending batch of {} events to Audit Server", totalEvents);

        boolean batchSuccess = sendBatch(events, restClient);

        if (batchSuccess) {
            addSuccessCount(totalEvents);
        } else {
            LOG.error("Failed to send batch of {} events", totalEvents);

            addFailedCount(totalEvents);
        }

        LOG.debug("<== logAsBatch(): batch processing complete: {}/{} events sent successfully", batchSuccess ? totalEvents : 0, totalEvents);

        return batchSuccess;
    }

    private boolean sendBatch(Collection<AuditEventBase> events, RangerRESTClient restClient) {
        boolean             ret;
        Map<String, String> queryParams = new HashMap<>();
        String              serviceName = fetchServiceName(events);
        String              appId       = fetchAppId(events);

        LOG.debug("Adding serviceName={} to audit request", serviceName);

        queryParams.put(QUERY_PARAM_SERVICE_NAME, serviceName);

        if (StringUtils.isNotEmpty(appId)) {
            LOG.debug("Adding appId={} to audit request for batch processing", appId);

            queryParams.put(QUERY_PARAM_APP_ID, appId);
        }

        Response response = null;

        try {
            final UserGroupInformation user         = MiscUtil.getUGILoginUser();
            final boolean              isSecureMode = isKerberosAuthenticated();

            if (isSecureMode && user != null) {
                LOG.debug("Sending audit batch of {} events using Kerberos. Principal: {}, AuthMethod: {}", events.size(), user.getUserName(), user.getAuthenticationMethod());
            } else {
                LOG.debug("Sending audit batch of {} events. SecureMode: {}, User: {}", events.size(), isSecureMode, user != null ? user.getUserName() : "null");
            }

            if (isSecureMode) {
                response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<Response>) () -> {
                    try {
                        return postAuditEvents(restClient, queryParams, events);
                    } catch (Exception e) {
                        LOG.error("Failed to post audit events in privileged action: {}", e.getMessage());
                        throw e;
                    }
                });
            } else {
                response = postAuditEvents(restClient, queryParams, events);
            }

            if (response != null) {
                int status = response.getStatus();

                if (status == HttpStatus.SC_OK) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Audit batch sent successfully. {} events delivered. Response: {}", events.size(), response.readEntity(String.class));
                    }

                    ret = true;
                } else {
                    String errorBody = "";

                    try {
                        if (response.hasEntity()) {
                            errorBody = response.readEntity(String.class);
                        }
                    } catch (Exception e) {
                        LOG.debug("Failed to read error response body", e);
                    }

                    LOG.error("Failed to send audit batch. HTTP status: {}, Response: {}", status, errorBody);

                    if (status == HttpStatus.SC_UNAUTHORIZED) {
                        LOG.error("Authentication failure (401). Verify credentials are valid and audit server is properly configured.");
                    }

                    ret = false;
                }
            } else {
                LOG.error("Received null response from audit server for batch of {} events", events.size());

                ret = false;
            }
        } catch (Exception e) {
            LOG.error("Failed to send audit batch of {} events. Error: {}", events.size(), e.getMessage(), e);

            ret = false;
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (Exception e) {
                    LOG.debug("Error closing HTTP response", e);
                }
            }
        }

        return ret;
    }

    private Response postAuditEvents(RangerRESTClient restClient, Map<String, String> params, Collection<AuditEventBase> events) {
        LOG.debug("Posting {} audit events to {}", events.size(), REST_RELATIVE_PATH_POST);

        WebTarget target = restClient.getResource(REST_RELATIVE_PATH_POST);

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                target = target.queryParam(entry.getKey(), entry.getValue());
            }
        }

        return target.request(MediaType.APPLICATION_JSON_TYPE)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.entity(events, MediaType.APPLICATION_JSON_TYPE));
    }

    private static Configuration createRESTClientConfiguration(Properties props, String propPrefix, String authType) {
        Configuration config = new Configuration();

        for (String key : props.stringPropertyNames()) {
            config.set(key, props.getProperty(key));
        }

        final String restClientPrefix = "ranger.plugin";

        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
            String jwtEnv  = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUTHN_JWT_ENV);
            String jwtFile = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUTHN_JWT_FILE);

            if (StringUtils.isNotBlank(jwtEnv)) {
                config.set(restClientPrefix + ".policy.rest.client.jwt.source", "env");
                config.set(restClientPrefix + ".policy.rest.client.jwt.env", jwtEnv);
            } else if (StringUtils.isNotBlank(jwtFile)) {
                config.set(restClientPrefix + ".policy.rest.client.jwt.source", "file");
                config.set(restClientPrefix + ".policy.rest.client.jwt.file", jwtFile);
            } else {
                LOG.warn("JWT authentication configured but no token found. Configure {} or {}", PROP_AUTHN_JWT_ENV, PROP_AUTHN_JWT_FILE);
            }
        } else if (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            initKerberos();

            // For Kerberos, no additional configuration is needed in RangerRESTClient
            // Authentication happens via Subject.doAs() security context
            LOG.info("Kerberos authentication will be used via privileged action (Subject.doAs)");
        } else if (AUTH_TYPE_BASIC.equalsIgnoreCase(authType)) {
            String userName          = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUTHN_BASIC_USERNAME);
            String password          = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUTHN_BASIC_PASSWORD);

            if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
                config.set(restClientPrefix + ".policy.rest.client.username", userName);
                config.set(restClientPrefix + ".policy.rest.client.password", password);

                LOG.info("Basic authentication configured for user: {}", userName);
            } else {
                LOG.warn("Basic authentication configured but username/password not found. Configure {} and {}", PROP_AUTHN_BASIC_USERNAME, PROP_AUTHN_BASIC_PASSWORD);
            }
        }

        return config;
    }

    private static void initKerberos() {
        LOG.info("==> RangerAuditServerDestination.initKerberos()");

        try {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();

            if (ugi == null) {
                LOG.warn("No UserGroupInformation available for Kerberos authentication");
            } else if (!ugi.hasKerberosCredentials()) {
                LOG.warn("User {} does not have Kerberos credentials for authentication", ugi.getUserName());
            } else {
                LOG.info("Kerberos authentication for user: {}, authMethod: {}", ugi.getUserName(), ugi.getAuthenticationMethod());

                ugi.checkTGTAndReloginFromKeytab();

                LOG.debug("TGT verified and refreshed if needed for user: {}", ugi.getUserName());

                LOG.info("Kerberos authentication completed successfully");
            }
        } catch (Exception e) {
            LOG.warn("Kerberos authentication failed. First request will retry authentication", e);
        }

        LOG.info("<== RangerAuditServerDestination.initKerberos()");
    }

    private static boolean isKerberosAuthenticated() {
        try {
            boolean isSecurityEnabled = UserGroupInformation.isSecurityEnabled();

            if (isSecurityEnabled) {
                UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();

                return loggedInUser != null &&
                        loggedInUser.hasKerberosCredentials() &&
                        UserGroupInformation.AuthenticationMethod.KERBEROS.equals(loggedInUser.getAuthenticationMethod());
            }
        } catch (Exception e) {
            LOG.warn("Failed to check Kerberos authentication status", e);
        }

        return false;
    }

    private static String fetchServiceName(Collection<AuditEventBase> events) {
        Iterator<AuditEventBase> iter       = events.iterator();
        AuditEventBase           auditEvent = iter.hasNext() ? iter.next() : null;

        return (auditEvent instanceof AuthzAuditEvent) ? ((AuthzAuditEvent) auditEvent).getRepositoryName() : null;
    }

    private static String fetchAppId(Collection<AuditEventBase> events) {
        Iterator<AuditEventBase> iter       = events.iterator();
        AuditEventBase           auditEvent = iter.hasNext() ? iter.next() : null;

        return (auditEvent instanceof AuthzAuditEvent) ? ((AuthzAuditEvent) auditEvent).getAgentId() : null;
    }
}
