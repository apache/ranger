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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RangerAuditServerDestination extends AuditDestination {
    public static final String PROP_AUDITSERVER_URL                          = "xasecure.audit.destination.auditserver.url";
    public static final String PROP_AUDITSERVER_USER_NAME                    = "xasecure.audit.destination.auditserver.username";
    public static final String PROP_AUDITSERVER_USER_PASSWORD                = "xasecure.audit.destination.auditserver.password";
    public static final String PROP_AUDITSERVER_AUTH_TYPE                    = "xasecure.audit.destination.auditserver.authentication.type";
    public static final String PROP_AUDITSERVER_JWT_TOKEN                    = "xasecure.audit.destination.auditserver.jwt.token";
    public static final String PROP_AUDITSERVER_JWT_TOKEN_FILE               = "xasecure.audit.destination.auditserver.jwt.token.file";
    public static final String PROP_AUDITSERVER_CLIENT_CONN_TIMEOUT_MS       = "xasecure.audit.destination.auditserver.connection.timeout.ms";
    public static final String PROP_AUDITSERVER_CLIENT_READ_TIMEOUT_MS       = "xasecure.audit.destination.auditserver.read.timeout.ms";
    public static final String PROP_AUDITSERVER_SSL_CONFIG_FILE              = "xasecure.audit.destination.auditserver.ssl.config.file";
    public static final String PROP_AUDITSERVER_MAX_RETRY_ATTEMPTS           = "xasecure.audit.destination.auditserver.max.retry.attempts";
    public static final String PROP_AUDITSERVER_RETRY_INTERVAL_MS            = "xasecure.audit.destination.auditserver.retry.interval.ms";
    public static final String PROP_SERVICE_TYPE                             = "ranger.plugin.audit.service.type";
    public static final String PROP_APP_ID                                   = "ranger.plugin.audit.app.id";
    public static final String REST_RELATIVE_PATH_POST                       = "/api/audit/access";
    public static final String QUERY_PARAM_SERVICE_NAME                      = "serviceName";
    public static final String QUERY_PARAM_APP_ID                            = "appId";

    // Authentication types
    public static final String AUTH_TYPE_KERBEROS                            = "kerberos";
    public static final String AUTH_TYPE_BASIC                               = "basic";
    public static final String AUTH_TYPE_JWT                                 = "jwt";

    private static final Logger           LOG        = LoggerFactory.getLogger(RangerAuditServerDestination.class);
    private              RangerRESTClient restClient;
    private              String           authType;
    private              String           serviceType;
    private              String           appId;

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("==> RangerAuditServerDestination:init()");
        super.init(props, propPrefix);

        String url               = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_URL);
        String sslConfigFileName = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_SSL_CONFIG_FILE);
        String userName          = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_USER_NAME);
        String password          = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_USER_PASSWORD);
        int    connTimeoutMs     = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_CLIENT_CONN_TIMEOUT_MS, 120000);
        int    readTimeoutMs     = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_CLIENT_READ_TIMEOUT_MS, 30000);
        int    maxRetryAttempts  = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_MAX_RETRY_ATTEMPTS, 3);
        int    retryIntervalMs   = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_RETRY_INTERVAL_MS, 1000);

        this.authType    = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_AUTH_TYPE);
        this.serviceType = MiscUtil.getStringProperty(props, PROP_SERVICE_TYPE);
        this.appId       = MiscUtil.getStringProperty(props, PROP_APP_ID);

        if (StringUtils.isEmpty(authType)) {
            // Authentication priority: JWT → Kerberos → Basic
            try {
                if (StringUtils.isNotEmpty(MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN)) ||
                        StringUtils.isNotEmpty(MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN_FILE))) {
                    this.authType = AUTH_TYPE_JWT;
                } else if (isKerberosAuthenticated()) {
                    this.authType = AUTH_TYPE_KERBEROS;
                } else if (StringUtils.isNotEmpty(userName)) {
                    this.authType = AUTH_TYPE_BASIC;
                }
            } catch (Exception e) {
                LOG.warn("Failed to auto-detect authentication type", e);
            }
        }

        LOG.info("Audit destination authentication type: {}", authType);

        if (StringUtils.isEmpty(this.serviceType)) {
            LOG.error("Service type not available in audit properties. This is a configuration error. Audit destination will not function correctly.");
            LOG.error("Ensure that RangerBasePlugin is properly initialized with a valid serviceType (hdfs, hive, kafka, etc.)");
        } else {
            LOG.info("Audit destination configured for service type: {}", this.serviceType);
        }

        if (StringUtils.isNotEmpty(this.appId)) {
            LOG.info("Audit destination configured with appId: {}", this.appId);
        } else {
            LOG.warn("appId not available in audit properties. Batch processing may not be optimal.");
        }

        if (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            preAuthenticateKerberos();
        }

        Configuration config = createConfigurationFromProperties(props, authType, userName, password);
        this.restClient = new RangerRESTClient(url, sslConfigFileName, config);
        this.restClient.setRestClientConnTimeOutMs(connTimeoutMs);
        this.restClient.setRestClientReadTimeOutMs(readTimeoutMs);
        this.restClient.setMaxRetryAttempts(maxRetryAttempts);
        this.restClient.setRetryIntervalMs(retryIntervalMs);

        LOG.info("<== RangerAuditServerDestination:init()");
    }

    private String initJwtToken() {
        LOG.info("==> RangerAuditServerDestination:initJwtToken()");

        String jwtToken = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN);
        if (StringUtils.isEmpty(jwtToken)) {
            String jwtTokenFile = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN_FILE);
            if (StringUtils.isNotEmpty(jwtTokenFile)) {
                try {
                    jwtToken = readJwtTokenFromFile(jwtTokenFile);
                    LOG.info("JWT token loaded from file: {}", jwtTokenFile);
                } catch (Exception e) {
                    LOG.error("Failed to read JWT token from file: {}", jwtTokenFile, e);
                }
            }
        }

        if (StringUtils.isEmpty(jwtToken)) {
            LOG.warn("JWT authentication configured but no token found. Configure {} or {}", PROP_AUDITSERVER_JWT_TOKEN, PROP_AUDITSERVER_JWT_TOKEN_FILE);
        } else {
            LOG.info("JWT authentication initialized successfully");
        }

        LOG.info("<== RangerAuditServerDestination:initJwtToken()");

        return jwtToken;
    }

    private String readJwtTokenFromFile(String tokenFile) throws IOException {
        try (InputStream in = getFileInputStream(tokenFile)) {
            if (in != null) {
                return IOUtils.toString(in, Charset.defaultCharset()).trim();
            } else {
                throw new IOException("Unable to read JWT token file: " + tokenFile);
            }
        }
    }

    private void preAuthenticateKerberos() {
        LOG.info("==> RangerAuditServerDestination:preAuthenticateKerberos()");

        try {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();

            if (ugi == null) {
                LOG.warn("No UserGroupInformation available for Kerberos pre-authentication");
                return;
            }

            if (!ugi.hasKerberosCredentials()) {
                LOG.warn("User {} does not have Kerberos credentials for pre-authentication", ugi.getUserName());
                return;
            }

            LOG.info("Pre-authenticating Kerberos for user: {}, authMethod: {}", ugi.getUserName(), ugi.getAuthenticationMethod());

            ugi.checkTGTAndReloginFromKeytab();
            LOG.debug("TGT verified and refreshed if needed for user: {}", ugi.getUserName());

            LOG.info("Kerberos pre-authentication completed successfully");
        } catch (Exception e) {
            LOG.warn("Kerberos pre-authentication failed. First request will retry authentication", e);
        }

        LOG.info("<== RangerAuditServerDestination:preAuthenticateKerberos()");
    }

    @Override
    public void stop() {
        LOG.info("==> RangerAuditServerDestination.stop() called..");
        logStatus();
        if (restClient != null) {
            restClient.resetClient();
            restClient = null;
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

            if (restClient == null) {
                LOG.error("REST client is not initialized. Cannot send audit events");
                addDeferredCount(events.size());
                return ret;
            }

            ret = logAsBatch(events);
        } catch (Throwable t) {
            addDeferredCount(events.size());
            logError("Error sending audit to Audit Server", t);
        }
        return ret;
    }

    public boolean isAsync() {
        return true;
    }

    private boolean logAsBatch(Collection<AuditEventBase> events) {
        boolean  batchSuccess = false;
        int      totalEvents  = events.size();

        LOG.debug("==> logAsBatch() Sending batch of {} events to Audit Server....", totalEvents);

        batchSuccess = sendBatch(events);
        if (batchSuccess) {
            addSuccessCount(totalEvents);
        } else {
            LOG.error("Failed to send batch of {} events", totalEvents);
            addFailedCount(totalEvents);
        }

        LOG.debug("<== logAsBatch() Batch processing complete: {}/{} events sent successfully", batchSuccess ? totalEvents : 0, totalEvents);

        return batchSuccess;
    }

    private boolean sendBatch(Collection<AuditEventBase> events) {
        boolean ret = false;
        Map<String, String> queryParams = new HashMap<>();

        // Add serviceName to query parameters
        if (StringUtils.isNotEmpty(serviceType)) {
            queryParams.put(QUERY_PARAM_SERVICE_NAME, serviceType);
            LOG.debug("Adding serviceName={} to audit request", serviceType);
        } else {
            LOG.error("Cannot send audit batch: serviceType is not set. This indicates a configuration error.");
            LOG.error("Audit server requires serviceName parameter. Please ensure RangerBasePlugin is properly initialized.");
            return false;
        }

        // Add appId to query parameters for batch processing
        if (StringUtils.isNotEmpty(appId)) {
            queryParams.put(QUERY_PARAM_APP_ID, appId);
            LOG.debug("Adding appId={} to audit request for batch processing", appId);
        }

        try {
            final UserGroupInformation user         = MiscUtil.getUGILoginUser();
            final boolean              isSecureMode = isKerberosAuthenticated();

            if (isSecureMode && user != null) {
                LOG.debug("Sending audit batch of {} events using Kerberos. Principal: {}, AuthMethod: {}", events.size(), user.getUserName(), user.getAuthenticationMethod());
            } else {
                LOG.debug("Sending audit batch of {} events. SecureMode: {}, User: {}", events.size(), isSecureMode, user != null ? user.getUserName() : "null");
            }

            final ClientResponse response;

            if (isSecureMode) {
                response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                    try {
                        return postAuditEvents(REST_RELATIVE_PATH_POST, queryParams, events);
                    } catch (Exception e) {
                        LOG.error("Failed to post audit events in privileged action: {}", e.getMessage());
                        throw e;
                    }
                });
            } else {
                response = postAuditEvents(REST_RELATIVE_PATH_POST, queryParams, events);
            }

            if (response != null) {
                int status = response.getStatus();

                if (status == HttpServletResponse.SC_OK) {
                    String responseStr = response.getEntity(String.class);
                    LOG.debug("Audit batch sent successfully. {} events delivered. Response: {}", events.size(), responseStr);
                    ret = true;
                } else {
                    String errorBody = "";
                    try {
                        if (response.hasEntity()) {
                            errorBody = response.getEntity(String.class);
                        }
                    } catch (Exception e) {
                        LOG.debug("Failed to read error response body", e);
                    }

                    LOG.error("Failed to send audit batch. HTTP status: {}, Response: {}", status, errorBody);

                    if (status == HttpServletResponse.SC_UNAUTHORIZED) {
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
        }
        return ret;
    }

    private ClientResponse postAuditEvents(String relativeUrl, Map<String, String> params, Collection<AuditEventBase> events) throws Exception {
        LOG.debug("Posting {} audit events to {}", events.size(), relativeUrl);

        WebResource webResource = restClient.getResource(relativeUrl);
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                webResource = webResource.queryParam(entry.getKey(), entry.getValue());
            }
        }

        return webResource
                .accept("application/json")
                .type("application/json")
                .entity(events)
                .post(ClientResponse.class);
    }

    private Configuration createConfigurationFromProperties(Properties props, String authType, String userName, String password) {
        Configuration config = new Configuration();

        for (String key : props.stringPropertyNames()) {
            config.set(key, props.getProperty(key));
        }

        final String restClientPrefix = "ranger.plugin";
        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
            String jwtToken = initJwtToken();
            if (StringUtils.isNotEmpty(jwtToken)) {
                String jwtTokenFile = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN_FILE);
                if (StringUtils.isNotEmpty(jwtTokenFile)) {
                    config.set(restClientPrefix + ".policy.rest.client.jwt.source", "file");
                    config.set(restClientPrefix + ".policy.rest.client.jwt.file", jwtTokenFile);
                    LOG.info("JWT authentication configured via file: {}", jwtTokenFile);
                } else {
                    LOG.warn("JWT token is set but file path is not available. JWT may not work as expected");
                }
            }
        } else if (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            // For Kerberos, no additional configuration is needed in RangerRESTClient
            // Authentication happens via Subject.doAs() security context
            LOG.info("Kerberos authentication will be used via privileged action (Subject.doAs)");
        } else if (AUTH_TYPE_BASIC.equalsIgnoreCase(authType) && StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)) {
            config.set(restClientPrefix + ".policy.rest.client.username", userName);
            config.set(restClientPrefix + ".policy.rest.client.password", password);
            LOG.info("Basic authentication configured for user: {}", userName);
        }

        return config;
    }

    private boolean isKerberosAuthenticated() {
        try {
            UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();

            if (loggedInUser == null) {
                return false;
            }

            boolean isSecurityEnabled      = UserGroupInformation.isSecurityEnabled();
            boolean hasKerberosCredentials = loggedInUser.hasKerberosCredentials();
            UserGroupInformation.AuthenticationMethod authMethod = loggedInUser.getAuthenticationMethod();

            return isSecurityEnabled && hasKerberosCredentials && authMethod.equals(UserGroupInformation.AuthenticationMethod.KERBEROS);
        } catch (Exception e) {
            LOG.warn("Failed to check Kerberos authentication status", e);
            return false;
        }
    }

    private InputStream getFileInputStream(String fileName) throws IOException {
        if (StringUtils.isEmpty(fileName)) {
            return null;
        }

        java.io.File file = new java.io.File(fileName);

        if (file.exists()) {
            return new java.io.FileInputStream(file);
        } else {
            return ClassLoader.getSystemResourceAsStream(fileName);
        }
    }
}
