/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.ranger.authorization.elasticsearch.authorizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpStatus;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.elasticsearch.SpecialPermission;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * Posts access audits to the Ranger audit ingestor on the Elasticsearch request thread.
 * Uses {@link HttpURLConnection} inside {@code SpecialPermission.check()} / {@code doPrivileged}
 * so ES Security Manager allows outbound sockets (Jersey/async clients do not).
 * When JAAS Kerberos settings are present in the audit configuration, posts use SPNEGO
 * without {@code AuthenticatedURL} (ES plugin policy cannot grant cookie-handler permissions).
 */
final class ElasticsearchAuditIngestorClient {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchAuditIngestorClient.class);

    private static final String REST_PATH_POST      = "/api/audit/access";
    private static final String QUERY_PARAM_SERVICE = "serviceName";
    private static final String QUERY_PARAM_APP_ID  = "appId";
    private static final String AUTHZ_HEADER        = "Authorization";
    private static final String NEGOTIATE_PREFIX    = "Negotiate ";
    private static final String WWW_AUTH_NEGOTIATE    = "Negotiate";

    private static final String JAAS_PRINCIPAL_PROP = "xasecure.audit.jaas.Client.option.principal";
    private static final String JAAS_KEYTAB_PROP    = "xasecure.audit.jaas.Client.option.keyTab";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Object       KERBEROS_INIT_LOCK = new Object();

    private static volatile boolean kerberosInitialized;

    private ElasticsearchAuditIngestorClient() {
    }

    static void init(Configuration config) {
        if (config == null || kerberosInitialized || !isKerberosConfigured(config)) {
            return;
        }

        synchronized (KERBEROS_INIT_LOCK) {
            if (kerberosInitialized || !isKerberosConfigured(config)) {
                return;
            }

            try {
                SpecialPermission.check();

                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    initKerberos(config);

                    return null;
                });

                kerberosInitialized = true;
            } catch (PrivilegedActionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;

                LOG.error("Failed to initialize Kerberos for audit ingestor client: {}", cause.getMessage(), cause);
            } catch (RuntimeException e) {
                LOG.error("Failed to initialize Kerberos for audit ingestor client: {}", e.getMessage(), e);
            }
        }
    }

    static boolean post(String auditServerBaseUrl, AuthzAuditEvent auditEvent) {
        if (StringUtils.isBlank(auditServerBaseUrl) || auditEvent == null) {
            return false;
        }

        try {
            SpecialPermission.check();

            return AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> doPost(auditServerBaseUrl, auditEvent));
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;

            LOG.error("Failed to post audit event to ingestor at {}: {}", auditServerBaseUrl, cause.getMessage(), cause);

            return false;
        } catch (RuntimeException e) {
            LOG.error("Failed to post audit event to ingestor at {}: {}", auditServerBaseUrl, e.getMessage(), e);

            return false;
        }
    }

    private static void initKerberos(Configuration config) throws IOException {
        String principal = config.get(JAAS_PRINCIPAL_PROP);
        String keytab    = config.get(JAAS_KEYTAB_PROP);

        Configuration hadoopConf = new Configuration(false);

        hadoopConf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoopConf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

        LOG.info("Kerberos initialized for audit ingestor client. Principal: {}", loginUser != null ? loginUser.getUserName() : principal);
    }

    private static boolean isKerberosConfigured(Configuration config) {
        String principal = config.get(JAAS_PRINCIPAL_PROP);
        String keytab    = config.get(JAAS_KEYTAB_PROP);

        return StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keytab) && !"%EMPTY%".equalsIgnoreCase(principal);
    }

    private static boolean doPost(String auditServerBaseUrl, AuthzAuditEvent auditEvent) throws Exception {
        if (kerberosInitialized && UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            if (loginUser != null && loginUser.hasKerberosCredentials()) {
                return loginUser.doAs((PrivilegedExceptionAction<Boolean>) () -> doPostConnection(auditServerBaseUrl, auditEvent));
            }
        }

        return doPostConnection(auditServerBaseUrl, auditEvent);
    }

    private static boolean doPostConnection(String auditServerBaseUrl, AuthzAuditEvent auditEvent) throws Exception {
        String baseUrl = auditServerBaseUrl.endsWith("/") ? auditServerBaseUrl.substring(0, auditServerBaseUrl.length() - 1) : auditServerBaseUrl;
        String service = auditEvent.getRepositoryName();
        String appId   = auditEvent.getAgentId();
        StringBuilder urlBuilder = new StringBuilder(baseUrl)
                .append(REST_PATH_POST)
                .append('?')
                .append(QUERY_PARAM_SERVICE)
                .append('=')
                .append(URLEncoder.encode(service, StandardCharsets.UTF_8.name()));

        if (StringUtils.isNotBlank(appId)) {
            urlBuilder.append('&')
                    .append(QUERY_PARAM_APP_ID)
                    .append('=')
                    .append(URLEncoder.encode(appId, StandardCharsets.UTF_8.name()));
        }

        List<AuthzAuditEvent> payload = Collections.singletonList(auditEvent);
        byte[]                body    = OBJECT_MAPPER.writeValueAsBytes(payload);
        String                url     = urlBuilder.toString();
        URL                   requestUrl = new URL(url);

        if (kerberosInitialized) {
            return doPostWithSpnego(requestUrl, body, auditEvent, service);
        }

        return postOnce(requestUrl, body, null, auditEvent, service);
    }

    private static boolean doPostWithSpnego(URL requestUrl, byte[] body, AuthzAuditEvent auditEvent, String service) throws Exception {
        GSSContext context = createSpnegoContext(requestUrl.getHost());
        byte[]     token   = context.initSecContext(new byte[0], 0, 0);
        String     negotiateToken = token == null || token.length == 0 ? null : Base64.getEncoder().encodeToString(token);

        HttpPostResult result = postOnceWithStatus(requestUrl, body, negotiateToken);

        if (result.status == HttpStatus.SC_OK) {
            logSuccess(service, auditEvent);

            return true;
        }

        if (result.status == HttpStatus.SC_UNAUTHORIZED && result.wwwAuthenticate != null) {
            byte[] challengeToken = extractNegotiateChallenge(result.wwwAuthenticate);

            if (challengeToken != null && challengeToken.length > 0) {
                token = context.initSecContext(challengeToken, 0, challengeToken.length);

                if (token != null && token.length > 0) {
                    negotiateToken = Base64.getEncoder().encodeToString(token);
                    result         = postOnceWithStatus(requestUrl, body, negotiateToken);

                    if (result.status == HttpStatus.SC_OK) {
                        logSuccess(service, auditEvent);

                        return true;
                    }
                }
            }
        }

        LOG.error("Failed to post audit event to ingestor. HTTP status: {}", result.status);

        return false;
    }

    private static boolean postOnce(URL requestUrl, byte[] body, String negotiateToken, AuthzAuditEvent auditEvent, String service) throws IOException {
        HttpPostResult result = postOnceWithStatus(requestUrl, body, negotiateToken);

        if (result.status == HttpStatus.SC_OK) {
            logSuccess(service, auditEvent);

            return true;
        }

        LOG.error("Failed to post audit event to ingestor. HTTP status: {}", result.status);

        return false;
    }

    private static HttpPostResult postOnceWithStatus(URL requestUrl, byte[] body, String negotiateToken) throws IOException {
        HttpURLConnection connection = openConnection(requestUrl, body, negotiateToken);
        int               status     = connection.getResponseCode();
        String            wwwAuth    = connection.getHeaderField("WWW-Authenticate");

        drainResponse(connection, status);

        return new HttpPostResult(status, wwwAuth);
    }

    private static HttpURLConnection openConnection(URL requestUrl, byte[] body, String negotiateToken) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();

        connection.setConnectTimeout(30_000);
        connection.setReadTimeout(30_000);
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");

        if (StringUtils.isNotBlank(negotiateToken)) {
            connection.setRequestProperty(AUTHZ_HEADER, NEGOTIATE_PREFIX + negotiateToken);
        }

        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(body);
        }

        return connection;
    }

    private static void drainResponse(HttpURLConnection connection, int status) {
        try (InputStream stream = status >= HttpStatus.SC_BAD_REQUEST ? connection.getErrorStream() : connection.getInputStream()) {
            if (stream != null) {
                while (stream.read() >= 0) {
                    // drain so the connection can be reused or closed cleanly
                }
            }
        } catch (IOException e) {
            LOG.debug("Failed to drain audit ingestor response stream: {}", e.getMessage());
        }
    }

    private static byte[] extractNegotiateChallenge(String wwwAuthenticateHeader) {
        for (String headerValue : wwwAuthenticateHeader.split(",")) {
            String trimmed = headerValue.trim();

            if (trimmed.regionMatches(true, 0, WWW_AUTH_NEGOTIATE, 0, WWW_AUTH_NEGOTIATE.length())) {
                String tokenPart = trimmed.substring(WWW_AUTH_NEGOTIATE.length()).trim();

                if (tokenPart.startsWith("=")) {
                    tokenPart = tokenPart.substring(1).trim();
                }

                if (StringUtils.isNotBlank(tokenPart)) {
                    return Base64.getDecoder().decode(tokenPart);
                }
            }
        }

        return null;
    }

    private static void logSuccess(String service, AuthzAuditEvent auditEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Audit event posted to ingestor for service={} user={}", service, auditEvent.getUser());
        }
    }

    private static GSSContext createSpnegoContext(String host) throws Exception {
        GSSManager manager = GSSManager.getInstance();
        GSSName    server  = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
        Oid        mech    = resolveSpnegoMechanism(manager);

        GSSContext context = manager.createContext(server, mech, null, GSSContext.DEFAULT_LIFETIME);

        context.requestMutualAuth(true);
        context.requestCredDeleg(false);

        return context;
    }

    private static Oid resolveSpnegoMechanism(GSSManager manager) throws Exception {
        Oid spnego = new Oid("1.3.6.1.5.5.14");

        for (Oid mech : manager.getMechs()) {
            if (spnego.equals(mech)) {
                return spnego;
            }
        }

        return new Oid("1.2.840.113554.1.2.2");
    }

    private static final class HttpPostResult {
        private final int    status;
        private final String wwwAuthenticate;

        private HttpPostResult(int status, String wwwAuthenticate) {
            this.status           = status;
            this.wwwAuthenticate  = wwwAuthenticate;
        }
    }
}
