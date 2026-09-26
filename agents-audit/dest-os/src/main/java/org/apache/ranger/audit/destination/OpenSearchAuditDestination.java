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
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.credutils.CredentialsProviderUtil;
import org.apache.ranger.authorization.credutils.kerberos.KerberosCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

public class OpenSearchAuditDestination extends AuditDestination {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAuditDestination.class);

    public static final String CONFIG_PREFIX              = "ranger.audit.opensearch";
    public static final String CONFIG_URLS                = "urls";
    public static final String CONFIG_PORT                = "port";
    public static final String CONFIG_USER                = "user";
    public static final String CONFIG_PASSWORD            = "password";
    public static final String CONFIG_PROTOCOL            = "protocol";
    public static final String CONFIG_INDEX               = "index";
    public static final String CONFIG_AUTH_TYPE           = "authentication.type";
    public static final String CONFIG_KERBEROS_PRINCIPAL  = "kerberos.principal";
    public static final String CONFIG_KERBEROS_KEYTAB     = "kerberos.keytab";
    public static final String DEFAULT_INDEX              = "ranger_audits";

    public static final String AUTH_TYPE_KERBEROS = "kerberos";
    public static final String AUTH_TYPE_BASIC    = "basic";
    public static final String AUTH_TYPE_NONE     = "none";

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        return sdf;
    });

    private volatile OpenSearchClient    client;
    private volatile OpenSearchTransport transport;

    private String index;
    private String user;
    private String password;
    private String protocol;
    private String urls;
    private int    port;
    private String authType;
    private String kerberosPrincipal;
    private String kerberosKeytab;

    public OpenSearchAuditDestination() {
        propPrefix = CONFIG_PREFIX;
    }

    @Override
    public void init(Properties props, String propPrefix) {
        super.init(props, propPrefix);

        this.urls     = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_URLS, "localhost");
        this.port     = MiscUtil.getIntProperty(props, propPrefix + "." + CONFIG_PORT, 9200);
        this.protocol = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_PROTOCOL, "http");
        this.user     = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_USER, "");
        this.password = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_PASSWORD, "");
        this.index    = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_INDEX, DEFAULT_INDEX);

        this.authType          = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_AUTH_TYPE, "");
        this.kerberosPrincipal = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_KERBEROS_PRINCIPAL, "");
        this.kerberosKeytab    = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_KERBEROS_KEYTAB, "");

        LOG.info("OpenSearchAuditDestination.init(): urls={}, port={}, index={}", urls, port, index);

        getClient();
    }

    @Override
    public void stop() {
        logStatus();

        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                LOG.error("Error closing OpenSearch client", e);
            }
        }
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        if (events == null || events.isEmpty()) {
            return true;
        }

        boolean           ret           = false;
        OpenSearchClient  currentClient = getClient();

        if (currentClient == null) {
            LOG.error("OpenSearch client is null. Cannot write audit events.");
        } else {
            try {
                List<BulkOperation> operations = new ArrayList<>();

                for (AuditEventBase event : events) {
                    AuthzAuditEvent     auditEvent = (AuthzAuditEvent) event;
                    Map<String, Object> doc        = toDoc(auditEvent);
                    String              id         = (String) doc.get("id");

                    if (StringUtils.isBlank(id)) {
                        id = UUID.randomUUID().toString();

                        doc.put("id", id);
                    }

                    final String              documentId = id;
                    final Map<String, Object> document   = doc;

                    operations.add(BulkOperation.of(b -> b.index(IndexOperation.of(io -> io.index(index).id(documentId).document(document)))));
                }

                BulkRequest  bulkRequest  = BulkRequest.of(b -> b.operations(operations));
                BulkResponse bulkResponse = currentClient.bulk(bulkRequest);

                if (bulkResponse.errors()) {
                    LOG.error("OpenSearch bulk response contains item-level errors");
                } else {
                    ret = true;
                }
            } catch (Exception e) {
                LOG.error("Failed to write audit events to OpenSearch", e);
            }
        }

        if (ret) {
            addSuccessCount(events.size());
        } else {
            addFailedCount(events.size());
        }

        return ret;
    }

    public boolean isAsync() {
        return true;
    }

    synchronized OpenSearchClient getClient() {
        if (client == null) {
            if (StringUtils.isBlank(urls) || "NONE".equalsIgnoreCase(urls)) {
                LOG.error("OpenSearch URLs not configured");

                return null;
            }

            String resolvedAuthType = resolveAuthType(authType, user, password);

            if (AUTH_TYPE_KERBEROS.equals(resolvedAuthType)) {
                String principal = isConfigured(kerberosPrincipal) ? kerberosPrincipal : user;
                String keytab    = isConfigured(kerberosKeytab) ? kerberosKeytab : password;

                transport = createKerberosTransport(principal, keytab);
                client    = new OpenSearchClient(transport);

                LOG.info("OpenSearch client configured with Kerberos authentication for principal: {}", principal);
            } else {
                transport = createHttpClient5Transport(resolvedAuthType);
                client    = new OpenSearchClient(transport);

                if (AUTH_TYPE_BASIC.equals(resolvedAuthType)) {
                    LOG.info("OpenSearch client configured with basic authentication for user: {}", user);
                } else {
                    LOG.info("OpenSearch client configured without authentication");
                }
            }
        }

        return client;
    }

    public static boolean isConfigured(final String value) {
        return StringUtils.isNotBlank(value) && !"NONE".equalsIgnoreCase(value.trim());
    }

    /**
     * Resolves the authentication scheme. When {@code authentication.type} is set explicitly
     * ({@value AUTH_TYPE_KERBEROS}/{@value AUTH_TYPE_BASIC}/{@value AUTH_TYPE_NONE}) it is honored as-is.
     * When unset, the type is inferred for backward compatibility: a password pointing to an existing
     * keytab file selects Kerberos, a user+password pair selects basic auth, otherwise no authentication.
     */
    public static String resolveAuthType(final String authType, final String user, final String password) {
        if (StringUtils.isNotBlank(authType)) {
            return authType.trim().toLowerCase(Locale.ROOT);
        }

        if (isConfigured(user) && isConfigured(password)) {
            if (password.contains("keytab") && new File(password).exists()) {
                return AUTH_TYPE_KERBEROS;
            }

            return AUTH_TYPE_BASIC;
        }

        return AUTH_TYPE_NONE;
    }

    Map<String, Object> toDoc(AuthzAuditEvent event) {
        Map<String, Object> doc = new HashMap<>();

        doc.put("id", event.getEventId());
        doc.put("access", event.getAccessType());
        doc.put("enforcer", event.getAclEnforcer());
        doc.put("agent", event.getAgentId());
        doc.put("repo", event.getRepositoryName());
        doc.put("sess", event.getSessionId());
        doc.put("reqUser", event.getUser());
        doc.put("reqData", event.getRequestData());
        doc.put("resource", event.getResourcePath());
        doc.put("cliIP", event.getClientIP());
        doc.put("logType", event.getLogType());
        doc.put("result", event.getAccessResult());
        doc.put("policy", event.getPolicyId());
        doc.put("repoType", event.getRepositoryType());
        doc.put("resType", event.getResourceType());
        doc.put("reason", event.getResultReason());
        doc.put("action", event.getAction());
        doc.put("evtTime", formatDate(event.getEventTime()));
        doc.put("seq_num", event.getSeqNum());
        doc.put("event_count", event.getEventCount());
        doc.put("event_dur_ms", event.getEventDurationMS());
        doc.put("tags", event.getTags());
        doc.put("datasets", event.getDatasets());
        doc.put("projects", event.getProjects());
        doc.put("cluster", event.getClusterName());
        doc.put("zoneName", event.getZoneName());
        doc.put("agentHost", event.getAgentHostname());
        doc.put("policyVersion", event.getPolicyVersion());

        return doc;
    }

    private OpenSearchTransport createKerberosTransport(String principal, String keytab) {
        org.apache.http.HttpHost[] hosts   = Arrays.stream(urls.split(",")).map(String::trim).filter(h -> !h.isEmpty()).map(h -> new org.apache.http.HttpHost(h, port, protocol)).toArray(org.apache.http.HttpHost[]::new);
        RestClientBuilder          builder = RestClient.builder(hosts);

        KerberosCredentialsProvider credentialsProvider = CredentialsProviderUtil.getKerberosCredentials(principal, keytab);
        Lookup<AuthSchemeProvider>  authRegistry        = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setDefaultAuthSchemeRegistry(authRegistry));

        RestClient restClient = builder.build();

        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    private OpenSearchTransport createHttpClient5Transport(String resolvedAuthType) {
        org.apache.hc.core5.http.HttpHost[] hosts   = Arrays.stream(urls.split(",")).map(String::trim).filter(h -> !h.isEmpty()).map(h -> new org.apache.hc.core5.http.HttpHost(protocol, h, port)).toArray(org.apache.hc.core5.http.HttpHost[]::new);
        ApacheHttpClient5TransportBuilder   builder = ApacheHttpClient5TransportBuilder.builder(hosts);

        builder.setMapper(new JacksonJsonpMapper());

        if (AUTH_TYPE_BASIC.equals(resolvedAuthType)) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            credentialsProvider.setCredentials(new AuthScope(null, null, -1, null, null), new UsernamePasswordCredentials(user, password.toCharArray()));

            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return builder.build();
    }

    private static String formatDate(Date date) {
        return date != null ? DATE_FORMAT.get().format(date) : null;
    }
}
