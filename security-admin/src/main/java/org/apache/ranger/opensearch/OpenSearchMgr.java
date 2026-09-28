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

package org.apache.ranger.opensearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.ranger.audit.destination.OpenSearchAuditDestination;
import org.apache.ranger.authorization.credutils.CredentialsProviderUtil;
import org.apache.ranger.authorization.credutils.kerberos.KerberosCredentialsProvider;
import org.apache.ranger.common.PropertiesUtil;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import java.security.PrivilegedActionException;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

@Component
public class OpenSearchMgr {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchMgr.class);

    private static final String CONFIG_PREFIX              = OpenSearchAuditDestination.CONFIG_PREFIX;
    private static final String CONFIG_URLS                = OpenSearchAuditDestination.CONFIG_URLS;
    private static final String CONFIG_PORT                = OpenSearchAuditDestination.CONFIG_PORT;
    private static final String CONFIG_PROTOCOL            = OpenSearchAuditDestination.CONFIG_PROTOCOL;
    private static final String CONFIG_USER                = OpenSearchAuditDestination.CONFIG_USER;
    private static final String CONFIG_PASSWORD            = OpenSearchAuditDestination.CONFIG_PASSWORD;
    private static final String CONFIG_INDEX               = OpenSearchAuditDestination.CONFIG_INDEX;
    private static final String CONFIG_AUTH_TYPE           = OpenSearchAuditDestination.CONFIG_AUTH_TYPE;
    private static final String CONFIG_KERBEROS_PRINCIPAL  = OpenSearchAuditDestination.CONFIG_KERBEROS_PRINCIPAL;
    private static final String CONFIG_KERBEROS_KEYTAB     = OpenSearchAuditDestination.CONFIG_KERBEROS_KEYTAB;

    private volatile RestClient client;
    private Subject             subject;
    private String              user;
    private String              password;
    private String              index;
    private String              loginPrincipal;
    private String              loginKeytab;

    public RestClient getClient() {
        RestClient me = client;

        if (me != null && subject != null) {
            KerberosTicket ticket = CredentialsProviderUtil.getTGT(subject);

            try {
                if (new Date().getTime() > ticket.getEndTime().getTime()) {
                    client                                     = null;
                    CredentialsProviderUtil.ticketExpireTime80 = 0;

                    me = connect();
                } else if (CredentialsProviderUtil.ticketWillExpire(ticket)) {
                    subject = CredentialsProviderUtil.login(loginPrincipal, loginKeytab);
                }
            } catch (PrivilegedActionException e) {
                LOG.error("PrivilegedActionException:", e);

                throw new RuntimeException(e);
            }

            return me;
        } else {
            me = connect();
        }

        return me;
    }

    String getIndex() {
        return index;
    }

    void setIndex(String index) {
        this.index = index;
    }

    synchronized RestClient connect() {
        RestClient me = client;

        if (me == null) {
            synchronized (OpenSearchMgr.class) {
                me = client;

                if (me == null) {
                    String urls     = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_URLS);
                    String protocol = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PROTOCOL, "http");

                    user     = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_USER, "");
                    password = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PASSWORD, "");

                    String authType          = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_AUTH_TYPE, "");
                    String kerberosPrincipal = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_KERBEROS_PRINCIPAL, "");
                    String kerberosKeytab    = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_KERBEROS_KEYTAB, "");

                    int port = Integer.parseInt(PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PORT, "9200"));

                    this.index = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_INDEX, "ranger_audits");

                    String parameterString = String.format(Locale.ROOT, "User:%s, %s://%s:%s/%s", user, protocol, urls, port, index);

                    LOG.info("Initializing OpenSearch connection: {}", parameterString);

                    if (urls != null) {
                        urls = urls.trim();
                    }

                    if (StringUtils.isBlank(urls) || "NONE".equalsIgnoreCase(urls)) {
                        LOG.warn("OpenSearch URLs not configured or set to NONE");

                        return null;
                    }

                    try {
                        if (OpenSearchAuditDestination.AUTH_TYPE_KERBEROS.equals(OpenSearchAuditDestination.resolveAuthType(authType, user, password))) {
                            loginPrincipal = OpenSearchAuditDestination.isConfigured(kerberosPrincipal) ? kerberosPrincipal : user;
                            loginKeytab    = OpenSearchAuditDestination.isConfigured(kerberosKeytab) ? kerberosKeytab : password;
                            subject        = CredentialsProviderUtil.login(loginPrincipal, loginKeytab);
                        }

                        RestClientBuilder builder = buildRestClientBuilder(urls, protocol, user, password, port, authType, kerberosPrincipal, kerberosKeytab);

                        client = builder.build();
                        me     = client;
                    } catch (Throwable t) {
                        LOG.error("Cannot connect to OpenSearch: {}", parameterString, t);
                    }
                }
            }
        }

        return me;
    }

    public static RestClientBuilder buildRestClientBuilder(String urls, String protocol, String user, String password, int port) {
        return buildRestClientBuilder(urls, protocol, user, password, port, "", "", "");
    }

    public static RestClientBuilder buildRestClientBuilder(String urls, String protocol, String user, String password, int port, String authType, String kerberosPrincipal, String kerberosKeytab) {
        HttpHost[] hosts = Arrays.stream(urls.split(",")).map(String::trim).filter(h -> !h.isEmpty()).map(h -> new HttpHost(h, port, protocol)).toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(hosts);

        String resolvedAuthType = OpenSearchAuditDestination.resolveAuthType(authType, user, password);

        if (OpenSearchAuditDestination.AUTH_TYPE_KERBEROS.equals(resolvedAuthType)) {
            String principal = OpenSearchAuditDestination.isConfigured(kerberosPrincipal) ? kerberosPrincipal : user;
            String keytab    = OpenSearchAuditDestination.isConfigured(kerberosKeytab) ? kerberosKeytab : password;

            KerberosCredentialsProvider credentialsProvider = CredentialsProviderUtil.getKerberosCredentials(principal, keytab);
            Lookup<AuthSchemeProvider> authRegistry = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                httpClientBuilder.setDefaultAuthSchemeRegistry(authRegistry);
                return httpClientBuilder;
            });
        } else if (OpenSearchAuditDestination.AUTH_TYPE_BASIC.equals(resolvedAuthType)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return builder;
    }
}
