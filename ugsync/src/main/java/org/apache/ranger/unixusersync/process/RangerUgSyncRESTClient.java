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

package org.apache.ranger.unixusersync.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.HttpHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class RangerUgSyncRESTClient extends RangerRESTClient {
    public RangerUgSyncRESTClient(String policyMgrBaseUrls, String ugKeyStoreFile, String ugKeyStoreFilepwd, String ugKeyStoreType, String ugTrustStoreFile, String ugTrustStoreFilepwd, String ugTrustStoreType, String authenticationType, String principal, String keytab, String polMgrUsername, String polMgrPassword) {
        super(policyMgrBaseUrls, "", UserGroupSyncConfig.getInstance().getConfig(), "ranger.usersync");

        boolean isKerberized = "kerberos".equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab);

        if (!isKerberized) {
            setBasicAuthInfo(polMgrUsername, polMgrPassword);
        }

        if (isSSL()) {
            setKeyStoreType(ugKeyStoreType);
            setTrustStoreType(ugTrustStoreType);

            KeyManager[]   kmList     = getKeyManagers(ugKeyStoreFile, ugKeyStoreFilepwd);
            TrustManager[] tmList     = getTrustManagers(ugTrustStoreFile, ugTrustStoreFilepwd);
            SSLContext     sslContext = getSSLContext(kmList, tmList);

            HostnameVerifier hv = new HostnameVerifier() {
                @Override
                public boolean verify(String urlHostName, SSLSession session) {
                    return session.getPeerHost().equals(urlHostName);
                }
            };

            ClientConfig config = new ClientConfig();
            RangerJersey2ClientBuilder.applyAntiMoxyConfiguration(config);
            config.property(ClientProperties.CONNECT_TIMEOUT, getRestClientConnTimeOutMs());
            config.property(ClientProperties.READ_TIMEOUT, getRestClientReadTimeOutMs());
            RangerJersey2ClientBuilder.validateAntiMoxyConfiguration(config);

            if (StringUtils.isNotEmpty(getUsername()) && StringUtils.isNotEmpty(getPassword())) {
                final String user = getUsername();
                final String pass = getPassword();
                config.register(new javax.ws.rs.client.ClientRequestFilter() {
                    @Override
                    public void filter(javax.ws.rs.client.ClientRequestContext requestContext) {
                        String authHeader = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
                        requestContext.getHeaders().add(HttpHeaders.AUTHORIZATION, authHeader);
                    }
                });
            }

            Client secureClient = RangerJersey2ClientBuilder.newBuilder()
                    .sslContext(sslContext)
                    .hostnameVerifier(hv)
                    .withConfig(config)
                    .build();
            setClient(secureClient);

            UserGroupSyncConfig userGroupSyncConfig = UserGroupSyncConfig.getInstance();
            if (userGroupSyncConfig.isUserSyncRangerCookieEnabled()) {
                setCookieAuthClient(getClient());
            }
        }

        UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();

        super.setMaxRetryAttempts(config.getPolicyMgrMaxRetryAttempts());
        super.setRetryIntervalMs(config.getPolicyMgrRetryIntervalMs());
    }
}
