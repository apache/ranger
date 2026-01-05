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

import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.client.Client;

public class RangerUgSyncRESTClient extends RangerRESTClient {
    public RangerUgSyncRESTClient(String policyMgrBaseUrls, String ugKeyStoreFile, String ugKeyStoreFilepwd, String ugKeyStoreType, String ugTrustStoreFile, String ugTrustStoreFilepwd, String ugTrustStoreType, String authenticationType, String principal, String keytab, String polMgrUsername, String polMgrPassword) {
        super(policyMgrBaseUrls, "", UserGroupSyncConfig.getInstance().getConfig());

        String authKerberos = "kerberos";

        UserGroupSyncConfig userGroupSyncConfig = UserGroupSyncConfig.getInstance();
        if (!(authenticationType != null && authKerberos.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab))) {
            setBasicAuthInfo(polMgrUsername, polMgrPassword);
        }

        if (isSSL()) {
            setKeyStoreType(ugKeyStoreType);
            setTrustStoreType(ugTrustStoreType);
            KeyManager[]   kmList       = getKeyManagers(ugKeyStoreFile, ugKeyStoreFilepwd);
            TrustManager[] tmList       = getTrustManagers(ugTrustStoreFile, ugTrustStoreFilepwd);
            SSLContext     sslContext   = getSSLContext(kmList, tmList);

            HostnameVerifier hv = new HostnameVerifier() {
                public boolean verify(String urlHostName, SSLSession session) {
                    return session.getPeerHost().equals(urlHostName);
                }
            };

            // Use RangerJersey2ClientBuilder to create secure client with MOXy prevention
            Client secureClient = RangerJersey2ClientBuilder.createSecureClient(sslContext, hv, getRestClientConnTimeOutMs(), getRestClientReadTimeOutMs());
            setClient(secureClient);

            // Basic authentication is already handled by the base class RangerRESTClient
            if (userGroupSyncConfig.isUserSyncRangerCookieEnabled()) {
                Client  client = getClient();
                setCookieAuthClient(client);
            }
        }

        super.setMaxRetryAttempts(userGroupSyncConfig.getPolicyMgrMaxRetryAttempts());
        super.setRetryIntervalMs(userGroupSyncConfig.getPolicyMgrRetryIntervalMs());
    }
}
