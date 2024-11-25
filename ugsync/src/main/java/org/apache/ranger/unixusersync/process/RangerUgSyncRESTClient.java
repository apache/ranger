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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;

import com.fasterxml.jackson.core.util.JacksonFeature;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.ClientRequestFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;

import java.util.Base64;


public class RangerUgSyncRESTClient extends RangerRESTClient {
	private String AUTH_KERBEROS = "kerberos";

	public RangerUgSyncRESTClient(String policyMgrBaseUrls, String ugKeyStoreFile, String ugKeyStoreFilepwd,
			String ugKeyStoreType, String ugTrustStoreFile, String ugTrustStoreFilepwd, String ugTrustStoreType,
			String authenticationType, String principal, String keytab, String polMgrUsername, String polMgrPassword) {

		super(policyMgrBaseUrls, "", UserGroupSyncConfig.getInstance().getConfig());
		if (!(authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType)
				&& SecureClientLogin.isKerberosCredentialExists(principal, keytab))) {
			setBasicAuthInfo(polMgrUsername, polMgrPassword);
		}

		if (isSSL()) {
			setKeyStoreType(ugKeyStoreType);
			setTrustStoreType(ugTrustStoreType);
			KeyManager[] kmList = getKeyManagers(ugKeyStoreFile, ugKeyStoreFilepwd);
			TrustManager[] tmList = getTrustManagers(ugTrustStoreFile, ugTrustStoreFilepwd);
			SSLContext sslContext = getSSLContext(kmList, tmList);

			ClientBuilder builder = ClientBuilder.newBuilder()
					.sslContext(sslContext)
					.hostnameVerifier(new HostnameVerifier() {
						@Override
						public boolean verify(String hostname, SSLSession session) {
							return session.getPeerHost().equals(hostname);
						}
					})
					.register(JacksonFeature.class);

			setClient(builder.build());

			if (StringUtils.isNotEmpty(getUsername()) && StringUtils.isNotEmpty(getPassword())) {
				getClient().register((ClientRequestFilter) clientRequestContext -> {
                    String token = getUsername() + ":" + getPassword();
                    String encodedToken = Base64.getEncoder().encodeToString(token.getBytes());
                    clientRequestContext.getHeaders().add("Authorization", "Basic " + encodedToken);
                });
			}
		}

		UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();

		super.setMaxRetryAttempts(config.getPolicyMgrMaxRetryAttempts());
		super.setRetryIntervalMs(config.getPolicyMgrRetryIntervalMs());
	}
}
