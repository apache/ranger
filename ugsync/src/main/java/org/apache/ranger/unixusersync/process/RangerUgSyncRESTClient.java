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

import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.core.Cookie;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

public class RangerUgSyncRESTClient extends RangerRESTClient {

	private static final Log LOG = LogFactory.getLog(RangerUgSyncRESTClient.class);

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
			ClientConfig config = new DefaultClientConfig();

			config.getClasses().add(JacksonJsonProvider.class); // to handle List<> unmarshalling
			HostnameVerifier hv = new HostnameVerifier() {
				public boolean verify(String urlHostName, SSLSession session) {
					return session.getPeerHost().equals(urlHostName);
				}
			};
			config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hv, sslContext));

			setClient(Client.create(config));
			if (StringUtils.isNotEmpty(getUsername()) && StringUtils.isNotEmpty(getPassword())) {
				getClient().addFilter(new HTTPBasicAuthFilter(getUsername(), getPassword()));
			}
		}
	}

	public ClientResponse get(String relativeURL, Map<String, String> params, Cookie sessionId) throws Exception {
		ClientResponse response = null;
		int startIndex = getLastKnownActiveUrlIndex();
		int currentIndex = 0;

		for (int index = 0; index < getConfiguredURLs().size(); index++) {
			try {
				currentIndex = (startIndex + index) % getConfiguredURLs().size();

				WebResource webResource = createWebResourceForCookieAuth(currentIndex, relativeURL);
				webResource = setQueryParams(webResource, params);
				WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
				response = br.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).get(ClientResponse.class);
				if (response != null) {
					setLastKnownActiveUrlIndex(currentIndex);
					break;
				}
			} catch (ClientHandlerException e) {
				LOG.warn("Failed to communicate with Ranger Admin, URL : " + getConfiguredURLs().get(currentIndex));
				processException(index, e);
			}
		}
		return response;
	}

	public ClientResponse post(String relativeURL, Map<String, String> params, Object obj, Cookie sessionId)
			throws Exception {
		ClientResponse response = null;
		int startIndex = getLastKnownActiveUrlIndex();
		int currentIndex = 0;

		for (int index = 0; index < getConfiguredURLs().size(); index++) {
			try {
				currentIndex = (startIndex + index) % getConfiguredURLs().size();

				WebResource webResource = createWebResourceForCookieAuth(currentIndex, relativeURL);
				webResource = setQueryParams(webResource, params);
				WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
				response = br.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON)
						.post(ClientResponse.class, toJson(obj));
				if (response != null) {
					setLastKnownActiveUrlIndex(currentIndex);
					break;
				}
			} catch (ClientHandlerException e) {
				LOG.warn("Failed to communicate with Ranger Admin, URL : " + getConfiguredURLs().get(currentIndex));
				processException(index, e);
			}
		}
		return response;
	}

	public ClientResponse delete(String relativeURL, Map<String, String> params, Cookie sessionId) throws Exception {
		ClientResponse response = null;
		int startIndex = getLastKnownActiveUrlIndex();
		int currentIndex = 0;
		for (int index = 0; index < getConfiguredURLs().size(); index++) {
			try {
				currentIndex = (startIndex + index) % getConfiguredURLs().size();

				WebResource webResource = createWebResourceForCookieAuth(currentIndex, relativeURL);
				webResource = setQueryParams(webResource, params);
				WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
				response = br.delete(ClientResponse.class);
				if (response != null) {
					setLastKnownActiveUrlIndex(currentIndex);
					break;
				}
			} catch (ClientHandlerException e) {
				LOG.warn("Failed to communicate with Ranger Admin, URL : " + getConfiguredURLs().get(currentIndex));
				processException(index, e);
			}
		}
		return response;
	}
}
