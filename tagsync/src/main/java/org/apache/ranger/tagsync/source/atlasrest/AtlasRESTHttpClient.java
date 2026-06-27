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

package org.apache.ranger.tagsync.source.atlasrest;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

/**
 * Minimal Atlas REST client for TagSync.
 *
 * <p>Uses {@link HttpURLConnection} instead of {@code AtlasClientV2} so TagSync
 * does not load the Atlas Jersey 1.x client on the same classpath as Ranger's
 * Jersey 2.x REST sink (see RANGER-4076).
 */
public final class AtlasRESTHttpClient {
    /** Class logger. */
    private static final Logger LOG =
            LoggerFactory.getLogger(AtlasRESTHttpClient.class);

    /** Atlas basic search API path (POST). */
    private static final String SEARCH_PATH = "api/atlas/v2/search/basic";

    /** Atlas typedef export API path (GET). */
    private static final String TYPEDEFS_PATH = "api/atlas/v2/types/typedefs/";

    /** Minimum HTTP status treated as an error response. */
    private static final int HTTP_STATUS_CLIENT_ERROR = 400;

    private AtlasRESTHttpClient() {
        // utility class
    }

    /**
     * Search Atlas for entities matching the given parameters.
     *
     * @param restBaseUrl       Atlas REST base URL ending with {@code /}
     * @param searchParameters  search filter (classification, limit, etc.)
     * @param userNamePassword  basic-auth credentials; empty for Kerberos
     * @param useKerberos       {@code true} when TagSync uses a keytab
     * @return parsed search result
     * @throws IOException when the REST call fails
     */
    public static AtlasSearchResult facetedSearch(final String restBaseUrl,
            final SearchParameters searchParameters,
            final String[] userNamePassword, final boolean useKerberos)
            throws IOException {
        String body = AtlasType.toJson(searchParameters);
        String json = postJson(restBaseUrl + SEARCH_PATH, body,
                userNamePassword, useKerberos);

        return AtlasType.fromJson(json, AtlasSearchResult.class);
    }

    /**
     * Download all Atlas type definitions.
     *
     * @param restBaseUrl       Atlas REST base URL ending with {@code /}
     * @param userNamePassword  basic-auth credentials; empty for Kerberos
     * @param useKerberos       {@code true} when TagSync uses a keytab
     * @return typedef bundle from Atlas
     * @throws IOException when the REST call fails
     */
    public static AtlasTypesDef getAllTypeDefs(final String restBaseUrl,
            final String[] userNamePassword, final boolean useKerberos)
            throws IOException {
        String json = getJson(restBaseUrl + TYPEDEFS_PATH, userNamePassword,
                useKerberos);

        return AtlasType.fromJson(json, AtlasTypesDef.class);
    }

    private static String postJson(final String url, final String body,
            final String[] userNamePassword, final boolean useKerberos)
            throws IOException {
        return execute(url, "POST", body, userNamePassword, useKerberos);
    }

    private static String getJson(final String url,
            final String[] userNamePassword, final boolean useKerberos)
            throws IOException {
        return execute(url, "GET", null, userNamePassword, useKerberos);
    }

    private static String execute(final String url, final String method,
            final String body, final String[] userNamePassword,
            final boolean useKerberos) throws IOException {
        if (useKerberosAuth(userNamePassword, useKerberos)) {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();

            ugi.checkTGTAndReloginFromKeytab();

            try {
                return ugi.doAs((PrivilegedExceptionAction<String>) () ->
                        openAndRead(url, method, body, null, true));
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("Atlas REST call interrupted",
                        interruptedException);
            }
        }

        return openAndRead(url, method, body, userNamePassword, false);
    }

    private static boolean useKerberosAuth(final String[] userNamePassword,
            final boolean useKerberos) {
        return useKerberos && (userNamePassword == null
                || userNamePassword.length == 0
                || StringUtils.isBlank(userNamePassword[0]));
    }

    private static String openAndRead(final String url, final String method,
            final String body, final String[] userNamePassword,
            final boolean kerberos) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection) new URL(url).openConnection();

        connection.setRequestMethod(method);
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");

        if (!kerberos && userNamePassword != null
                && userNamePassword.length >= 2
                && StringUtils.isNotEmpty(userNamePassword[0])) {
            String credentials = userNamePassword[0] + ":"
                    + userNamePassword[1];
            String encoded = Base64.getEncoder().encodeToString(
                    credentials.getBytes(StandardCharsets.UTF_8));

            connection.setRequestProperty("Authorization", "Basic " + encoded);
        }

        if (body != null) {
            connection.setDoOutput(true);
            byte[] payload = body.getBytes(StandardCharsets.UTF_8);

            connection.setRequestProperty("Content-Length",
                    String.valueOf(payload.length));

            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(payload);
            }
        }

        int status = connection.getResponseCode();
        InputStream stream = status >= HTTP_STATUS_CLIENT_ERROR
                ? connection.getErrorStream()
                : connection.getInputStream();

        if (stream == null) {
            throw new IOException("Atlas REST " + method + " " + url
                    + " returned HTTP " + status + " with empty body");
        }

        String response = readStream(stream);

        if (status >= HTTP_STATUS_CLIENT_ERROR) {
            throw new IOException("Atlas REST " + method + " " + url
                    + " failed with HTTP " + status + ": " + response);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Atlas REST {} {} -> HTTP {}", method, url, status);
        }

        return response;
    }

    private static String readStream(final InputStream stream)
            throws IOException {
        byte[] buffer = stream.readAllBytes();

        return new String(buffer, StandardCharsets.UTF_8);
    }
}
