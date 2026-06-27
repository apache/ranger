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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Minimal Atlas REST client for TagSync.
 *
 * <p>Uses {@link RangerRESTClient} for SSL, HA URL failover, and auth so TagSync
 * does not load the Atlas Jersey 1.x client on the same classpath as Ranger's
 * Jersey 2.x REST sink.
 */
public final class AtlasRESTHttpClient {
    /** Class logger. */
    private static final Logger LOG =
            LoggerFactory.getLogger(AtlasRESTHttpClient.class);

    /** Atlas basic search API path (POST). */
    private static final String SEARCH_PATH = "/api/atlas/v2/search/basic";

    /** Atlas typedef export API path (GET). */
    private static final String TYPEDEFS_PATH = "/api/atlas/v2/types/typedefs/";

    private final RangerRESTClient restClient;
    private final boolean          kerberized;

    /**
     * Create an Atlas REST client backed by {@link RangerRESTClient}.
     *
     * @param restUrls          comma-separated Atlas REST base URLs
     * @param sslConfigFile     SSL config file for HTTPS endpoints
     * @param config            Hadoop configuration
     * @param userNamePassword  basic-auth credentials; ignored when kerberized
     * @param kerberized        {@code true} when TagSync uses a keytab
     */
    public AtlasRESTHttpClient(final String restUrls, final String sslConfigFile, final Configuration config, final String[] userNamePassword, final boolean kerberized) {
        this.kerberized = kerberized;
        this.restClient = new RangerRESTClient(restUrls, sslConfigFile, config, "ranger.tagsync");

        if (userNamePassword != null && userNamePassword.length >= 2 && StringUtils.isNotEmpty(userNamePassword[0])) {
            restClient.setBasicAuthInfo(userNamePassword[0], userNamePassword[1]);
        }

        restClient.getClient();
    }

    /**
     * Search Atlas for entities matching the given parameters.
     *
     * @param searchParameters search filter (classification, limit, etc.)
     * @return parsed search result
     * @throws Exception when the REST call fails on all configured URLs
     */
    public AtlasSearchResult facetedSearch(final SearchParameters searchParameters)
            throws Exception {
        Response response = execute(() -> restClient.post(SEARCH_PATH, null, searchParameters));
        String json = readResponseBody(response, SEARCH_PATH);
        return AtlasType.fromJson(json, AtlasSearchResult.class);
    }

    /**
     * Download all Atlas type definitions.
     *
     * @return typedef bundle from Atlas
     * @throws Exception when the REST call fails on all configured URLs
     */
    public AtlasTypesDef getAllTypeDefs() throws Exception {
        Response response = execute(() -> restClient.get(TYPEDEFS_PATH, null));
        String json = readResponseBody(response, TYPEDEFS_PATH);

        return AtlasType.fromJson(json, AtlasTypesDef.class);
    }

    private Response execute(final RestCall restCall) throws Exception {
        if (kerberized) {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();

            ugi.checkTGTAndReloginFromKeytab();

            try {
                return ugi.doAs((PrivilegedExceptionAction<Response>) restCall::run);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("Atlas REST call interrupted", interruptedException);
            }
        }

        return restCall.run();
    }

    private String readResponseBody(final Response response, final String relativeUrl)
            throws IOException {
        if (response == null) {
            throw new IOException("Atlas REST " + relativeUrl + " failed: no response from any configured URL");
        }

        int status = response.getStatus();
        String body = response.hasEntity() ? response.readEntity(String.class) : "";

        if (status != Response.Status.OK.getStatusCode()) {
            throw new IOException("Atlas REST " + relativeUrl + " failed with HTTP " + status + ": " + body);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Atlas REST {} -> HTTP {}", relativeUrl, status);
        }

        return body;
    }

    @FunctionalInterface
    private interface RestCall {
        Response run() throws Exception;
    }
}
