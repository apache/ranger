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

package org.apache.ranger.server.tomcat;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.logging.Logger;

public class OpenSearchIndexBootStrapper extends Thread {
    private static final Logger LOG = Logger.getLogger(OpenSearchIndexBootStrapper.class.getName());

    private static final String CONFIG_PREFIX     = "ranger.audit.opensearch";
    private static final String CONFIG_URLS       = CONFIG_PREFIX + ".urls";
    private static final String CONFIG_PORT       = CONFIG_PREFIX + ".port";
    private static final String CONFIG_PROTOCOL   = CONFIG_PREFIX + ".protocol";
    private static final String CONFIG_USER       = CONFIG_PREFIX + ".user";
    private static final String CONFIG_PASSWORD   = CONFIG_PREFIX + ".password";
    private static final String CONFIG_INDEX      = CONFIG_PREFIX + ".index";
    private static final String CONFIG_INTERVAL   = CONFIG_PREFIX + ".time.interval";
    private static final String CONFIG_SHARDS     = CONFIG_PREFIX + ".no.shards";
    private static final String CONFIG_REPLICAS   = CONFIG_PREFIX + ".no.replica";
    private static final String CONFIG_MAX_RETRY  = CONFIG_PREFIX + ".max.retry";
    private static final String SCHEMA_FILE_NAME  = "ranger_opensearch_schema.json";

    private volatile RestClient client;
    private String  index;
    private String  urls;
    private String  protocol;
    private String  user;
    private String  password;
    private int     port;
    private int     noOfShards;
    private int     noOfReplicas;
    private int     maxRetry;
    private long    timeInterval;

    @Override
    public void run() {
        LOG.info("OpenSearchIndexBootStrapper: starting...");

        readConfig();

        if (StringUtils.isBlank(urls) || "NONE".equalsIgnoreCase(urls)) {
            LOG.severe("OpenSearch URL is not configured. Aborting bootstrap.");

            return;
        }

        int retryCounter = 0;

        while (maxRetry == -1 || retryCounter < maxRetry) {
            retryCounter++;

            try {
                LOG.info("Trying to acquire OpenSearch connection");

                connect();

                if (client == null) {
                    logErrorAndWait(retryCounter, "Failed to create OpenSearch client");

                    continue;
                }

                LOG.info("Connection to OpenSearch established successfully");

                if (indexExists()) {
                    LOG.info("Index '" + index + "' already exists. Bootstrap complete.");

                    return;
                }

                createIndex();

                LOG.info("Index '" + index + "' created successfully.");

                return;
            } catch (Exception e) {
                logErrorAndWait(retryCounter, e.getMessage());
            }
        }

        LOG.severe("OpenSearch index bootstrap failed after " + retryCounter + " attempts.");
    }

    private void readConfig() {
        urls         = EmbeddedServerUtil.getConfig(CONFIG_URLS, "");
        protocol     = EmbeddedServerUtil.getConfig(CONFIG_PROTOCOL, "http");
        user         = EmbeddedServerUtil.getConfig(CONFIG_USER, "");
        password     = EmbeddedServerUtil.getConfig(CONFIG_PASSWORD, "");
        port         = Integer.parseInt(EmbeddedServerUtil.getConfig(CONFIG_PORT, "9200"));
        index        = EmbeddedServerUtil.getConfig(CONFIG_INDEX, "ranger_audits");
        noOfShards   = Integer.parseInt(EmbeddedServerUtil.getConfig(CONFIG_SHARDS, "1"));
        noOfReplicas = Integer.parseInt(EmbeddedServerUtil.getConfig(CONFIG_REPLICAS, "1"));
        maxRetry     = Integer.parseInt(EmbeddedServerUtil.getConfig(CONFIG_MAX_RETRY, "30"));
        timeInterval = Long.parseLong(EmbeddedServerUtil.getConfig(CONFIG_INTERVAL, "60000"));
    }

    private void connect() {
        if (client != null) {
            return;
        }

        HttpHost[] hosts = Arrays.stream(urls.split(",")).map(String::trim).filter(h -> !h.isEmpty()).map(h -> new HttpHost(h, port, protocol)).toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(hosts);

        if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && !"NONE".equalsIgnoreCase(user) && !"NONE".equalsIgnoreCase(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        client = builder.build();
    }

    private boolean indexExists() throws IOException {
        Request request = new Request("HEAD", "/" + index);
        request.addParameter("ignore", "404");

        Response response = client.performRequest(request);

        return response.getStatusLine().getStatusCode() == 200;
    }

    private void createIndex() throws IOException {
        String schemaJson = loadSchemaFile();
        String body       = buildCreateIndexBody(schemaJson);

        Request request = new Request("PUT", "/" + index);

        request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        String result = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() >= 400) {
            throw new IOException("Failed to create index '" + index + "': " + result);
        }

        LOG.info("Create index response: " + result);
    }

    private String buildCreateIndexBody(String mappingsJson) {
        return "{\"settings\":{\"number_of_shards\":" + noOfShards
                + ",\"number_of_replicas\":" + noOfReplicas
                + "},\"mappings\":" + mappingsJson + "}";
    }

    private String loadSchemaFile() throws IOException {
        String jarLocation;

        try {
            jarLocation = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        } catch (Exception e) {
            throw new IOException("Cannot determine Ranger home directory", e);
        }

        String rangerHomeDir = new File(jarLocation).getParentFile().getParentFile().getParentFile().getPath();
        String schemaPath    = Paths.get(rangerHomeDir, "contrib", "opensearch_for_audit_setup", "conf", SCHEMA_FILE_NAME).toString();

        File schemaFile = new File(schemaPath);

        if (!schemaFile.exists()) {
            throw new IOException("OpenSearch schema file not found: " + schemaPath);
        }

        return new String(Files.readAllBytes(Paths.get(schemaPath)));
    }

    private void logErrorAndWait(int retryCounter, String message) {
        int attemptsLeft = maxRetry == -1 ? -1 : maxRetry - retryCounter;

        LOG.severe("Error during OpenSearch bootstrap. [retrying after " + timeInterval
                + " ms]. Attempts left: " + attemptsLeft + ". Error: " + message);

        try {
            Thread.sleep(timeInterval);
        } catch (InterruptedException e) {
            LOG.warning("OpenSearch bootstrap thread interrupted");

            Thread.currentThread().interrupt();
        }
    }
}
