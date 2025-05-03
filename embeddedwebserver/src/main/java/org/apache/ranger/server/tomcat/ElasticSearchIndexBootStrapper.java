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

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.ranger.authorization.credutils.CredentialsProviderUtil;
import org.apache.ranger.authorization.credutils.kerberos.KerberosCredentialsProvider;
import org.apache.ranger.credentialapi.CredentialReader;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class ElasticSearchIndexBootStrapper extends Thread {
    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexBootStrapper.class.getName());

    private static final String ES_CONFIG_USERNAME             = "ranger.audit.elasticsearch.user";
    private static final String ES_CONFIG_PASSWORD             = "ranger.audit.elasticsearch.password";
    private static final String ES_CONFIG_URLS                 = "ranger.audit.elasticsearch.urls";
    private static final String ES_CONFIG_PORT                 = "ranger.audit.elasticsearch.port";
    private static final String ES_CONFIG_PROTOCOL             = "ranger.audit.elasticsearch.protocol";
    private static final String ES_CONFIG_INDEX                = "ranger.audit.elasticsearch.index";
    private static final String ES_TIME_INTERVAL               = "ranger.audit.elasticsearch.time.interval";
    private static final String ES_NO_SHARDS                   = "ranger.audit.elasticsearch.no.shards";
    private static final String ES_NO_REPLICA                  = "ranger.audit.elasticsearch.no.replica";
    private static final String ES_CREDENTIAL_PROVIDER_PATH    = "ranger.credential.provider.path";
    private static final String ES_CREDENTIAL_ALIAS            = "ranger.audit.elasticsearch.credential.alias";
    private static final String ES_BOOTSTRAP_MAX_RETRY         = "ranger.audit.elasticsearch.max.retry";
    private static final String DEFAULT_INDEX_NAME             = "ranger_audits";
    private static final String ES_RANGER_AUDIT_SCHEMA_FILE    = "ranger_es_schema.json";
    private static final long   DEFAULT_ES_TIME_INTERVAL_MS    = 60000L;
    private static final int    TRY_UNTIL_SUCCESS              = -1;
    private static final int    DEFAULT_ES_BOOTSTRAP_MAX_RETRY = 30;

    private volatile RestHighLevelClient client;

    private final AtomicLong lastLoggedAt = new AtomicLong(0);
    private final Long       timeInterval;
    private final String     user;
    private       String     password;
    private final String     hosts;
    private final String     protocol;
    private final String     index;
    private final String     esRangerAuditSchemaJson;
    private final int         port;
    private final int         maxRetry;
    private       int         retryCounter;
    private final int         noOfReplicas;
    private final int         noOfShards;
    private       boolean     isCompleted;

    public ElasticSearchIndexBootStrapper() throws IOException {
        LOG.info("Starting Ranger audit schema setup in ElasticSearch.");

        timeInterval = EmbeddedServerUtil.getLongConfig(ES_TIME_INTERVAL, DEFAULT_ES_TIME_INTERVAL_MS);
        user         = EmbeddedServerUtil.getConfig(ES_CONFIG_USERNAME);
        hosts        = EmbeddedServerUtil.getHosts(EmbeddedServerUtil.getConfig(ES_CONFIG_URLS));
        port         = EmbeddedServerUtil.getIntConfig(ES_CONFIG_PORT, 9200);
        protocol     = EmbeddedServerUtil.getConfig(ES_CONFIG_PROTOCOL, "http");
        index        = EmbeddedServerUtil.getConfig(ES_CONFIG_INDEX, DEFAULT_INDEX_NAME);
        password     = EmbeddedServerUtil.getConfig(ES_CONFIG_PASSWORD);
        noOfReplicas = EmbeddedServerUtil.getIntConfig(ES_NO_REPLICA, 1);
        noOfShards   = EmbeddedServerUtil.getIntConfig(ES_NO_SHARDS, 1);
        maxRetry     = EmbeddedServerUtil.getIntConfig(ES_BOOTSTRAP_MAX_RETRY, DEFAULT_ES_BOOTSTRAP_MAX_RETRY);

        String jarLocation = null;

        try {
            jarLocation = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        } catch (Exception ex) {
            LOG.severe("Error finding base location:" + ex);
        }

        String rangerHomeDir = new File(jarLocation).getParentFile().getParentFile().getParentFile().getPath();
        Path   esSchemaPath  = Paths.get(rangerHomeDir, "contrib", "elasticsearch_for_audit_setup", "conf", ES_RANGER_AUDIT_SCHEMA_FILE);

        esRangerAuditSchemaJson = new String(Files.readAllBytes(esSchemaPath), StandardCharsets.UTF_8);

        String providerPath     = EmbeddedServerUtil.getConfig(ES_CREDENTIAL_PROVIDER_PATH);
        String credentialAlias  = EmbeddedServerUtil.getConfig(ES_CREDENTIAL_ALIAS, ES_CONFIG_PASSWORD);
        String keyStoreFileType = EmbeddedServerUtil.getConfig("ranger.keystore.file.type", KeyStore.getDefaultType());

        if (providerPath != null && credentialAlias != null) {
            password = CredentialReader.getDecryptedString(providerPath.trim(), credentialAlias.trim(), keyStoreFileType);

            if (StringUtils.isBlank(password) || "none".equalsIgnoreCase(password.trim())) {
                password = EmbeddedServerUtil.getConfig(ES_CONFIG_PASSWORD);
            }
        }
    }

    public static RestClientBuilder getRestClientBuilder(String urls, String protocol, String user, String password, int port) {
        RestClientBuilder restClientBuilder = RestClient.builder(EmbeddedServerUtil.toArray(urls, ",").stream().map(x -> new HttpHost(x, port, protocol)).toArray(HttpHost[]::new));

        if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && !user.equalsIgnoreCase("NONE") && !password.equalsIgnoreCase("NONE")) {
            if (password.contains("keytab") && new File(password).exists()) {
                final KerberosCredentialsProvider credentialsProvider = CredentialsProviderUtil.getKerberosCredentials(user, password);
                Lookup<AuthSchemeProvider>        authSchemeRegistry  = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

                restClientBuilder.setHttpClientConfigCallback(clientBuilder -> {
                    clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    clientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
                    return clientBuilder;
                });
            } else {
                final CredentialsProvider credentialsProvider = CredentialsProviderUtil.getBasicCredentials(user, password);

                restClientBuilder.setHttpClientConfigCallback(clientBuilder -> clientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
        } else {
            LOG.severe("ElasticSearch Credentials not provided!!");

            final CredentialsProvider credentialsProvider = null;

            restClientBuilder.setHttpClientConfigCallback(clientBuilder -> clientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return restClientBuilder;
    }

    public void run() {
        LOG.info("Started run method");

        if (StringUtils.isNotBlank(hosts)) {
            LOG.info("Elastic search hosts=" + hosts + ", index=" + index);

            while (!isCompleted && (maxRetry == TRY_UNTIL_SUCCESS || retryCounter < maxRetry)) {
                try {
                    LOG.info("Trying to acquire elastic search connection");

                    if (connect()) {
                        LOG.info("Connection to elastic search established successfully");

                        if (createIndex()) {
                            isCompleted = true;
                            break;
                        } else {
                            logErrorMessageAndWait("Error while performing operations on elasticsearch. ", null);
                        }
                    } else {
                        logErrorMessageAndWait("Cannot connect to elasticsearch kindly check the elasticsearch related configs. ", null);
                    }
                } catch (Exception ex) {
                    logErrorMessageAndWait("Error while validating elasticsearch index ", ex);
                }
            }
        } else {
            LOG.severe("elasticsearch hosts values are empty. Please set property " + ES_CONFIG_URLS);
        }
    }

    private String connectionString() {
        return String.format(Locale.ROOT, "User:%s, %s://%s:%s/%s", user, protocol, hosts, port, index);
    }

    private synchronized boolean connect() {
        RestHighLevelClient me = client;

        if (me == null) {
            synchronized (ElasticSearchIndexBootStrapper.class) {
                me = client;

                if (me == null) {
                    try {
                        createClient();

                        me = client;
                    } catch (Exception ex) {
                        LOG.severe("Can't connect to elasticsearch server. host=" + hosts + ", index=" + index + ex);
                    }
                }
            }
        }

        return me != null;
    }

    private void createClient() {
        try {
            RestClientBuilder restClientBuilder = getRestClientBuilder(hosts, protocol, user, password, port);

            client = new RestHighLevelClient(restClientBuilder);
        } catch (Throwable t) {
            lastLoggedAt.updateAndGet(lastLoggedAt -> {
                long now     = System.currentTimeMillis();
                long elapsed = now - lastLoggedAt;

                if (elapsed > TimeUnit.MINUTES.toMillis(1)) {
                    LOG.severe("Can't connect to ElasticSearch server: " + connectionString() + t);

                    return now;
                } else {
                    return lastLoggedAt;
                }
            });
        }
    }

    private boolean createIndex() {
        boolean exits = false;

        if (client == null) {
            connect();
        }

        if (client != null) {
            try {
                exits = client.indices().open(new OpenIndexRequest(this.index), RequestOptions.DEFAULT).isShardsAcknowledged();
            } catch (Exception e) {
                LOG.info("Index " + this.index + " not available.");
            }

            if (!exits) {
                LOG.info("Index does not exist. Attempting to create index:" + this.index);

                CreateIndexRequest request = new CreateIndexRequest(this.index);

                if (this.noOfShards >= 0 && this.noOfReplicas >= 0) {
                    request.settings(Settings.builder().put("index.number_of_shards", this.noOfShards).put("index.number_of_replicas", this.noOfShards));
                }

                request.mapping(esRangerAuditSchemaJson, XContentType.JSON);
                request.setMasterTimeout(TimeValue.timeValueMinutes(1));
                request.setTimeout(TimeValue.timeValueMinutes(2));

                try {
                    CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

                    if (createIndexResponse != null) {
                        exits = client.indices().open(new OpenIndexRequest(this.index), RequestOptions.DEFAULT).isShardsAcknowledged();

                        if (exits) {
                            LOG.info("Index " + this.index + " created successfully.");
                        }
                    }
                } catch (Exception e) {
                    LOG.severe("Unable to create Index. Reason:" + e);
                    e.printStackTrace();
                }
            } else {
                LOG.info("Index " + this.index + " is already created.");
            }
        }

        return exits;
    }

    private void logErrorMessageAndWait(String msg, Exception exception) {
        retryCounter++;

        String attemptMessage;

        if (maxRetry != TRY_UNTIL_SUCCESS) {
            attemptMessage = (retryCounter == maxRetry) ? ("Maximum attempts reached for setting up elasticsearch.") : ("[retrying after " + timeInterval + " ms]. No. of attempts left : " + (maxRetry - retryCounter) + " . Maximum attempts : " + maxRetry);
        } else {
            attemptMessage = "[retrying after " + timeInterval + " ms]";
        }

        StringBuilder errorBuilder = new StringBuilder(msg);

        if (exception != null) {
            errorBuilder.append("Error : ".concat(exception.getMessage() + ". "));
        }

        errorBuilder.append(attemptMessage);

        LOG.severe(errorBuilder.toString());

        try {
            Thread.sleep(timeInterval);
        } catch (InterruptedException ex) {
            LOG.info("sleep interrupted: " + ex.getMessage());
        }
    }
}
