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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SolrCollectionBootstrapper extends Thread {
    private static final Logger logger = Logger.getLogger(SolrCollectionBootstrapper.class.getName());

    public static final  String DEFAULT_COLLECTION_NAME              = "ranger_audits";
    public static final  String DEFAULT_CONFIG_NAME                  = "ranger_audits";
    public static final  long   DEFAULT_SOLR_TIME_INTERVAL_MS        = 60000L;
    public static final  int    DEFAULT_VALUE                        = 1;
    public static final  int    DEFAULT_SOLR_BOOTSTRP_MAX_RETRY      = -1;
    static final         String SOLR_ZK_HOSTS                        = "ranger.audit.solr.zookeepers";
    static final         String SOLR_COLLECTION_NAME_KEY             = "ranger.audit.solr.collection.name";
    static final         String SOLR_CONFIG_NAME_KEY                 = "ranger.audit.solr.config.name";
    static final         String CONFIG_SET_LOCATION                  = "ranger.audit.solr.configset.location";
    static final         String SOLR_NO_SHARDS                       = "ranger.audit.solr.no.shards";
    static final         String SOLR_MAX_SHARD_PER_NODE              = "ranger.audit.solr.max.shards.per.node";
    static final         String SOLR_NO_REPLICA                      = "ranger.audit.solr.no.replica";
    static final         String SOLR_TIME_INTERVAL                   = "ranger.audit.solr.time.interval";
    static final         String SOLR_BOOTSTRP_MAX_RETRY              = "ranger.audit.solr.max.retry";
    static final         String PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    private static final String AUTH_TYPE_KERBEROS                   = "kerberos";
    private static final String AUTHENTICATION_TYPE                  = "hadoop.security.authentication";
    private static final String RANGER_SERVICE_HOSTNAME              = "ranger.service.host";
    private static final String ADMIN_USER_PRINCIPAL                 = "ranger.admin.kerberos.principal";
    private static final String SSL_ENABLED_PARAM                    = "ranger.service.https.attrib.ssl.enabled";
    private static final int    TRY_UNTIL_SUCCESS                    = -1;

    private final String  customConfigSetLocation;
    private final File    configSetFolder;
    private final boolean isSSLEnabled;

    boolean         solrCloudMode;
    boolean         isCompleted;
    boolean         isKERBEROS;
    String          principal;
    String          hostName;
    String          keytab;
    String          nameRules;
    String          solrCollectionName;
    String          solrConfigName;
    Path            pathForCloudMode;
    int             noOfReplicas;
    int             maxNodePerShards;
    int             maxRetry;
    int             retryCounter;
    Long            timeInterval;
    SolrClient      solrClient;
    CloudSolrClient solrCloudClient;
    SolrZooKeeper   solrZookeeper;
    SolrZkClient    zkClient;

    public SolrCollectionBootstrapper() throws IOException {
        logger.info("Starting Solr Setup");
        logger.info("AUTHENTICATION_TYPE : " + EmbeddedServerUtil.getConfig(AUTHENTICATION_TYPE));

        if (EmbeddedServerUtil.getConfig(AUTHENTICATION_TYPE) != null && EmbeddedServerUtil.getConfig(AUTHENTICATION_TYPE).trim().equalsIgnoreCase(AUTH_TYPE_KERBEROS)) {
            isKERBEROS = true;
            hostName   = EmbeddedServerUtil.getConfig(RANGER_SERVICE_HOSTNAME);

            try {
                principal = SecureClientLogin.getPrincipal(EmbeddedServerUtil.getConfig(ADMIN_USER_PRINCIPAL), hostName);
            } catch (IOException ignored) {
                logger.warning("Failed to get ranger.admin.kerberos.principal. Reason: " + ignored);
            }
        }

        solrCollectionName = EmbeddedServerUtil.getConfig(SOLR_COLLECTION_NAME_KEY, DEFAULT_COLLECTION_NAME);

        logger.info("Solr Collection name provided is : " + solrCollectionName);

        solrConfigName = EmbeddedServerUtil.getConfig(SOLR_CONFIG_NAME_KEY, DEFAULT_CONFIG_NAME);

        logger.info("Solr Config name provided is : " + solrConfigName);

        noOfReplicas = EmbeddedServerUtil.getIntConfig(SOLR_NO_REPLICA, DEFAULT_VALUE);

        logger.info("No. of replicas provided is : " + noOfReplicas);

        maxNodePerShards = EmbeddedServerUtil.getIntConfig(SOLR_MAX_SHARD_PER_NODE, DEFAULT_VALUE);

        logger.info("Max no of nodes per shards provided is : " + maxNodePerShards);

        timeInterval = EmbeddedServerUtil.getLongConfig(SOLR_TIME_INTERVAL, DEFAULT_SOLR_TIME_INTERVAL_MS);

        logger.info("Solr time interval provided is : " + timeInterval);

        maxRetry = EmbeddedServerUtil.getIntConfig(SOLR_BOOTSTRP_MAX_RETRY, DEFAULT_SOLR_BOOTSTRP_MAX_RETRY);

        if (System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null) {
            System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG, "/dev/null");
        }

        String basedir     = new File(".").getCanonicalPath();
        String solrFileDir = new File(basedir).getParent();

        this.customConfigSetLocation = EmbeddedServerUtil.getConfig(CONFIG_SET_LOCATION);

        logger.info("Provided custom configSet location : " + this.customConfigSetLocation);

        if (StringUtils.isNotEmpty(this.customConfigSetLocation)) {
            this.configSetFolder = new File(this.customConfigSetLocation);
        } else {
            pathForCloudMode = Paths.get(solrFileDir, "contrib", "solr_for_audit_setup", "conf");
            configSetFolder  = pathForCloudMode.toFile();
        }

        String sslEnabledProp = EmbeddedServerUtil.getConfig(SSL_ENABLED_PARAM);

        isSSLEnabled = ("true".equalsIgnoreCase(sslEnabledProp));
    }

    public static Map postDataAndGetResponse(CloudSolrClient cloudClient, String uri, ByteBuffer bytarr) throws IOException {
        HttpPost   httpPost = null;
        HttpEntity entity;
        String     response = null;
        Map        m        = null;

        try {
            httpPost = new HttpPost(uri);

            httpPost.setHeader("Content-Type", "application/octet-stream");
            httpPost.setEntity(new ByteArrayEntity(bytarr.array(), bytarr.arrayOffset(), bytarr.limit()));

            entity = cloudClient.getLbClient().getHttpClient().execute(httpPost).getEntity();

            try {
                response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                m        = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
            } catch (JSONParser.ParseException e) {
                logger.severe("Error response: " + response);
            }
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }

        return m;
    }

    public void run() {
        logger.info("Started run method");

        List<String> zookeeperHosts = getZkHosts();

        if (zookeeperHosts != null && !zookeeperHosts.isEmpty() && zookeeperHosts.stream().noneMatch(h -> h.equalsIgnoreCase("none"))) {
            logger.info("Solr zkHosts=" + zookeeperHosts + ", collectionName=" + solrCollectionName);

            while (!isCompleted && (maxRetry == TRY_UNTIL_SUCCESS || retryCounter < maxRetry)) {
                try {
                    if (connect(zookeeperHosts)) {
                        if (solrCloudMode) {
                            if (uploadConfiguration() && createCollection()) {
                                isCompleted = true;
                                break;
                            } else {
                                logErrorMessageAndWait("Error while performing operations on solr. ", null);
                            }
                        }
                    } else {
                        logErrorMessageAndWait("Cannot connect to solr kindly check the solr related configs. ", null);
                    }
                } catch (Exception ex) {
                    logErrorMessageAndWait("Error while configuring solr. ", ex);
                }

                try {
                    if (solrCloudClient != null) {
                        solrCloudClient.close();
                    }
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "Error while closing the solr client. ", ex);
                }
            }
        } else {
            logger.severe("Solr ZKHosts for Audit are empty. Please set property " + SOLR_ZK_HOSTS);
        }
    }

    private boolean connect(List<String> zookeeperHosts) {
        try {
            logger.info("Solr is in Cloud mode");

            if (isKERBEROS) {
                setHttpClientBuilderForKrb();
            }

            solrCloudClient = new CloudSolrClient.Builder(zookeeperHosts, Optional.empty()).build();

            solrCloudClient.setDefaultCollection(solrCollectionName);
            solrCloudClient.connect();

            zkClient      = solrCloudClient.getZkStateReader().getZkClient();
            solrClient    = solrCloudClient;
            solrCloudMode = true;

            return true;
        } catch (Exception ex) {
            logger.severe("Can't connect to Solr server. ZooKeepers=" + zookeeperHosts + ", collection=" + solrCollectionName + ex);

            return false;
        }
    }

    private void setHttpClientBuilderForKrb() {
        try (Krb5HttpClientBuilder krbBuild = new Krb5HttpClientBuilder()) {
            SolrHttpClientBuilder kb = krbBuild.getBuilder();

            HttpClientUtil.setHttpClientBuilder(kb);
        }
    }

    private boolean uploadConfiguration() {
        try {
            if (zkClient != null) {
                ZkConfigManager zkConfigManager = new ZkConfigManager(zkClient);
                boolean         configExists    = zkConfigManager.configExists(solrConfigName);

                if (!configExists) {
                    try {
                        logger.info("Config does not exist with name " + solrConfigName);

                        String zipOfConfigs = null;

                        if (this.configSetFolder.exists() && this.configSetFolder.isFile()) {
                            zipOfConfigs = this.configSetFolder.getAbsolutePath();
                        } else {
                            String[] files = this.configSetFolder.list();

                            if (files != null) {
                                for (String aFile : files) {
                                    if (aFile != null) {
                                        if (aFile.equals("solr_audit_conf.zip")) {
                                            zipOfConfigs = this.configSetFolder + "/" + aFile;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (zipOfConfigs == null) {
                            throw new FileNotFoundException("Could Not Find Configs Zip File : " + getConfigSetFolder());
                        }

                        File       file             = new File(zipOfConfigs);
                        byte[]     arrByte          = Files.readAllBytes(file.toPath());
                        ByteBuffer byteBuffer       = ByteBuffer.wrap(arrByte);
                        String     baseUrl          = getBaseUrl();
                        String     protocol         = isSSLEnabled ? "https" : "http";
                        String     uploadConfigsUrl = String.format("%s://%s/admin/configs?action=UPLOAD&name=%s", protocol, baseUrl, solrConfigName);

                        postDataAndGetResponse(solrCloudClient, uploadConfigsUrl, byteBuffer);

                        return true;
                    } catch (Exception ex) {
                        logger.log(Level.SEVERE, "Error while uploading configs : ", ex);

                        return false;
                    }
                } else {
                    logger.info("Config already exists with name " + solrConfigName);

                    return true;
                }
            } else {
                logger.severe("Solr is in cloud mode and could not find the zookeeper client for performing upload operations. ");

                return false;
            }
        } catch (Exception ex) {
            logger.severe("Error while uploading configuration : " + ex);

            return false;
        }
    }

    private void logErrorMessageAndWait(String msg, Exception exception) {
        retryCounter++;

        String attempMessage;

        if (maxRetry != TRY_UNTIL_SUCCESS) {
            attempMessage = (retryCounter == maxRetry) ? ("Maximum attempts reached for setting up Solr.") : ("[retrying after " + timeInterval + " ms]. No. of attempts left : " + (maxRetry - retryCounter) + " . Maximum attempts : " + maxRetry);
        } else {
            attempMessage = "[retrying after " + timeInterval + " ms]";
        }

        StringBuilder errorBuilder = new StringBuilder(msg);

        if (exception != null) {
            errorBuilder.append("Error : ".concat(exception.getMessage() + ". "));
        }

        errorBuilder.append(attempMessage);

        logger.severe(errorBuilder.toString());

        try {
            Thread.sleep(timeInterval);
        } catch (InterruptedException ex) {
            logger.info("sleep interrupted: " + ex.getMessage());
        }
    }

    private boolean createCollection() {
        try {
            List<String> allCollectionList = getCollections();

            if (allCollectionList != null) {
                if (!allCollectionList.contains(solrCollectionName)) {
                    int shardsCalculation = solrCloudClient != null ? solrCloudClient.getClusterStateProvider().getLiveNodes().size() : DEFAULT_VALUE;
                    int noOfShards        = EmbeddedServerUtil.getIntConfig(SOLR_NO_SHARDS, shardsCalculation);

                    logger.info("No. of shards provided is : " + noOfShards);

                    CollectionAdminRequest.Create createCollection = CollectionAdminRequest.createCollection(solrCollectionName, solrConfigName, noOfShards, noOfReplicas);

                    createCollection.setMaxShardsPerNode(maxNodePerShards);

                    CollectionAdminResponse createResponse = createCollection.process(solrClient);

                    if (createResponse.getStatus() != 0) {
                        logger.severe("Error creating collection. collectionName=" + solrCollectionName + " , solr config name = " + solrConfigName + " , replicas = " + noOfReplicas + ", shards=" + noOfShards + " , max node per shards = " + maxNodePerShards + ", response=" + createResponse);

                        return false;
                    } else {
                        allCollectionList = getCollections();

                        if (allCollectionList != null) {
                            if (allCollectionList.contains(solrCollectionName)) {
                                logger.info("Created collection " + solrCollectionName + " with config name " + solrConfigName + " replicas =  " + noOfReplicas + " Shards = " + noOfShards + " max node per shards  = " + maxNodePerShards);

                                return true;
                            } else {
                                logger.severe("Collection does not exist. collectionName=" + solrCollectionName + " , solr config name = " + solrConfigName + " , replicas = " + noOfReplicas + ", shards=" + noOfShards + " , max node per shards = " + maxNodePerShards + ", response=" + createResponse);

                                return false;
                            }
                        } else {
                            logger.severe("Error while getting collection list after creating collection");

                            return false;
                        }
                    }
                } else {
                    logger.info("Collection already exists with name " + solrCollectionName);

                    return true;
                }
            } else {
                logger.severe("Error while connecting to solr ");

                return false;
            }
        } catch (Exception ex) {
            logger.severe("Error while creating collection in solr : " + ex);

            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getCollections() throws IOException {
        try {
            CollectionAdminRequest.List colListReq = new CollectionAdminRequest.List();
            CollectionAdminResponse     response   = colListReq.process(solrClient);

            if (response.getStatus() != 0) {
                logger.severe("Error getting collection list from solr.  response=" + response);

                return null;
            }

            return (List<String>) response.getResponse().get("collections");
        } catch (SolrException | SolrServerException e) {
            logger.severe("getCollections() operation failed : " + e);

            return null;
        }
    }

    private File getConfigSetFolder() {
        return configSetFolder;
    }

    private static List<String> getZkHosts() {
        List<String> zookeeperHosts = null;

        if (!StringUtil.isEmpty(EmbeddedServerUtil.getConfig(SOLR_ZK_HOSTS))) {
            String zkHosts = EmbeddedServerUtil.getConfig(SOLR_ZK_HOSTS).trim();

            zookeeperHosts = new ArrayList<>(Arrays.asList(zkHosts.split(",")));
        }

        return zookeeperHosts;
    }

    private String getBaseUrl() throws Exception {
        Set<String> nodes = solrCloudClient.getClusterStateProvider().getLiveNodes();

        if (CollectionUtils.isEmpty(nodes)) {
            throw new Exception("No live SolrServers available");
        }

        String[] nodeArr = nodes.toArray(new String[0]);

        // getting nodes URL as 'port_solr', so converting it to 'port/solr'
        return nodeArr[0].replaceAll("_", "/");
    }
}
