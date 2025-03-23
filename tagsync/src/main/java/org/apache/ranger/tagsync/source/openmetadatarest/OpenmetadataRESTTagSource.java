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

package org.apache.ranger.tagsync.source.openmetadatarest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

// import OpenMetadata packages
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;

import org.openmetadata.client.model.Table;
import org.openmetadata.client.model.TagLabel;
import org.openmetadata.client.model.Column;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.api.TablesApi.ListTablesQueryParams;

import org.openmetadata.client.gateway.OpenMetadata;

// import ranger plugin and hadoop modules
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import common java modules

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class OpenmetadataRESTTagSource extends AbstractTagSource implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(OpenmetadataRESTTagSource.class);
    private static final ThreadLocal<DateFormat> DATE_FORMATTER = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss.ssz");
            return dateFormat;
        }
    };

    private long sleepTimeBetweenCycleInMillis;
    private String[] restUrls         = null;
    private boolean  isKerberized     = false;
    private int      entitiesBatchSize = TagSyncConfig.DEFAULT_TAGSYNC_OPENMETADATAREST_SOURCE_ENTITIES_BATCH_SIZE;

    private Thread myThread = null;

    public static void main(String[] args) {

        OpenmetadataRESTTagSource openmetadataRESTTagSource = new OpenmetadataRESTTagSource();

        TagSyncConfig config = TagSyncConfig.getInstance();

        Properties props = config.getProperties();

        TagSynchronizer.printConfigurationProperties(props);

        boolean ret = TagSynchronizer.initializeKerberosIdentity(props);

        if (ret) {

            TagSink tagSink = TagSynchronizer.initializeTagSink(props);

            if (tagSink != null) {

                if (openmetadataRESTTagSource.initialize(props)) {
                    try {
                        tagSink.start();
                        openmetadataRESTTagSource.setTagSink(tagSink);
                        OpenMetadata openMetadataGateway = getOpenMetadataClient(props);
                        LOG.info("==> Openmetadata client initialized. Starting the Sync");
                        openmetadataRESTTagSource.synchUp(openMetadataGateway);
                    } catch (Exception exception) {
                        LOG.error("ServiceTags upload failed : ", exception);
                        System.exit(1);
                    }
                } else {
                    LOG.error("OpenmetadataRESTTagSource initialization failed, exiting.");
                    System.exit(1);
                }

            } else {
                LOG.error("TagSink initialization failed, exiting.");
                System.exit(1);
            }
        } else {
            LOG.error("Error initializing kerberos identity");
            System.exit(1);
        }

    }
    @Override
    public boolean initialize(Properties properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OpenmetadataRESTTagSource.initialize()");
        }

        boolean ret = OpenmetadataResourceMapperUtil.initializeOpenmetadataResourceMappers(properties);

        sleepTimeBetweenCycleInMillis = TagSyncConfig.getOpenmetadataRESTTagSourceDownloadIntervalInMillis(properties);
        isKerberized = TagSyncConfig.getTagsyncKerberosIdentity(properties) != null;
        entitiesBatchSize = TagSyncConfig.getOpenmetadataRestTagSourceEntitiesBatchSize(properties);

        String restEndpoint       = TagSyncConfig.getOpenmetadataRESTEndpoint(properties);
        String sslConfigFile = TagSyncConfig.getOpenmetadataRESTSslConfigFile(properties);

        if (LOG.isDebugEnabled()) {
            LOG.debug("restUrl=" + restEndpoint);
            LOG.debug("sslConfigFile=" + sslConfigFile);
            LOG.debug("kerberized=" + isKerberized);
        }
        if (StringUtils.isNotEmpty(restEndpoint)) {
            this.restUrls = restEndpoint.split(",");

            for (int i = 0; i < restUrls.length; i++) {
                if (!restUrls[i].endsWith("/")) {
                    restUrls[i] += "/";
                }
            }
        } else {
            LOG.info("OpenmetadataEndpoint not specified, Initial download of Atlas-entities cannot be done.");
            ret = false;
        }


        LOG.debug("<== OpenmetadataRESTTagSource.initialize(), result=" + ret);

        return ret;
    }

    @Override
    public boolean start() {

        myThread = new Thread(this);
        myThread.setDaemon(true);
        myThread.start();

        return true;
    }

    @Override
    public void stop() {
        if (myThread != null && myThread.isAlive()) {
            myThread.interrupt();
        }
    }

    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OpenmetadataRESTTagSource.run()");
        }
        while (true) {
            try {
                if (TagSyncConfig.isTagSyncServiceActive()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("==> OpenmetadataRESTTagSource.run() is running as server is Active");
                    }
                    TagSyncConfig config = TagSyncConfig.getInstance();
                    Properties props = config.getProperties();
                    OpenMetadata openMetadataGateway = getOpenMetadataClient(props);
                    synchUp(openMetadataGateway);
                }else{
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("==> This server is running in passive mode");
                    }
                }
                LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");

                Thread.sleep(sleepTimeBetweenCycleInMillis);

            } catch (InterruptedException exception) {
                LOG.error("Interrupted..: ", exception);
                return;
            } catch (Exception e) {
                LOG.error("Caught exception", e);
                return;
            }
        }
    }
    @SuppressWarnings("null")
    public static OpenMetadata getOpenMetadataClient(Properties properties) {
        try {
            String token = TagSyncConfig.getOpenmetadataRESTToken(properties);
            String restEndpoint = TagSyncConfig.getOpenmetadataRESTEndpoint(properties);
            OpenMetadataConnection openmetadataClient = new OpenMetadataConnection();
            OpenMetadataJWTClientConfig openMetadataJWTClientConfig = new OpenMetadataJWTClientConfig();
            openMetadataJWTClientConfig.setJwtToken(token);
            openmetadataClient.setHostPort(restEndpoint);
            openmetadataClient.setApiVersion("v1");
            openmetadataClient.setAuthProvider(AuthProvider.fromValue("openmetadata"));
            openmetadataClient.setSecurityConfig(openMetadataJWTClientConfig);
            OpenMetadata openMetadataGateway = new OpenMetadata(openmetadataClient);
            return openMetadataGateway;
        }
        catch (Exception exception) {
            LOG.error("Failed to get Openmetadata client", exception);
            return null;
        }
    }
    public void synchUp(OpenMetadata openMetadataGateway) throws Exception {
        List<RangerOpenmetadataEntityWithTags> rangerOpenmetadataEntities = getOpenmetadataActiveEntities(openMetadataGateway);

        if (CollectionUtils.isNotEmpty(rangerOpenmetadataEntities)) {
            if (LOG.isDebugEnabled()) {
                for (RangerOpenmetadataEntityWithTags element : rangerOpenmetadataEntities) {
                    LOG.debug(Objects.toString(element));
                }
            }
            Map<String, ServiceTags> serviceTagsMap = OpenmetadataMappingHelper.processOpenmetadataEntities(rangerOpenmetadataEntities);

            if (MapUtils.isNotEmpty(serviceTagsMap)) {
                for (Map.Entry<String, ServiceTags> entry : serviceTagsMap.entrySet()) {
                    if (LOG.isDebugEnabled()) {
                        try {
                            String serviceTagsString = JsonUtils.objectToJson(entry.getValue());
                            LOG.debug("serviceTags=" + serviceTagsString);
                        }catch (Exception e) {
                            LOG.error("An error occurred while conveting serviceTags to string", e);
                        }
                    }
                    updateSink(entry.getValue());
                }
            }
        }

    }

    @SuppressWarnings("null")
    public List<RangerOpenmetadataEntityWithTags> getOpenmetadataActiveEntities(OpenMetadata  openMetadataGateway){
        if (LOG.isDebugEnabled()){
            LOG.debug("==> getOpenmetadataActiveEntities()");
        }
        List<RangerOpenmetadataEntityWithTags> taggedEntities = new ArrayList<>();
        taggedEntities = getOpenmetadataTableEntities(openMetadataGateway);

        /*More entity methods such as Dashboards, Containers etc, can be added here.
         * Future enhancement: Add getOpenmetadataContainerEntities()
         * Future enhancement: Add getOpenmetadataMessagingEntities()
         */

        return taggedEntities;
    }

    @SuppressWarnings("null")
    public List<RangerOpenmetadataEntityWithTags> getOpenmetadataTableEntities(OpenMetadata openMetadataGateway){
        List<RangerOpenmetadataEntityWithTags> ret = new ArrayList<>();
        TablesApi tablesApiClient = openMetadataGateway.buildClient(TablesApi.class);
        ListTablesQueryParams queryParams = new ListTablesQueryParams();
        Integer numberOfTagEntities = 0;
        String pagination = null;

        do{
            List<Table> TableList = tablesApiClient.listTables(queryParams.fields("tags,columns").limit(100).after(pagination)).getData();
            LOG.debug("==> Array of Table Objects fetched from Openmetadata: ",TableList);
            // Get "after" token for pagination to loop through batches of api outputs
            pagination = tablesApiClient.listTables(queryParams.fields("tags,columns").limit(100)).getPaging().getAfter();
            // declare array list of type "TagLabel" to store Tags of Tables and columns respectively
            List<TagLabel> allTagsForColumnEntity = new ArrayList<>();
            List<TagLabel> allTagsForTableEntity = new ArrayList<>();
            // Increment counter to represent number of entities fetched per page
            numberOfTagEntities += 100;
            // Iterate over Array of Table objects extracted from Openmetadata source
            for(Table tableEntity: TableList){
                // declare type of table
                String TypeName = "table";
                // pass in the type, id and table object to get entity payload def without classifications
                RangerOpenmetadataEntity entity = new RangerOpenmetadataEntity(TypeName, tableEntity.getId(), tableEntity, null);
                // check if the current table entity object is associated with any tags
                if (!tableEntity.getTags().isEmpty()) {
                    allTagsForTableEntity = tableEntity.getTags();
                    // pass in the table entity payload def from above and the array of tags AKA classifications to get the final payload entity def with tags
                    RangerOpenmetadataEntityWithTags entityWithTags = new RangerOpenmetadataEntityWithTags(entity, allTagsForTableEntity);
                    // add the above entity with tags payload def to an array list
                    ret.add(entityWithTags);
                }
                // iterate over column objects
                for (Column columnEntity: tableEntity.getColumns()){
                    /*
                    * Unlike Atlas, Openmetadata does not have seperate entity type for column
                    * So in order to make the logic seamless and similar to that of Atlas and Ranger
                    * A type for column is assumed through the following lines and will be used by
                      subsequent classes of ranger entity, entitieswithtags and table mapper methods
                    * */
                    // declare type of column
                    TypeName = "column";
                    // pass in the type, and column object to get entity payload def without classifications
                    //Note: Open metadata does not have column type like that of Atlas and hence it does not have ID associated with it.
                    entity = new RangerOpenmetadataEntity(TypeName, tableEntity.getId(),tableEntity, columnEntity);
                    // check if current column attribute object of table is associated with any tags
                    if (!columnEntity.getTags().isEmpty()) {
                        allTagsForColumnEntity = columnEntity.getTags();
                        // pass in the column entity payload def from above and the array of tags AKA classifications to get the final payload entity def with tags
                        RangerOpenmetadataEntityWithTags entityWithTags = new RangerOpenmetadataEntityWithTags(entity, allTagsForColumnEntity);
                        // add the above entity with tags payload def to an array list
                        ret.add(entityWithTags);
                    }
                }

            }
            LOG.info("==> Looping at : ");
            LOG.info(numberOfTagEntities.toString());

        } while (pagination != null);
        return ret;
    }
}
