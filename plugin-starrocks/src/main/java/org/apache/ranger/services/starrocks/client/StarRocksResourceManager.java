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
package org.apache.ranger.services.starrocks.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.CATALOG;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.COLUMN;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.DATABASE;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.FUNCTION;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.GLOBAL_FUNCTION;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.MATERIALIZED_VIEW;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.RESOURCE;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.RESOURCE_GROUP;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.STORAGE_VOLUME;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.TABLE;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.USER;
import static org.apache.ranger.services.starrocks.client.StarRocksResourceType.VIEW;

public class StarRocksResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksResourceManager.class);

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        Map<String, Object> ret;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> StarRocksResourceManager.connectionTest() ServiceName: " + serviceName + " Configs: " + configs);
        }

        try {
            ret = StarRocksClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== StarRocksResourceManager.connectionTest() Error: " + e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== StarRocksResourceManager.connectionTest() Result : " + ret);
        }

        return ret;
    }

    public static List<String> getStarRocksResources(String serviceName, String serviceType, Map<String, String> configs,
                                                     ResourceLookupContext context) throws Exception {

        String userInput = context.getUserInput();
        if (userInput == null || userInput.isEmpty()) {
            userInput = "*";
        }

        String resourceName = context.getResourceName().trim().toLowerCase();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== StarRocksResourceManager.getStarRocksResources() UserInput: \"" + userInput + "\" resourceName : " +
                    resourceName + " resourceMap: " + context.getResources());
        }

        List<String> resultList = null;
        try {
            final StarRocksClient starRocksClient =
                    new StarRocksConnectionManager().getStarRocksConnection(serviceName, serviceType, configs);

            Callable<List<String>> callableObj = null;
            if (starRocksClient != null) {
                switch (resourceName) {
                    case CATALOG: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        String catalogName = userInput;

                        callableObj = () -> starRocksClient.getCatalogList(catalogName, catalogList);
                    }
                    break;
                    case DATABASE: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        String dbName = userInput;

                        callableObj = () -> starRocksClient.getDatabaseList(dbName, catalogList, databaseList);
                    }
                    break;
                    case TABLE: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        List<String> tableList = resourceMap.get(TABLE);
                        String tableName = userInput;

                        callableObj = () -> starRocksClient.getTableList(tableName, catalogList, databaseList, tableList);
                    }
                    break;
                    case COLUMN: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        List<String> tableList = resourceMap.get(TABLE);
                        List<String> columnList = resourceMap.get(COLUMN);
                        String columnName = userInput;

                        callableObj = () -> starRocksClient.getColumnList(columnName, catalogList, databaseList, tableList,
                                columnList);
                    }
                    break;
                    case VIEW: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        List<String> viewList = resourceMap.get(VIEW);
                        String viewName = userInput;

                        callableObj = () -> starRocksClient.getViewList(viewName, catalogList, databaseList, viewList);
                    }
                    break;
                    case MATERIALIZED_VIEW: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        List<String> mvList = resourceMap.get(MATERIALIZED_VIEW);
                        String mvName = userInput;

                        callableObj = () -> starRocksClient.getMaterializedViewList(mvName, catalogList, databaseList, mvList);
                    }
                    break;
                    case FUNCTION: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> catalogList = resourceMap.get(CATALOG);
                        List<String> databaseList = resourceMap.get(DATABASE);
                        List<String> mvList = resourceMap.get(FUNCTION);
                        String functionName = userInput;

                        callableObj = () -> starRocksClient.getFunctionList(functionName, catalogList, databaseList, mvList);
                    }
                    break;
                    case GLOBAL_FUNCTION: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> globalFunctionList = resourceMap.get(GLOBAL_FUNCTION);
                        String functionName = userInput;
                        callableObj = () -> starRocksClient.getFunctionList(functionName, globalFunctionList);
                    }
                    break;
                    case RESOURCE: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> globalFunctionList = resourceMap.get(RESOURCE);
                        String functionName = userInput;
                        callableObj = () -> starRocksClient.getResourceList(functionName, globalFunctionList);
                    }
                    break;
                    case RESOURCE_GROUP: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> globalFunctionList = resourceMap.get(RESOURCE_GROUP);
                        String functionName = userInput;
                        callableObj = () -> starRocksClient.getResourceGroupList(functionName, globalFunctionList);
                    }
                    break;
                    case STORAGE_VOLUME: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> globalFunctionList = resourceMap.get(STORAGE_VOLUME);
                        String functionName = userInput;
                        callableObj = () -> starRocksClient.getStorageVolumeList(functionName, globalFunctionList);
                    }
                    break;
                    case USER: {
                        Map<String, List<String>> resourceMap = context.getResources();
                        List<String> globalFunctionList = resourceMap.get(USER);
                        String functionName = userInput;
                        callableObj = () -> starRocksClient.getUserList(functionName, globalFunctionList);
                    }
                    break;

                    default:
                        break;
                }

                if (callableObj != null) {
                    synchronized (starRocksClient) {
                        resultList = TimedEventUtil.timedTask(callableObj, 5, TimeUnit.SECONDS);
                    }
                } else {
                    LOG.error("Could not initiate a StarRocks timedTask");
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to get StarRocks resource", e);
            throw e;
        }

        return resultList;
    }
}
