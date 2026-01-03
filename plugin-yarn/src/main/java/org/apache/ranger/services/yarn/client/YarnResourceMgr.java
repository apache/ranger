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

package org.apache.ranger.services.yarn.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class YarnResourceMgr {
    private static final Logger LOG       = LoggerFactory.getLogger(YarnResourceMgr.class);
    private static final String YARNQUEUE = "queue";

    private YarnResourceMgr(){
    }

    public static Map<String, Object> validateConfig(String serviceName, Map<String, String> configs) {
        Map<String, Object> ret;

        LOG.debug("==> YarnResourceMgr.validateConfig ServiceName: {}Configs{}", serviceName, configs);

        try {
            ret = YarnClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== YarnResourceMgr.validateConfig Error: {}", String.valueOf(e));
            throw e;
        }

        LOG.debug("<== YarnResourceMgr.validateConfig Result : {}", ret);
        return ret;
    }

    public static List<String> getYarnResources(String serviceName, Map<String, String> configs, ResourceLookupContext context) {
        String                    userInput     = context.getUserInput();
        Map<String, List<String>> resourceMap   = context.getResources();
        List<String>              resultList    = null;
        List<String>              yarnQueueList = null;
        String                    yarnQueueName;

        if (resourceMap != null && !resourceMap.isEmpty() && resourceMap.get(YARNQUEUE) != null) {
            yarnQueueName = userInput;
            yarnQueueList = resourceMap.get(YARNQUEUE);
        } else {
            yarnQueueName = userInput;
        }

        if (configs == null || configs.isEmpty()) {
            LOG.error("Connection Config is empty");
        } else {
            resultList = getYarnResource(serviceName, configs, yarnQueueName, yarnQueueList);
        }
        return resultList;
    }

    public static List<String> getYarnResource(String serviceName, Map<String, String> configs, String yarnQueueName, List<String> yarnQueueList) {
        final YarnClient yarnClient   = YarnConnectionMgr.getYarnClient(serviceName, configs);
        List<String>     topologyList = null;
        if (yarnClient != null) {
            synchronized (yarnClient) {
                topologyList = yarnClient.getQueueList(yarnQueueName, yarnQueueList);
            }
        }
        return topologyList;
    }
}
