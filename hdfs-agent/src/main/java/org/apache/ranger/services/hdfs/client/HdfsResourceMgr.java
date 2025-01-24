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

package org.apache.ranger.services.hdfs.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class HdfsResourceMgr {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsResourceMgr.class);

    public static final String PATH = "path";

    private HdfsResourceMgr() {
        // to block instantiation
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        LOG.debug("<== HdfsResourceMgr.connectionTest ServiceName: {}Configs{}", serviceName, configs);

        Map<String, Object> ret;

        try {
            ret = HdfsClient.connectionTest(serviceName, configs);
        } catch (HadoopException e) {
            LOG.error("<== HdfsResourceMgr.testConnection Error: {}", e.getMessage(), e);

            throw e;
        }

        LOG.debug("<== HdfsResourceMgr.connectionTest Result : {}", ret);

        return ret;
    }

    public static List<String> getHdfsResources(String serviceName, String serviceType, Map<String, String> configs, ResourceLookupContext context) throws Exception {
        List<String>              resultList  = null;
        String                    userInput   = context.getUserInput();
        String                    resource    = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        final List<String>        pathList    = new ArrayList<>();

        if (resource != null && resourceMap != null && resourceMap.get(PATH) != null) {
            pathList.addAll(resourceMap.get(PATH));
        }

        if (serviceName != null && userInput != null) {
            try {
                LOG.debug("<== HdfsResourceMgr.getHdfsResources() UserInput: {}configs: {}context: {}", userInput, configs, context);

                String           wildCardToMatch;
                final HdfsClient hdfsClient = new HdfsConnectionMgr().getHadoopConnection(serviceName, serviceType, configs);

                if (hdfsClient != null) {
                    int lastIndex = userInput.lastIndexOf("/");

                    if (lastIndex < 0) {
                        wildCardToMatch = userInput + "*";
                        userInput       = "/";
                    } else if (lastIndex == 0 && userInput.length() == 1) {
                        wildCardToMatch = null;
                        userInput       = "/";
                    } else if ((lastIndex + 1) == userInput.length()) {
                        wildCardToMatch = null;
                        userInput       = userInput.substring(0, lastIndex + 1);
                    } else {
                        wildCardToMatch = userInput.substring(lastIndex + 1) + "*";
                        userInput       = userInput.substring(0, lastIndex + 1);
                    }

                    final String                 finalBaseDir         = userInput;
                    final String                 finalWildCardToMatch = wildCardToMatch;
                    final Callable<List<String>> callableObj          = () -> hdfsClient.listFiles(finalBaseDir, finalWildCardToMatch, pathList);

                    synchronized (hdfsClient) {
                        resultList = TimedEventUtil.timedTask(callableObj, 5, TimeUnit.SECONDS);
                    }

                    LOG.debug("Resource dir : {} wild card to match : {}\n Matching resources : {}", userInput, wildCardToMatch, resultList);
                }
            } catch (HadoopException e) {
                LOG.error("Unable to get hdfs resources.", e);

                throw e;
            }
        }

        LOG.debug("<== HdfsResourceMgr.getHdfsResources() Result : {}", resultList);

        return resultList;
    }
}
