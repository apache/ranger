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
package org.apache.ranger.services.seatunnel.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SeatunnelResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(SeatunnelResourceManager.class);

  private static final String WORKSPACE = "workspace";
  private static final String JOB = "job";
  private static final String DATASOURCE = "datasource";
  private static final String VIRTUAL_TABLE = "virtual_table";
  private static final String USER = "user";
  private static final SeatunnelConnectionManager SEATUNNEL_CONNECTION_MANAGER = new SeatunnelConnectionManager();


  public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
    Map<String, Object> ret;

    if (LOG.isDebugEnabled()) {
        LOG.debug("==> SeatunnelResourceManager.connectionTest() ServiceName: {} Configs: {}", serviceName, configs);
    }

    try {
      ret = SeatunnelClient.connectionTest(serviceName, configs);
    } catch (Exception e) {
        LOG.error("<== SeatunnelResourceManager.connectionTest() Error: {}", String.valueOf(e));
      throw e;
    }

    if (LOG.isDebugEnabled()) {
        LOG.debug("<== SeatunnelResourceManager.connectionTest() Result : {}", ret);
    }

    return ret;
  }

  public static List<String> getSeatunnelResources(String serviceName, String serviceType, Map<String, String> configs, ResourceLookupContext context) throws Exception {
    String userInput = context.getUserInput();
    String resourceType = context.getResourceName();
    Map<String, List<String>> resourceMap = context.getResources();
    List<String> resultList = null;

    if (LOG.isDebugEnabled()) {
      LOG.debug("<== SeatunnelResourceMgr.getSeatunnelResources() UserInput: \"{}\" resourceType : {} resourceMap: {}", userInput, resourceType, resourceMap);
    }

    if (userInput != null && resourceType != null && resourceMap != null && !resourceMap.isEmpty()) {
      List<String> workspaceList = resourceMap.get(WORKSPACE);
      List<String> datasourceList = resourceMap.get(DATASOURCE);
      List<String> jobList = resourceMap.get(JOB);
      List<String> userList = resourceMap.get(USER);
      List<String> virtualTableList = resourceMap.get(VIRTUAL_TABLE);

      String selectedWorkspace = (workspaceList != null && !workspaceList.isEmpty()) ? workspaceList.get(0) : null;

      SeatunnelClient seatunnelClient = SEATUNNEL_CONNECTION_MANAGER.getSeatunnelConnection(serviceName, serviceType, configs);
      if (seatunnelClient != null) {
        Callable<List<String>> callableObj = null;

        switch (resourceType.trim().toLowerCase()) {
          case WORKSPACE:
            callableObj = () -> seatunnelClient.getWorkspaceList(userInput, workspaceList);
            break;
          case DATASOURCE:
            callableObj = () -> seatunnelClient.getDatasourceList(selectedWorkspace, userInput, datasourceList);
            break;
          case JOB:
            callableObj = () -> seatunnelClient.getJobList(selectedWorkspace, userInput, jobList);
            break;
          case USER:
            callableObj = () -> seatunnelClient.getUserList(userInput, userList);
            break;
          case VIRTUAL_TABLE:
            callableObj = () -> seatunnelClient.getVirtualTableList(selectedWorkspace, userInput, virtualTableList);
            break;
          default:
            LOG.error("Invalid resourceType: {}", resourceType);
            break;
        }

        if (callableObj != null) {
          synchronized (seatunnelClient) {
            resultList = TimedEventUtil.timedTask(callableObj, 30, TimeUnit.SECONDS);
          }
        } else {
          LOG.error("Could not initiate a SeatunnelClient timedTask");
        }
      }
    }

    return resultList;
  }
}
