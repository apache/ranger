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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeatunnelConnectionManager {
  private static final Logger LOG = LoggerFactory.getLogger(SeatunnelConnectionManager.class);

  protected ConcurrentMap<String, SeatunnelClient> seatunnelConnectionCache;

  public SeatunnelConnectionManager() {
    seatunnelConnectionCache = new ConcurrentHashMap<>();
  }

  public SeatunnelClient getSeatunnelConnection(final String serviceName, final String serviceType, final Map<String, String> configs) throws IOException, InterruptedException {
    if (serviceType == null) {
      LOG.error("Asset not found with name {}", serviceName, new Throwable());
      return null;
    }

    SeatunnelClient seatunnelClient = seatunnelConnectionCache.get(serviceName);
    if (seatunnelClient == null && configs != null) {
      try {
        seatunnelClient = TimedEventUtil.timedTask(() -> new SeatunnelClient(configs), 5, TimeUnit.SECONDS);
        SeatunnelClient oldClient = seatunnelConnectionCache.putIfAbsent(serviceName, seatunnelClient);
        if (oldClient != null) {
          seatunnelClient.close();
          seatunnelClient = oldClient;
        }
      } catch (Exception e) {
        LOG.error("Error connecting to Seatunnel repository: {} using config: {}", serviceName, configs, e);
      }
    } else if (seatunnelClient != null) {
      try {
        seatunnelClient.getWorkspaceList(null, null);
      } catch (Exception e) {
        seatunnelConnectionCache.remove(serviceName);
        seatunnelClient.close();
        throw e;
      }
    } else {
      LOG.error("Connection Config not defined for asset :{}", serviceName, new Throwable());
    }

    return seatunnelClient;
  }
}
