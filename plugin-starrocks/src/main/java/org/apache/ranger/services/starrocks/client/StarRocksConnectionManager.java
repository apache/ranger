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

import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class StarRocksConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksConnectionManager.class);

    protected ConcurrentMap<String, StarRocksClient> starrocksConnectionCache;
    protected ConcurrentMap<String, Boolean> repoConnectStatusMap;

    public StarRocksConnectionManager() {
        starrocksConnectionCache = new ConcurrentHashMap<>();
        repoConnectStatusMap = new ConcurrentHashMap<>();
    }

    public StarRocksClient getStarRocksConnection(final String serviceName, final String serviceType,
                                                  final Map<String, String> configs) {
        StarRocksClient starrocksClient = null;

        if (serviceType != null) {
            starrocksClient = starrocksConnectionCache.get(serviceName);
            if (starrocksClient == null) {
                if (configs != null) {
                    final Callable<StarRocksClient> connectStarRocks = new Callable<StarRocksClient>() {
                        @Override
                        public StarRocksClient call() throws Exception {
                            return new StarRocksClient(serviceName, configs);
                        }
                    };
                    try {
                        starrocksClient = TimedEventUtil.timedTask(connectStarRocks, 5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        LOG.error("Error connecting to StarRocks repository: " +
                                serviceName + " using config: " + configs, e);
                    }

                    StarRocksClient oldClient = null;
                    if (starrocksClient != null) {
                        oldClient = starrocksConnectionCache.putIfAbsent(serviceName, starrocksClient);
                    } else {
                        oldClient = starrocksConnectionCache.get(serviceName);
                    }

                    if (oldClient != null) {
                        if (starrocksClient != null) {
                            starrocksClient.close();
                        }
                        starrocksClient = oldClient;
                    }
                    repoConnectStatusMap.put(serviceName, true);
                } else {
                    LOG.error("Connection Config not defined for asset :"
                            + serviceName, new Throwable());
                }
            } else {
                try {
                    starrocksClient.getCatalogList("*", null);
                } catch (Exception e) {
                    starrocksConnectionCache.remove(serviceName);
                    starrocksClient.close();
                    starrocksClient = getStarRocksConnection(serviceName, serviceType, configs);
                }
            }
        } else {
            LOG.error("Asset not found with name " + serviceName, new Throwable());
        }
        return starrocksClient;
    }
}
