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

package org.apache.ranger.metrics.source;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ranger.metrics.RangerMetricsInfo;
import org.apache.ranger.server.tomcat.EmbeddedServer;
import org.apache.ranger.server.tomcat.EmbeddedServerMetricsCollector;
import org.apache.hadoop.metrics2.MetricsCollector;

import java.util.Objects;

public class RangerMetricsContainerSource extends RangerMetricsSource {

    private EmbeddedServerMetricsCollector embeddedServerMetricsCollector;

    private long maxConnections;

    private int acceptCount;

    private long activeConnectionsCount;

    private int maxContainersThreadCount;

    private int minSpareThreadsCount;

    private int activeContainerThreadsCount;

    private int totalContainerThreadsCount;

    private long connectionTimeout;

    private long keepAliveTimeout;

    private final String context;

    public RangerMetricsContainerSource(String context) {
        this.context = context;
        this.embeddedServerMetricsCollector = EmbeddedServer.getServerMetricsCollector();
    }

    @Override
    protected void refresh() {

        if(Objects.nonNull(this.embeddedServerMetricsCollector))
        {
            this.maxConnections = embeddedServerMetricsCollector.getMaxAllowedConnection();
            this.acceptCount = embeddedServerMetricsCollector.getConnectionAcceptCount();
            this.activeConnectionsCount = embeddedServerMetricsCollector.getActiveConnectionCount();
            this.maxContainersThreadCount = embeddedServerMetricsCollector.getMaxContainerThreadsCount();
            this.minSpareThreadsCount = embeddedServerMetricsCollector.getMinSpareContainerThreadsCount();
            this.activeContainerThreadsCount = embeddedServerMetricsCollector.getActiveContainerThreadsCount();
            this.connectionTimeout = embeddedServerMetricsCollector.getConnectionTimeout();
            this.keepAliveTimeout = embeddedServerMetricsCollector.getKeepAliveTimeout();
            this.totalContainerThreadsCount = embeddedServerMetricsCollector.getTotalContainerThreadsCount();
        }


    }

    @Override
    protected void update(MetricsCollector collector, boolean all) {

        collector.addRecord("RangerWebContainer")
                .setContext(this.context)
                .addCounter(new RangerMetricsInfo("MaxConnectionsCount", "Ranger max configured container connections"), this.maxConnections)
                .addCounter(new RangerMetricsInfo("ActiveConnectionsCount", "Ranger active container connections"), this.activeConnectionsCount)
                .addCounter(new RangerMetricsInfo("ConnectionAcceptCount", "Ranger accept connections count"), this.acceptCount)
                .addCounter(new RangerMetricsInfo("ConnectionTimeout", "Ranger connection timeout"), this.connectionTimeout)
                .addCounter(new RangerMetricsInfo("KeepAliveTimeout", "Ranger connection keepAlive timeout"), this.keepAliveTimeout)
                .addCounter(new RangerMetricsInfo("MaxWorkerThreadsCount", "Ranger container worker threads count"), this.maxContainersThreadCount)
                .addCounter(new RangerMetricsInfo("MinSpareWorkerThreadsCount", "Ranger container minimum spare worker threads count"), this.minSpareThreadsCount)
                .addCounter(new RangerMetricsInfo("ActiveWorkerThreadsCount", "Ranger container active worker threads count"), this.activeContainerThreadsCount)
                .addCounter(new RangerMetricsInfo("TotalWorkerThreadsCount", "Ranger container total worker threads count"), this.totalContainerThreadsCount);
    }

    @VisibleForTesting
    void setEmbeddedServerMetricsCollector( EmbeddedServerMetricsCollector embeddedServerMetricsCollector ){
        this.embeddedServerMetricsCollector = embeddedServerMetricsCollector;
    }

}
