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

import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.AbstractProtocol;

import java.util.concurrent.Executor;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;

public class EmbeddedServerMetricsCollector {

    private final Connector connector;
    private final AbstractProtocol protocolHandler;

    EmbeddedServerMetricsCollector( Tomcat server){
        this.connector = server.getConnector();
        this.protocolHandler = (AbstractProtocol) this.connector.getProtocolHandler();
    }

    /**
     *
     * @return: maxConfigured (allowed) connections to be accepted by the server.
     */
    public long getMaxAllowedConnection(){

        return this.protocolHandler.getMaxConnections();
    }

    /**
     *
     * @return: Once maxConnection is reached, OS would still accept few more connections in a queue and size of queue is determined by "acceptCount"
     * By default, it is 100.
     * Note: These connections will wait in the queue for serverSocket to accept.
     */
    public int getConnectionAcceptCount(){
        return this.protocolHandler.getAcceptCount();
    }

    /**
     *
     * @return: Returns the active connections count.
     */
    public long getActiveConnectionCount(){
        return this.protocolHandler.getConnectionCount();
    }

    /**
     *
     * @return: Max container threads count
     */
    public int getMaxContainerThreadsCount(){
        return this.protocolHandler.getMaxThreads();
    }

    /**
     *
     * @return: Returns the corePoolSize of threadpool
     */
    public int getMinSpareContainerThreadsCount(){
        return this.protocolHandler.getMinSpareThreads();
    }

    /**
     *
     * @return: Returns the current active worked threads count.
     * Note: {@link ThreadPoolExecutor#getActiveCount()} internally acquires lock, so it could be expensive.
     */
    public int getActiveContainerThreadsCount(){
        Executor executor = this.protocolHandler.getExecutor();

        int activeThreadCount = -1;

        if( executor instanceof ThreadPoolExecutor){

            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
            activeThreadCount = threadPoolExecutor.getActiveCount();
        }

        return activeThreadCount;
    }

    public int getTotalContainerThreadsCount(){
        Executor executor = this.protocolHandler.getExecutor();

        int totalThreadCount = -1;

        if( executor instanceof ThreadPoolExecutor){

            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
            totalThreadCount = threadPoolExecutor.getPoolSize();
        }

        return totalThreadCount;
    }


    public String getProtocolHandlerName(){
        return this.protocolHandler.getName();
    }
    public long getConnectionTimeout(){

        return this.protocolHandler.getConnectionTimeout();
    }

    public long getKeepAliveTimeout(){
        return this.protocolHandler.getKeepAliveTimeout();
    }

}
