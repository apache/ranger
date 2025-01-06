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

package org.apache.ranger.common;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.concurrent.TimeUnit;

@Service
@Scope("singleton")
public class TimedExecutorConfigurator {
    // these two are important and hence are user configurable.
    static final         String PROPERTY_MAX_THREAD_POOL_SIZE = "ranger.timed.executor.max.threadpool.size";
    static final         String PROPERTY_QUEUE_SIZE           = "ranger.timed.executor.queue.size";
    // We need these default-defaults since default-site.xml file isn't inside the jar, i.e. file itself may be missing or values in it might be messed up! :(
    static final         int    DEFAULT_MAX_THREAD_POOL_SIZE  = 10;
    private static final int    DEFAULT_BLOCKING_QUEUE_SIZE   = 100;

    private       int      maxThreadPoolSize;
    private       int      blockingQueueSize;
    private final TimeUnit keepAliveTimeUnit = TimeUnit.SECONDS;

    public TimedExecutorConfigurator() {
    }

    /**
     * Provided mostly only testability.
     *
     * @param maxThreadPoolSize
     * @param blockingQueueSize
     */
    public TimedExecutorConfigurator(int maxThreadPoolSize, int blockingQueueSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
        this.blockingQueueSize = blockingQueueSize;
    }

    public int getCoreThreadPoolSize() {
        // The following is hard-coded for now and can be exposed if there is a pressing need.
        return 1;
    }

    public int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public long getKeepAliveTime() {
        // The following is hard-coded for now and can be exposed if there is a pressing need.
        return 10;
    }

    public TimeUnit getKeepAliveTimeUnit() {
        return keepAliveTimeUnit;
    }

    public int getBlockingQueueSize() {
        return blockingQueueSize;
    }

    // Infrequently used class (once per lifetime of policy manager) hence, values read from property file aren't cached.
    @PostConstruct
    void initialize() {
        Integer value = PropertiesUtil.getIntProperty(PROPERTY_MAX_THREAD_POOL_SIZE);

        if (value == null) {
            maxThreadPoolSize = DEFAULT_MAX_THREAD_POOL_SIZE;
        } else {
            maxThreadPoolSize = value;
        }

        value = PropertiesUtil.getIntProperty(PROPERTY_QUEUE_SIZE);

        if (value == null) {
            blockingQueueSize = DEFAULT_BLOCKING_QUEUE_SIZE;
        } else {
            blockingQueueSize = value;
        }
    }
}
