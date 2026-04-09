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

package org.apache.ranger.audit.dispatcher.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
* Tracks active audit dispatcher instances.
*/
public class AuditDispatcherTracker {
    private static final Logger                 LOG               = LoggerFactory.getLogger(AuditDispatcherTracker.class);
    private static final AuditDispatcherTracker tracker           = new AuditDispatcherTracker();
    private final Map<String, AuditDispatcher>  activeDispatchers = new ConcurrentHashMap<>();

    private AuditDispatcherTracker() {
        LOG.debug("AuditDispatcherTracker instance created by classloader: {}", this.getClass().getClassLoader());
    }

    public static AuditDispatcherTracker getInstance() {
        return tracker;
    }

    /**
     * Register an active dispatcher instance for a specific destination type for healthcheck.
     *
     * @param destinationType The destination type identifier (e.g., "solr", "hdfs", "elasticsearch")
     * @param dispatcher The dispatcher instance
     */
    public void addActiveDispatcher(String destinationType, AuditDispatcher dispatcher) {
        if (dispatcher == null) {
            LOG.warn("Attempted to register null dispatcher for destination type: {}", destinationType);
            return;
        }
        activeDispatchers.put(destinationType, dispatcher);
        LOG.info("Registered active dispatcher for destination type: {}", destinationType);
    }

    public Collection<AuditDispatcher> getActiveDispatchers() {
        return activeDispatchers.values();
    }

    /**
      * Clear all active dispatcher references.
      * Called during shutdown after dispatchers have been stopped.
     */

    public void clearActiveDispatchers() {
        LOG.debug("Clearing {} active dispatcher references", activeDispatchers.size());
        activeDispatchers.clear();
    }
}
