/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.rest;

import org.apache.ranger.audit.server.AuditServerConfig;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Starts service-account refresh from the Spring webapp class loader so {@link AuditREST}
 * static allow maps stay in sync with the JAX-RS beans that authorize ingest traffic.
 */
@Component
public class AuditServiceAccountRefreshBootstrap {
    @PostConstruct
    public void start() {
        AuditREST.startServiceAccountUsersRefresh(AuditServerConfig.getInstance());
    }

    @PreDestroy
    public void stop() {
        AuditREST.stopServiceAccountUsersRefresh();
    }
}
