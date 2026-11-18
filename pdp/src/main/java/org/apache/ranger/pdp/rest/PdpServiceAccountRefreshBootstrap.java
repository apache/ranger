/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.pdp.rest;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Starts {@link RangerPdpREST} service-account refresh from the JAX-RS / HK2 runtime
 * (same lifecycle as delegated authorization checks), mirroring audit ingestor's bootstrap.
 */
@Singleton
public class PdpServiceAccountRefreshBootstrap {
    private final RangerPdpREST rangerPdpREST;

    @Inject
    public PdpServiceAccountRefreshBootstrap(RangerPdpREST rangerPdpREST) {
        this.rangerPdpREST = rangerPdpREST;
    }

    @PostConstruct
    public void start() {
        rangerPdpREST.startServiceAccountUsersRefresh();
    }

    @PreDestroy
    public void stop() {
        rangerPdpREST.stopServiceAccountUsersRefresh();
    }
}
