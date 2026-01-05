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

package org.apache.ranger.unixusersync.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.RangerHAInitializer;
import org.apache.ranger.ha.ActiveInstanceElectorService;
import org.apache.ranger.ha.ActiveStateChangeHandler;
import org.apache.ranger.ha.ServiceState;
import org.apache.ranger.ha.service.HARangerService;
import org.apache.ranger.ha.service.ServiceManager;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UserSyncHAInitializerImpl extends RangerHAInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(UserSyncHAInitializerImpl.class);

    private static UserSyncHAInitializerImpl theInstance;

    ActiveInstanceElectorService activeInstanceElectorService;
    ActiveStateChangeHandler     activeStateChangeHandler;
    List<HARangerService>        haRangerService;
    ServiceManager               serviceManager;

    private UserSyncHAInitializerImpl(Configuration configuration) {
        LOG.debug("==> UserSyncHAInitializerImpl.UserSyncHAInitializerImpl()");

        try {
            LOG.info("Ranger UserSync server is HA enabled : {}", configuration.getBoolean(UserGroupSyncConfig.UGSYNC_SERVER_HA_ENABLED_PARAM, false));

            init(configuration);
        } catch (Exception e) {
            LOG.error("UserSyncHAInitializerImpl initialization failed {}", e.getMessage());
        }

        LOG.info("<== UserSyncHAInitializerImpl.UserSyncHAInitializerImpl()");
    }

    public static UserSyncHAInitializerImpl getInstance(Configuration configuration) {
        UserSyncHAInitializerImpl me = theInstance;

        if (me == null) {
            synchronized (UserSyncHAInitializerImpl.class) {
                me = theInstance;

                if (me == null) {
                    me          = new UserSyncHAInitializerImpl(configuration);
                    theInstance = me;
                }
            }
        }

        return me;
    }

    public void init(Configuration configuration) throws Exception {
        super.init(configuration);

        LOG.info("==> UserSyncHAInitializerImpl.init() initialization started ");

        Set<ActiveStateChangeHandler> activeStateChangeHandlerProviders = new HashSet<>();

        activeInstanceElectorService = new ActiveInstanceElectorService(activeStateChangeHandlerProviders, curatorFactory, activeInstanceState, serviceState, configuration);
        haRangerService              = new ArrayList<>();

        haRangerService.add(activeInstanceElectorService);

        serviceManager = new ServiceManager(haRangerService);

        LOG.info("<== UserSyncHAInitializerImpl.init() initialization completed.");
    }

    @Override
    public void stop() {
        LOG.debug("==> UserSyncHAInitializerImpl.stop()");

        if (serviceManager != null) {
            serviceManager.stop();
        }

        if (curatorFactory != null) {
            curatorFactory.close();
        }

        LOG.debug("<== UserSyncHAInitializerImpl.stop()");
    }

    public boolean isActive() {
        try {
            // To let the curator thread a chance to run and set the active state if needed
            Thread.sleep(0L);
        } catch (InterruptedException exception) {
            // Ignore
        }
        return serviceState.getState().equals(ServiceState.ServiceStateValue.ACTIVE);
    }
}
