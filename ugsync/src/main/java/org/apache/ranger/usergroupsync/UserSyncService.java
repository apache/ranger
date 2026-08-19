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

package org.apache.ranger.usergroupsync;

import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.ha.UserSyncHAInitializerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM entry point for Ranger UserSync (user/group sync, HA, metrics).
 */
public class UserSyncService {
    private static final Logger LOG = LoggerFactory.getLogger(UserSyncService.class);

    private UserSyncHAInitializerImpl userSyncHAInitializerImpl;
    private Thread                  userGroupSyncThread;

    public static void main(String[] args) {
        UserSyncService service = new UserSyncService();
        service.userSyncHAInitializerImpl = UserSyncHAInitializerImpl.getInstance(UserGroupSyncConfig.getInstance().getUserGroupConfig());
        service.run();
    }

    public void run() {
        try {
            LOG.info("Starting User Sync Service!");
            startUserGroupSyncProcess();
            if (userGroupSyncThread != null) {
                userGroupSyncThread.join();
            }
        } catch (Throwable t) {
            LOG.error("ERROR: User Sync Service failed", t);
        } finally {
            LOG.info("User Sync Service - STOPPED");
            if (userSyncHAInitializerImpl != null) {
                LOG.info("Stopping curator leader latch service as main thread is closing");
                userSyncHAInitializerImpl.stop();
            }
        }
    }

    private void startUserGroupSyncProcess() {
        LOG.info("Start : startUserGroupSyncProcess");
        UserGroupSync syncProc          = new UserGroupSync();
        Thread        newSyncProcThread = new Thread(syncProc);
        newSyncProcThread.setName("UnixUserSyncThread");
        newSyncProcThread.setDaemon(false);
        newSyncProcThread.start();
        userGroupSyncThread = newSyncProcThread;
        LOG.info("UnixUserSyncThread started");
        LOG.info("creating UserSyncMetricsProducer thread with default metrics location : {}", System.getProperty("logdir"));

        boolean isUserSyncMetricsEnabled = UserGroupSyncConfig.getInstance().isUserSyncMetricsEnabled();
        if (isUserSyncMetricsEnabled) {
            UserSyncMetricsProducer userSyncMetricsProducer       = new UserSyncMetricsProducer();
            Thread                  userSyncMetricsProducerThread = new Thread(userSyncMetricsProducer);
            userSyncMetricsProducerThread.setName("UserSyncMetricsProducerThread");
            userSyncMetricsProducerThread.setDaemon(false);
            userSyncMetricsProducerThread.start();
            LOG.info("UserSyncMetricsProducer started");
        } else {
            LOG.info(" Ranger userSync metrics is not enabled");
        }
    }
}
