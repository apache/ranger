/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.db.upgrade;

import liquibase.command.CommandResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

@Component
@Scope("prototype")
class StatusCheckScheduledExecutorService {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCheckScheduledExecutorService.class);
    private static final int                      SLEEP_DURATION = 30;
    private final        ScheduledExecutorService scheduler      = Executors.newScheduledThreadPool(1);
    @Autowired
    ICommandDriver commandDriver;
    private              String changelogFilename;
    private              ScheduledFuture<?>       statusHandler;

    public void startStatusChecks(String serviceName, String changelogFilename) {
        LOG.info("StatusCheckScheduledExecutorService commandDriverClass={} \n", commandDriver.getClass());
        this.changelogFilename = changelogFilename;
        stopStatusChecks();
        final Runnable liquibaseStatusCommand = () -> {
            LOG.info("Executing status check command");
            try {
                CommandResults statusCheckResult = commandDriver.status(serviceName, changelogFilename);
                LOG.info("statusCheckResult={}", statusCheckResult.getResults());
            } catch (Exception e) {
                stopStatusChecks();
                throw new RuntimeException(e);
            }
        };
        LOG.info("Starting status check scheduler");
        statusHandler = scheduler.scheduleWithFixedDelay(liquibaseStatusCommand, 0, SLEEP_DURATION, SECONDS);
    }

    public void stopStatusChecks() {
        if (statusHandler == null) {
            LOG.info("statusHandler is null, stop not required for statusChecks");
            return;
        }
        LOG.info("Attempting to cancel statusChecks");
        try {
            scheduler.shutdown();
            statusHandler.cancel(true);
        } catch (Exception e) {
            LOG.info("Exception while trying to shutdown/cancel status check schedule");
        }
        if (statusHandler.isCancelled()) {
            LOG.info("Canceled the Status Check service for {}", changelogFilename);
        } else {
            try {
                LOG.info("Could not cancel the Status Check service for {}", changelogFilename);
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
