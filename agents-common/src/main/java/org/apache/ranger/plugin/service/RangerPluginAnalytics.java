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
package org.apache.ranger.plugin.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.DoubleAccumulator;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RangerPluginAnalytics {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPluginAnalytics.class);
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static final int SLEEP_DURATION = 300;
  private ScheduledFuture<?> statusHandler;

  private final DoubleAccumulator countIsAccessAllowed = new DoubleAccumulator(Double::sum, 0L);
  final Runnable asyncLoggerPluginAnalytics = () -> {
    LOG.info("===========Ranger Plugin Analytics=============");
    LOG.info("Accumulation period in seconds:"+SLEEP_DURATION);
    LOG.info("Number of times isAccessAllowed was called:"+countIsAccessAllowed.getThenReset());
  };

  public void startPluginAnalytics() {
    stopPluginAnalytics();
    LOG.info("Starting Scheduled Plugin Analytics");
    statusHandler = scheduler.scheduleWithFixedDelay(asyncLoggerPluginAnalytics, 0, SLEEP_DURATION, SECONDS);
  }

  public void stopPluginAnalytics() {
    if (statusHandler == null){
      LOG.info("statusHandler is null, stop not required for asyncLoggerPluginAnalytics");
      return;
    }
    LOG.info("Attempting to cancel asyncLoggerPluginAnalytics");
    scheduler.shutdown();
    statusHandler.cancel(true);
    if (statusHandler.isDone()){
      LOG.info("Canceled asyncLoggerPluginAnalytics");
    }
    else{
      LOG.info("Could not cancel asyncLoggerPluginAnalytics");
    }
  }

  public void addIsAccessAllowedCount(){
    countIsAccessAllowed.accumulate(1);
  }
}