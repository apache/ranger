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

package org.apache.ranger.service;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Service
public class RangerTransactionService {
    private static final String PROP_THREADPOOL_SIZE          = "ranger.admin.transaction.service.threadpool.size";
    private static final String PROP_SUMMARY_LOG_INTERVAL_SEC = "ranger.admin.transaction.service.summary.log.interval.sec";

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    private static final Logger LOG = LoggerFactory.getLogger(RangerTransactionService.class);

    private ScheduledExecutorService scheduler            = null;
    private AtomicLong               scheduledTaskCount   = new AtomicLong(0);
    private AtomicLong               executedTaskCount    = new AtomicLong(0);
    private AtomicLong               failedTaskCount      = new AtomicLong(0);
    private long                     summaryLogIntervalMs = 5 * 60 * 1000;
    private long                     nextLogSummaryTime   = System.currentTimeMillis() + summaryLogIntervalMs;

    @PostConstruct
    public void init() {
        RangerAdminConfig config = RangerAdminConfig.getInstance();

        int  numOfThreads          = config.getInt(PROP_THREADPOOL_SIZE, 1);
        long summaryLogIntervalSec = config.getInt(PROP_SUMMARY_LOG_INTERVAL_SEC, 5 * 60);

        scheduler            = Executors.newScheduledThreadPool(numOfThreads);
        summaryLogIntervalMs = summaryLogIntervalSec * 1000;
        nextLogSummaryTime   = System.currentTimeMillis() + summaryLogIntervalSec;

        LOG.info("{}={}", PROP_THREADPOOL_SIZE, numOfThreads);
        LOG.info("{}={}", PROP_SUMMARY_LOG_INTERVAL_SEC, summaryLogIntervalSec);
    }

    @PreDestroy
    public void destroy() {
        try {
            LOG.info("attempt to shutdown RangerTransactionService");
            scheduler.shutdown();
            scheduler.awaitTermination(5, TimeUnit.SECONDS);

            logSummary();
        }
        catch (InterruptedException e) {
            LOG.error("RangerTransactionService tasks interrupted");
        }
        finally {
            if (!scheduler.isTerminated()) {
                LOG.info("cancel non-finished RangerTransactionService tasks");
            }
            scheduler.shutdownNow();
            LOG.info("RangerTransactionService shutdown finished");
        }
    }

    public void scheduleToExecuteInOwnTransaction(final Runnable task, final long delayInMillis) {
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    if (task != null) {
                        try {
                            //Create new  transaction
                            TransactionTemplate txTemplate = new TransactionTemplate(txManager);
                            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                            txTemplate.execute(new TransactionCallback<Object>() {
                                public Object doInTransaction(TransactionStatus status) {
                                    task.run();
                                    return null;
                                }
                            });
                        } catch (Exception e) {
                            failedTaskCount.getAndIncrement();

                            LOG.error("Failed to commit TransactionService transaction", e);
                            LOG.error("Ignoring...");
                        } finally {
                            executedTaskCount.getAndIncrement();
                            logSummaryIfNeeded();
                        }
                    }
                }
            }, delayInMillis, MILLISECONDS);

            scheduledTaskCount.getAndIncrement();

            logSummaryIfNeeded();
        } catch (Exception e) {
            LOG.error("Failed to schedule TransactionService transaction:", e);
            LOG.error("Ignroing...");
        }
    }

    private void logSummaryIfNeeded() {
        long now = System.currentTimeMillis();

        if (summaryLogIntervalMs > 0 && now > nextLogSummaryTime) {
            synchronized (this) {
                if (now > nextLogSummaryTime) {
                    nextLogSummaryTime = now + summaryLogIntervalMs;

                    logSummary();
                }
            }
        }
    }

    private void logSummary() {
        long scheduled = scheduledTaskCount.get();
        long executed  = executedTaskCount.get();
        long failed    = failedTaskCount.get();
        long pending   = scheduled - executed;

        LOG.info("RangerTransactionService: tasks(scheduled={}, executed={}, failed={}, pending={})", scheduled, executed, failed, pending);
    }
}
