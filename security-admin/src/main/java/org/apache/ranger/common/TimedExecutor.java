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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.RangerDefaultService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Scope("singleton")
public class TimedExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TimedExecutor.class);

    @Autowired
    TimedExecutorConfigurator configurator;

    ExecutorService executorService;

    public TimedExecutor() {
    }

    public <T> T timedTask(Callable<T> callable, long time, TimeUnit unit) throws Exception {
        try {
            Future<T> future = executorService.submit(callable);

            if (LOG.isDebugEnabled()) {
                if (future.isCancelled()) {
                    LOG.debug("Got back a future that was cancelled already for callable[{}]!", callable);
                }
            }

            try {
                return future.get(time, unit);
            } catch (CancellationException | ExecutionException | InterruptedException e) {
                LOG.debug("TimedExecutor: Caught exception[{}] for callable[{}]: detail[{}].  Re-throwing...", e.getClass().getName(), callable, e.getMessage());

                if (StringUtils.contains(e.getMessage(), RangerDefaultService.ERROR_MSG_VALIDATE_CONFIG_NOT_IMPLEMENTED)) {
                    throw e;
                } else {
                    throw generateHadoopException(e);
                }
            } catch (TimeoutException e) {
                LOG.debug("TimedExecutor: Timed out waiting for callable[{}] to finish.  Cancelling the task.", callable);

                boolean interruptRunningTask = true;

                future.cancel(interruptRunningTask);

                throw e;
            }
        } catch (RejectedExecutionException e) {
            LOG.debug("Executor rejected callable[{}], due to resource exhaustion.  Rethrowing exception...", callable);

            throw e;
        }
    }

    @PostConstruct
    void initialize() {
        initialize(configurator);
    }

    // Not designed for public access - only for testability
    void initialize(TimedExecutorConfigurator configurator) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("timed-executor-pool-%d")
                .setUncaughtExceptionHandler(new LocalUncaughtExceptionHandler())
                .build();

        final BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(configurator.getBlockingQueueSize());

        executorService = new LocalThreadPoolExecutor(configurator.getCoreThreadPoolSize(), configurator.getMaxThreadPoolSize(),
                configurator.getKeepAliveTime(), configurator.getKeepAliveTimeUnit(),
                blockingQueue, threadFactory);
    }

    /**
     * Not designed for public access.  Non-private only for testability.  Expected to be called by tests to do proper cleanup.
     */
    void shutdown() {
        executorService.shutdownNow();
    }

    private HadoopException generateHadoopException(Exception e) {
        String msgDesc = "Unable to retrieve any files using given parameters, "
                + "You can still save the repository and start creating policies, "
                + "but you would not be able to use autocomplete for resource names. "
                + "Check ranger_admin.log for more info. ";
        HadoopException hpe = new HadoopException(e.getMessage(), e);
        hpe.generateResponseDataMap(false, hpe.getMessage(e), msgDesc, null, null);
        return hpe;
    }

    static class LocalUncaughtExceptionHandler implements UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOG.warn("TimedExecutor: Uncaught exception hanlder received exception[{}] in thread[{}]", t.getClass().getName(), t.getName(), e);
        }
    }

    static class LocalThreadPoolExecutor extends ThreadPoolExecutor {
        private final ThreadLocal<Long> startNanoTime = new ThreadLocal<>();

        public LocalThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("TimedExecutor: Starting execution of a task.");
                startNanoTime.set(System.nanoTime());
            }

            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);

            if (LOG.isDebugEnabled()) {
                long duration = System.nanoTime() - startNanoTime.get();

                LOG.debug("TimedExecutor: Done execution of task. Duration[{} ms].", duration / 1000000);
            }
        }

        @Override
        protected void terminated() {
            super.terminated();

            LOG.info("TimedExecutor: thread pool has terminated");
        }
    }
}
