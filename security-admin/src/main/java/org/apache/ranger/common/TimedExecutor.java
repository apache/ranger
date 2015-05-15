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

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Service
@Scope("singleton")
public class TimedExecutor {

	static final private Logger LOG = Logger.getLogger(TimedExecutor.class);

	@Autowired
	TimedExecutorConfigurator _configurator;
	
	ExecutorService _executorService;
	
	public TimedExecutor() {
	}
	
	@PostConstruct
	void initialize() {
		initialize(_configurator);
	}
		
	// Not designed for public access - only for testability
	void initialize(TimedExecutorConfigurator configurator) {
		final ThreadFactory _ThreadFactory = new ThreadFactoryBuilder()
										.setDaemon(true)
										.setNameFormat("timed-executor-pool-%d")
										.setUncaughtExceptionHandler(new LocalUncaughtExceptionHandler())
										.build();

		final BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(configurator.getBlockingQueueSize());

		_executorService = new LocalThreadPoolExecutor(configurator.getCoreThreadPoolSize(), configurator.getMaxThreadPoolSize(),
														configurator.getKeepAliveTime(), configurator.getKeepAliveTimeUnit(), 
														blockingQueue, _ThreadFactory);
	}
	
	public <T> T timedTask(Callable<T> callable, long time, TimeUnit unit) throws Exception{
		try {
		Future<T> future = _executorService.submit(callable);
			if (LOG.isDebugEnabled()) {
				if (future.isCancelled()) {
					LOG.debug("Got back a future that was cancelled already for callable[" + callable + "]!");
				}
			}
			try {
				T result = future.get(time, unit);
				return result;
			} catch (CancellationException | ExecutionException | InterruptedException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("TimedExecutor: Caught exception[%s] for callable[%s]: detail[%s].  Re-throwing...", e.getClass().getName(), callable, e.getMessage()));
				}
				throw e;
			} catch (TimeoutException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("TimedExecutor: Timed out waiting for callable[%s] to finish.  Cancelling the task.", callable));
				}
				boolean interruptRunningTask = true;
				future.cancel(interruptRunningTask);
				LOG.debug("TimedExecutor: Re-throwing timeout exception to caller");
				throw e;
			}
		} catch (RejectedExecutionException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Executor rejected callable[" + callable + "], due to resource exhaustion.  Rethrowing exception...");
			}
			throw e;
		}
	}
	
	/**
	 * Not designed for public access.  Non-private only for testability.  Expected to be called by tests to do proper cleanup.
	 */
	void shutdown() {
		_executorService.shutdownNow();
	}
	
	static class LocalUncaughtExceptionHandler implements UncaughtExceptionHandler {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			String message = String.format("TimedExecutor: Uncaught exception hanlder received exception[%s] in thread[%s]", t.getClass().getName(), t.getName());
			LOG.warn(message, e);
		}
	}
	
	static class LocalThreadPoolExecutor extends ThreadPoolExecutor {

		private ThreadLocal<Long> startNanoTime = new ThreadLocal<Long>();
		
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
				LOG.debug("TimedExecutor: Done execution of task. Duration[" + duration/1000000 + " ms].");
			}
		}
		
		@Override
		protected void terminated() {
			super.terminated();
			LOG.info("TimedExecutor: thread pool has terminated");
		}
	}
}