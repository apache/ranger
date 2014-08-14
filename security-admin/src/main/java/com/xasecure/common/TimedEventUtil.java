package com.xasecure.common;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class TimedEventUtil{

	static final Logger logger = Logger.getLogger(TimedEventUtil.class);

	public static void runWithTimeout(final Runnable runnable, long timeout, TimeUnit timeUnit) throws Exception {
		timedTask(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				runnable.run();
				return null;
			}
		}, timeout, timeUnit);
	}

	public static <T> T timedTask(Callable<T> callableObj, long timeout, 
			TimeUnit timeUnit) throws Exception{
		
		return callableObj.call();
		
		/*
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		final Future<T> future = executor.submit(callableObj);
		executor.shutdownNow();

		try {
			return future.get(timeout, timeUnit);
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			if(logger.isDebugEnabled()){
				logger.debug("Error executing task", e);
			}
			Throwable t = e.getCause();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof Exception) {
				throw (Exception) e;
			} else {
				throw new IllegalStateException(t);
			}
		}
		*/
		
	}
	

}