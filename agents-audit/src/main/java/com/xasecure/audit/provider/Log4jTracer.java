package com.xasecure.audit.provider;

import org.apache.commons.logging.Log;

public class Log4jTracer implements DebugTracer {
	private Log mLogger = null;

	public Log4jTracer(Log logger) {
		mLogger = logger;
	}

	public void debug(String msg) {
		mLogger.debug(msg);
	}

	public void debug(String msg, Throwable excp) {
		mLogger.debug(msg, excp);
	}

	public void warn(String msg) {
		mLogger.warn(msg);
	}

	public void warn(String msg, Throwable excp) {
		mLogger.warn(msg, excp);
	}

	public void error(String msg) {
		mLogger.error(msg);
	}

	public void error(String msg, Throwable excp) {
		mLogger.error(msg, excp);
	}
}
