package com.xasecure.audit.provider;

public interface DebugTracer {
	void debug(String msg);
	void debug(String msg, Throwable excp);
	void warn(String msg);
	void warn(String msg, Throwable excp);
	void error(String msg);
	void error(String msg, Throwable excp);
}
