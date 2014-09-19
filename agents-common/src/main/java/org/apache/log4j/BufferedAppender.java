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
package org.apache.log4j;


import org.apache.log4j.helpers.LogLog;

public abstract class BufferedAppender<T> extends AppenderSkeleton {
	private LogDestination<T> mLogDestination = null;
	private LogBuffer<T>      mLogBuffer      = null;

	protected void setBufferAndDestination(LogBuffer<T> buffer, LogDestination<T> destination) {
		close();

		mLogBuffer      = buffer;
		mLogDestination = destination;
	}

	protected boolean isLogable() {
		return mLogBuffer != null;
	}

	protected void addToBuffer(T log) {
		if(mLogBuffer != null) {
			mLogBuffer.add(log);
		}
	}

	protected void start() {
		LogLog.debug("==> BufferedAppender.start()");

		if(mLogBuffer == null) {
			LogLog.warn("BufferedAppender.start(): logBuffer is null");
		}

		if(mLogDestination == null) {
			LogLog.warn("BufferedAppender.start(): logDestination is null");
		}

		if(mLogBuffer != null && mLogDestination != null) {
			JVMShutdownHook jvmShutdownHook = new JVMShutdownHook(this);

		    Runtime.getRuntime().addShutdownHook(jvmShutdownHook);

			mLogBuffer.start(mLogDestination);
		}

		LogLog.debug("<== BufferedAppender.start()");
	}

	protected void stop() {
		LogLog.debug("==> BufferedAppender.stop()");

		LogBuffer<T> tmpBuff = mLogBuffer;

		mLogDestination = null;
		mLogBuffer      = null;

		if(tmpBuff != null) {
			tmpBuff.stop();
		}

		LogLog.debug("<== BufferedAppender.stop()");
	}

	@Override
	public void close() {
		LogLog.debug("==> BufferedAppender.close()");

		stop();

		LogLog.debug("<== BufferedAppender.close()");
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("BufferedAppender {");
		if(mLogDestination != null) {
			sb.append(mLogDestination.toString());
		}

		sb.append(" ");

		if(mLogBuffer != null) {
			sb.append(mLogBuffer.toString());
		}
		sb.append("}");

		return sb.toString();
	}

	private class JVMShutdownHook extends Thread {
		Appender mAppender = null;

		public JVMShutdownHook(Appender appender) {
			mAppender = appender;
		}

		public void run() {
			if(mAppender != null) {
				mAppender.close();
			}
	    }
	}
}
