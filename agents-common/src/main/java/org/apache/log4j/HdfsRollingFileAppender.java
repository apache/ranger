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
import org.apache.log4j.spi.LoggingEvent;


public class HdfsRollingFileAppender extends BufferedAppender<String> {
	String mEncoding                               = null;

	String mHdfsDestinationDirectory               = null;
	String mHdfsDestinationFile                    = null;
	int    mHdfsDestinationRolloverIntervalSeconds = 24 * 60 * 60;

	String mLocalFileBufferDirectory               = null;
	String mLocalFileBufferFile                    = null;
	int    mLocalFileBufferRolloverIntervalSeconds = 10 * 60;
	String mLocalFileBufferArchiveDirectory        = null;
	int    mLocalFileBufferArchiveFileCount        = 10;

	public void setEncoding(String encoding) {
		mEncoding = encoding;
	}

	public void setHdfsDestinationDirectory(String hdfsDestinationDirectory) {
		mHdfsDestinationDirectory = hdfsDestinationDirectory;
	}

	public void setHdfsDestinationFile(String hdfsDestinationFile) {
		mHdfsDestinationFile = hdfsDestinationFile;
	}

	public void setHdfsDestinationRolloverIntervalSeconds(int hdfsDestinationRolloverIntervalSeconds) {
		mHdfsDestinationRolloverIntervalSeconds = hdfsDestinationRolloverIntervalSeconds;
	}

	public void setLocalFileBufferDirectory(String localFileBufferDirectory) {
		mLocalFileBufferDirectory = localFileBufferDirectory;
	}

	public void setLocalFileBufferFile(String localFileBufferFile) {
		mLocalFileBufferFile = localFileBufferFile;
	}

	public void setLocalFileBufferRolloverIntervalSeconds(int localFileBufferRolloverIntervalSeconds) {
		mLocalFileBufferRolloverIntervalSeconds = localFileBufferRolloverIntervalSeconds;
	}

	public void setLocalFileBufferArchiveDirectory(String localFileBufferArchiveDirectory) {
		mLocalFileBufferArchiveDirectory = localFileBufferArchiveDirectory;
	}

	public void setLocalFileBufferArchiveFileCount(int localFileBufferArchiveFileCount) {
		mLocalFileBufferArchiveFileCount = localFileBufferArchiveFileCount;
	}

	@Override
	public boolean requiresLayout() {
		return true;
	}

	@Override
	public void activateOptions() {
		LogLog.debug("==> HdfsRollingFileAppender.activateOptions()");

		HdfsLogDestination hdfsDestination = new HdfsLogDestination();

		hdfsDestination.setDirectory(mHdfsDestinationDirectory);
		hdfsDestination.setFile(mHdfsDestinationFile);
		hdfsDestination.setEncoding(mEncoding);
		hdfsDestination.setRolloverIntervalSeconds(mHdfsDestinationRolloverIntervalSeconds);

		LocalFileLogBuffer localFileBuffer = new LocalFileLogBuffer();

		localFileBuffer.setDirectory(mLocalFileBufferDirectory);
		localFileBuffer.setFile(mLocalFileBufferFile);
		localFileBuffer.setEncoding(mEncoding);
		localFileBuffer.setRolloverIntervalSeconds(mLocalFileBufferRolloverIntervalSeconds);
		localFileBuffer.setArchiveDirectory(mLocalFileBufferArchiveDirectory);
		localFileBuffer.setArchiveFileCount(mLocalFileBufferArchiveFileCount);

		setBufferAndDestination(localFileBuffer, hdfsDestination);

		start();

		LogLog.debug("<== HdfsRollingFileAppender.activateOptions()");
	}

	@Override
	protected void append(LoggingEvent event) {
		if(isLogable()) {
			String logMsg = this.layout.format(event);
	
			if(layout.ignoresThrowable()) {
				String[] strThrowable = event.getThrowableStrRep();
				if (strThrowable != null) {
					StringBuilder sb = new StringBuilder();
	
					sb.append(logMsg);
	
					for(String s : strThrowable) {
						sb.append(s).append(Layout.LINE_SEP);
					}
					
					logMsg = sb.toString();
				}
			}
			
			addToBuffer(logMsg);
		}
	}
}
