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
package com.xasecure.audit.provider.hdfs;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.model.AuditEventBase;
import com.xasecure.audit.provider.BufferedAuditProvider;
import com.xasecure.audit.provider.DebugTracer;
import com.xasecure.audit.provider.LocalFileLogBuffer;
import com.xasecure.audit.provider.Log4jTracer;
import com.xasecure.audit.provider.MiscUtil;

public class HdfsAuditProvider extends BufferedAuditProvider {
	private static final Log LOG = LogFactory.getLog(HdfsAuditProvider.class);
	
	public HdfsAuditProvider() {
	}

	public void init(Map<String, String> properties) {
		String encoding                                = properties.get("encoding");

		String hdfsDestinationDirectory                = properties.get("destination.directory");
		String hdfsDestinationFile                     = properties.get("destination.file");
		int    hdfsDestinationFlushIntervalSeconds     = MiscUtil.parseInteger(properties.get("destination.flush.interval.seconds"), 15 * 60);
		int    hdfsDestinationRolloverIntervalSeconds  = MiscUtil.parseInteger(properties.get("destination.rollover.interval.seconds"), 24 * 60 * 60);
		int    hdfsDestinationOpenRetryIntervalSeconds = MiscUtil.parseInteger(properties.get("destination.open.retry.interval.seconds"), 60);

		String localFileBufferDirectory               = properties.get("local.buffer.directory");
		String localFileBufferFile                    = properties.get("local.buffer.file");
		int    localFileBufferFlushIntervalSeconds    = MiscUtil.parseInteger(properties.get("local.buffer.flush.interval.seconds"), 1 * 60);
		int    localFileBufferFileBufferSizeBytes     = MiscUtil.parseInteger(properties.get("local.buffer.file.buffer.size.bytes"), 8 * 1024);
		int    localFileBufferRolloverIntervalSeconds = MiscUtil.parseInteger(properties.get("local.buffer.rollover.interval.seconds"), 10 * 60);
		String localFileBufferArchiveDirectory        = properties.get("local.archive.directory");
		int    localFileBufferArchiveFileCount        = MiscUtil.parseInteger(properties.get("local.archive.max.file.count"), 10);

		DebugTracer tracer = new Log4jTracer(LOG);

		HdfsLogDestination<AuditEventBase> mHdfsDestination = new HdfsLogDestination<AuditEventBase>(tracer);

		mHdfsDestination.setDirectory(hdfsDestinationDirectory);
		mHdfsDestination.setFile(hdfsDestinationFile);
		mHdfsDestination.setFlushIntervalSeconds(hdfsDestinationFlushIntervalSeconds);
		mHdfsDestination.setEncoding(encoding);
		mHdfsDestination.setRolloverIntervalSeconds(hdfsDestinationRolloverIntervalSeconds);
		mHdfsDestination.setOpenRetryIntervalSeconds(hdfsDestinationOpenRetryIntervalSeconds);

		LocalFileLogBuffer<AuditEventBase> mLocalFileBuffer = new LocalFileLogBuffer<AuditEventBase>(tracer);

		mLocalFileBuffer.setDirectory(localFileBufferDirectory);
		mLocalFileBuffer.setFile(localFileBufferFile);
		mLocalFileBuffer.setFlushIntervalSeconds(localFileBufferFlushIntervalSeconds);
		mLocalFileBuffer.setFileBufferSizeBytes(localFileBufferFileBufferSizeBytes);
		mLocalFileBuffer.setEncoding(encoding);
		mLocalFileBuffer.setRolloverIntervalSeconds(localFileBufferRolloverIntervalSeconds);
		mLocalFileBuffer.setArchiveDirectory(localFileBufferArchiveDirectory);
		mLocalFileBuffer.setArchiveFileCount(localFileBufferArchiveFileCount);
		
		setBufferAndDestination(mLocalFileBuffer, mHdfsDestination);
	}
}



