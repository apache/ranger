package com.xasecure.audit.provider.hdfs;

import java.util.Map;

import com.xasecure.audit.model.AuditEventBase;
import com.xasecure.audit.provider.BufferedAuditProvider;
import com.xasecure.audit.provider.LocalFileLogBuffer;
import com.xasecure.audit.provider.MiscUtil;

public class HdfsAuditProvider extends BufferedAuditProvider {
	
	public HdfsAuditProvider() {
	}

	public void init(Map<String, String> properties) {
		String encoding                               = properties.get("encoding");

		String hdfsDestinationDirectory                = properties.get("destination.directroy");
		String hdfsDestinationFile                     = properties.get("destination.file");
		int    hdfsDestinationRolloverIntervalSeconds  = MiscUtil.parseInteger(properties.get("destination.rollover.interval.seconds"), 24 * 60 * 60);
		int    hdfsDestinationOpenRetryIntervalSeconds = MiscUtil.parseInteger(properties.get("destination.open.retry.interval.seconds"), 60);

		String localFileBufferDirectory               = properties.get("local.buffer.directroy");
		String localFileBufferFile                    = properties.get("local.buffer.file");
		int    localFileBufferRolloverIntervalSeconds = MiscUtil.parseInteger(properties.get("local.buffer.rollover.interval.seconds"), 10 * 60);
		String localFileBufferArchiveDirectory        = properties.get("local.archive.directroy");
		int    localFileBufferArchiveFileCount        = MiscUtil.parseInteger(properties.get("local.archive.max.file.count"), 10);

		HdfsLogDestination<AuditEventBase> mHdfsDestination = new HdfsLogDestination<AuditEventBase>();

		mHdfsDestination.setDirectory(hdfsDestinationDirectory);
		mHdfsDestination.setFile(hdfsDestinationFile);
		mHdfsDestination.setEncoding(encoding);
		mHdfsDestination.setRolloverIntervalSeconds(hdfsDestinationRolloverIntervalSeconds);
		mHdfsDestination.setOpenRetryIntervalSeconds(hdfsDestinationOpenRetryIntervalSeconds);

		LocalFileLogBuffer<AuditEventBase> mLocalFileBuffer = new LocalFileLogBuffer<AuditEventBase>();

		mLocalFileBuffer.setDirectory(localFileBufferDirectory);
		mLocalFileBuffer.setFile(localFileBufferFile);
		mLocalFileBuffer.setEncoding(encoding);
		mLocalFileBuffer.setRolloverIntervalSeconds(localFileBufferRolloverIntervalSeconds);
		mLocalFileBuffer.setArchiveDirectory(localFileBufferArchiveDirectory);
		mLocalFileBuffer.setArchiveFileCount(localFileBufferArchiveFileCount);
		
		setBufferAndDestination(mLocalFileBuffer, mHdfsDestination);
	}


}
