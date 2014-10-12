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
package com.xasecure.audit.provider.hdfs;


import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xasecure.audit.provider.DebugTracer;
import com.xasecure.audit.provider.LogDestination;
import com.xasecure.audit.provider.MiscUtil;

public class HdfsLogDestination<T> implements LogDestination<T> {
	public final static String EXCP_MSG_FILESYSTEM_CLOSED = "Filesystem closed";

	private String  mDirectory                = null;
	private String  mFile                     = null;
	private int     mFlushIntervalSeconds     = 1 * 60;
	private String  mEncoding                 = null;
	private boolean mIsAppend                 = false;
	private int     mRolloverIntervalSeconds  = 24 * 60 * 60;
	private int     mOpenRetryIntervalSeconds = 60;
	private DebugTracer mLogger               = null;

	private OutputStreamWriter mWriter             = null; 
	private String             mHdfsFilename       = null;
	private long               mNextRolloverTime   = 0;
	private long               mNextFlushTime      = 0;
	private long               mLastOpenFailedTime = 0;
	private boolean            mIsStopInProgress   = false;

	public HdfsLogDestination(DebugTracer tracer) {
		mLogger = tracer;
	}

	public String getDirectory() {
		return mDirectory;
	}

	public void setDirectory(String directory) {
		this.mDirectory = directory;
	}

	public String getFile() {
		return mFile;
	}

	public void setFile(String file) {
		this.mFile = file;
	}

	public int getFlushIntervalSeconds() {
		return mFlushIntervalSeconds;
	}

	public void setFlushIntervalSeconds(int flushIntervalSeconds) {
		mFlushIntervalSeconds = flushIntervalSeconds;
	}

	public String getEncoding() {
		return mEncoding;
	}

	public void setEncoding(String encoding) {
		mEncoding = encoding;
	}

	public int getRolloverIntervalSeconds() {
		return mRolloverIntervalSeconds;
	}

	public void setRolloverIntervalSeconds(int rolloverIntervalSeconds) {
		this.mRolloverIntervalSeconds = rolloverIntervalSeconds;
	}

	public int getOpenRetryIntervalSeconds() {
		return mOpenRetryIntervalSeconds;
	}

	public void setOpenRetryIntervalSeconds(int minIntervalOpenRetrySeconds) {
		this.mOpenRetryIntervalSeconds = minIntervalOpenRetrySeconds;
	}

	@Override
	public void start() {
		mLogger.debug("==> HdfsLogDestination.start()");

		openFile();

		mLogger.debug("<== HdfsLogDestination.start()");
	}

	@Override
	public void stop() {
		mLogger.debug("==> HdfsLogDestination.stop()");

		mIsStopInProgress = true;

		closeFile();

		mIsStopInProgress = false;

		mLogger.debug("<== HdfsLogDestination.stop()");
	}

	@Override
	public boolean isAvailable() {
		return mWriter != null;
	}

	@Override
	public boolean send(T log) {
		boolean ret = false;
		
		if(log != null) {
			String msg = log.toString();

			ret = sendStringified(msg);
		}

		return ret;
	}

	@Override
	public boolean sendStringified(String log) {
		boolean ret = false;

		checkFileStatus();

		OutputStreamWriter writer = mWriter;

		if(writer != null) {
			try {
				writer.write(log + MiscUtil.LINE_SEPARATOR);

				ret = true;
			} catch (IOException excp) {
				mLogger.warn("HdfsLogDestination.sendStringified(): write failed", excp);

				closeFile();
			}
		}

		return ret;
	}

	private void openFile() {
		mLogger.debug("==> HdfsLogDestination.openFile()");

		closeFile();

		mNextRolloverTime = MiscUtil.getNextRolloverTime(mNextRolloverTime, (mRolloverIntervalSeconds * 1000L));

		long startTime = MiscUtil.getRolloverStartTime(mNextRolloverTime, (mRolloverIntervalSeconds * 1000L));

		mHdfsFilename = MiscUtil.replaceTokens(mDirectory + org.apache.hadoop.fs.Path.SEPARATOR + mFile, startTime);

		FSDataOutputStream ostream     = null;
		FileSystem         fileSystem  = null;
		Path               pathLogfile = null;
		Configuration      conf        = null;
		boolean            bOverwrite  = false;

		try {
			mLogger.debug("HdfsLogDestination.openFile(): opening file " + mHdfsFilename);

			URI uri = URI.create(mHdfsFilename);

			// TODO: mechanism to XA-HDFS plugin to disable auditing of access checks to the current HDFS file

			conf        = new Configuration();
			pathLogfile = new Path(mHdfsFilename);
			fileSystem  = FileSystem.get(uri, conf);

			try {
				if(fileSystem.exists(pathLogfile)) { // file already exists. either append to the file or write to a new file
					if(mIsAppend) {
						ostream = fileSystem.append(pathLogfile);
					} else {
						mHdfsFilename =  getNewFilename(mHdfsFilename, fileSystem);
						pathLogfile   = new Path(mHdfsFilename);
					}
				}

				// if file does not exist or if mIsAppend==false, create the file
				if(ostream == null) {
					ostream = fileSystem.create(pathLogfile, bOverwrite);
				}
			} catch(IOException excp) {
				// append may not be supported by the filesystem; or the file might already be open by another application. Try a different filename - with current timestamp
				mHdfsFilename =  getNewFilename(mHdfsFilename, fileSystem);
				pathLogfile   = new Path(mHdfsFilename);
			}

			if(ostream == null){
				ostream = fileSystem.create(pathLogfile, bOverwrite);
			}
		} catch(IOException ex) {
			Path parentPath = pathLogfile.getParent();

			try {
				if(parentPath != null&& fileSystem != null && !fileSystem.exists(parentPath) && fileSystem.mkdirs(parentPath)) {
					ostream = fileSystem.create(pathLogfile, bOverwrite);
				} else {
					logException("HdfsLogDestination.openFile() failed", ex);
				}
			} catch (IOException e) {
				logException("HdfsLogDestination.openFile() failed", e);
			} catch (Throwable e) {
				mLogger.warn("HdfsLogDestination.openFile() failed", e);
			}
		} catch(Throwable ex) {
			mLogger.warn("HdfsLogDestination.openFile() failed", ex);
		} finally {
			// TODO: unset the property set above to exclude auditing of logfile opening
			//        System.setProperty(hdfsCurrentFilenameProperty, null);
		}

		mWriter = createWriter(ostream);

		if(mWriter != null) {
			mLogger.debug("HdfsLogDestination.openFile(): opened file " + mHdfsFilename);

			mNextFlushTime      = System.currentTimeMillis() + (mFlushIntervalSeconds * 1000L);
			mLastOpenFailedTime = 0;
		} else {
			mLogger.warn("HdfsLogDestination.openFile(): failed to open file for write " + mHdfsFilename);

			mHdfsFilename = null;
			mLastOpenFailedTime = System.currentTimeMillis();
		}

		mLogger.debug("<== HdfsLogDestination.openFile(" + mHdfsFilename + ")");
	}

	private void closeFile() {
		mLogger.debug("==> HdfsLogDestination.closeFile()");

		OutputStreamWriter writer = mWriter;

		mWriter = null;

		if(writer != null) {
			try {
				writer.flush();
				writer.close();
			} catch(IOException excp) {
				logException("HdfsLogDestination: failed to close file " + mHdfsFilename, excp);
			}
		}

		mLogger.debug("<== HdfsLogDestination.closeFile()");
	}

	private void rollover() {
		mLogger.debug("==> HdfsLogDestination.rollover()");

		closeFile();

		openFile();

		mLogger.debug("<== HdfsLogDestination.rollover()");
	}

	private void checkFileStatus() {
		long now = System.currentTimeMillis();

		if(mWriter == null) {
			if(now > (mLastOpenFailedTime + (mOpenRetryIntervalSeconds * 1000L))) {
				openFile();
			}
		} else  if(now > mNextRolloverTime) {
			rollover();
		} else if(now > mNextFlushTime) {
			try {
				mNextFlushTime = now + (mFlushIntervalSeconds * 1000L);

				mWriter.flush();
			} catch (IOException excp) {
				logException("HdfsLogDestination: failed to flush", excp);
			}
		}
	}

	private OutputStreamWriter createWriter(OutputStream os ) {
	    OutputStreamWriter writer = null;

	    if(os != null) {
			if(mEncoding != null) {
				try {
					writer = new OutputStreamWriter(os, mEncoding);
				} catch(UnsupportedEncodingException excp) {
					mLogger.warn("HdfsLogDestination.createWriter(): failed to create output writer.", excp);
				}
			}
	
			if(writer == null) {
				writer = new OutputStreamWriter(os);
			}
	    }

	    return writer;
	}

    private String getNewFilename(String fileName, FileSystem fileSystem) {
    	if(fileName == null) {
    		return "";
    	}

        for(int i = 1; ; i++) {
        	String ret = fileName;

	        String strToAppend = "-" + Integer.toString(i);
	
	        int extnPos = ret.lastIndexOf(".");
	
	        if(extnPos < 0) {
	            ret += strToAppend;
	        } else {
	            String extn = ret.substring(extnPos);
	
	            ret = ret.substring(0, extnPos) + strToAppend + extn;
	        }
	        
	        if(fileSystem != null && fileExists(ret, fileSystem)) {
        		continue;
	        } else {
	        	return ret;
	        }
    	}
    }
    
    private boolean fileExists(String fileName, FileSystem fileSystem) {
    	boolean ret = false;

    	if(fileName != null && fileSystem != null) {
    		Path path = new Path(fileName);

    		try {
    			ret = fileSystem.exists(path);
    		} catch(IOException excp) {
    			// ignore
    		}
    	}
 
    	return ret;
    }

    private void logException(String msg, IOException excp) {
		// during shutdown, the underlying FileSystem might already be closed; so don't print error details

		if(mIsStopInProgress) {
			return;
		}

		String  excpMsgToExclude   = EXCP_MSG_FILESYSTEM_CLOSED;;
		String  excpMsg            = excp != null ? excp.getMessage() : null;
		boolean excpExcludeLogging = (excpMsg != null && excpMsg.contains(excpMsgToExclude));
		
		if(! excpExcludeLogging) {
			mLogger.warn(msg, excp);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("HdfsLogDestination {");
		sb.append("Directory=").append(mDirectory).append("; ");
		sb.append("File=").append(mFile).append("; ");
		sb.append("RolloverIntervalSeconds=").append(mRolloverIntervalSeconds);
		sb.append("}");
		
		return sb.toString();
	}
}
