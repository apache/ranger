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


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LocalFileLogBuffer.MiscUtil;
import org.apache.log4j.helpers.LogLog;

public class HdfsLogDestination implements LogDestination<String> {
	private String  mDirectory               = null;
	private String  mFile                    = null;
	private String  mEncoding                = null;
	private boolean mIsAppend                = true;
	private int     mRolloverIntervalSeconds = 24 * 60 * 60;

	private OutputStreamWriter mWriter           = null; 
	private String             mCurrentFilename  = null;
	private long               mNextRolloverTime = 0;
	
	private String LINE_SEPARATOR = System.getProperty("line.separator");

	public HdfsLogDestination() {
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

	@Override
	public void start() {
		LogLog.debug("==> HdfsLogDestination.start()");

		openFile();

		LogLog.debug("<== HdfsLogDestination.start()");
	}

	@Override
	public void stop() {
		LogLog.debug("==> HdfsLogDestination.stop()");

		closeFile();

		LogLog.debug("<== HdfsLogDestination.stop()");
	}

	@Override
	public boolean isAvailable() {
		return mWriter != null;
	}

	@Override
	public boolean add(String log) {
		boolean ret = false;

		long now = System.currentTimeMillis();

		if(now > mNextRolloverTime) {
			rollover();
		}

		OutputStreamWriter writer = mWriter;

		if(writer != null) {
			try {
				writer.write(log + LINE_SEPARATOR);

				ret = true;
			} catch (IOException excp) {
				LogLog.warn("HdfsLogDestination.add(): write failed", excp);
			}
		}

		return ret;
	}

	private void openFile() {
		LogLog.debug("==> HdfsLogDestination.openFile()");

		closeFile();

		mCurrentFilename = MiscUtil.replaceTokens(mDirectory + File.separator + mFile);

		FSDataOutputStream ostream     = null;
		FileSystem         fileSystem  = null;
		Path               pathLogfile = null;
		Configuration      conf        = null;

		try {
			LogLog.debug("HdfsLogDestination.openFile(): opening file " + mCurrentFilename);

			URI uri = URI.create(mCurrentFilename);

			// TODO: mechanism to notify co-located XA-HDFS plugin to disable auditing of access checks to the current HDFS file
			//       this can be driven by adding an option (property) the logger, which can be configured at the deployment time.
			//       Like: hdfsCurrentFilenameProperty. When this option is set, do the following here:
			//        System.setProperty(hdfsCurrentFilenameProperty, uri.getPath());

			conf        = new Configuration();
			pathLogfile = new Path(mCurrentFilename);
			fileSystem  = FileSystem.get(uri, conf);

			if(fileSystem.exists(pathLogfile)) {
				if(mIsAppend) {
					try {
						ostream = fileSystem.append(pathLogfile);
					} catch(IOException excp) {
						// append may not be supported by the filesystem. rename existing file and create a new one
						String fileSuffix    = MiscUtil.replaceTokens("-" + MiscUtil.TOKEN_FILE_OPEN_TIME_START + "yyyyMMdd-HHmm.ss" + MiscUtil.TOKEN_FILE_OPEN_TIME_END);
						String movedFilename = appendToFilename(mCurrentFilename, fileSuffix);
						Path   movedFilePath = new Path(movedFilename);

						fileSystem.rename(pathLogfile, movedFilePath);
					}
				}
			}

			if(ostream == null){
				ostream = fileSystem.create(pathLogfile);
			}
		} catch(IOException ex) {
			Path parentPath = pathLogfile.getParent();

			try {
				if(parentPath != null&& fileSystem != null && !fileSystem.exists(parentPath) && fileSystem.mkdirs(parentPath)) {
					ostream = fileSystem.create(pathLogfile);
				}
			} catch (IOException e) {
				LogLog.warn("HdfsLogDestination.openFile() failed", e);
			} catch (Throwable e) {
				LogLog.warn("HdfsLogDestination.openFile() failed", e);
			}
		} catch(Throwable ex) {
			LogLog.warn("HdfsLogDestination.openFile() failed", ex);
		} finally {
			// TODO: unset the property set above to exclude auditing of logfile opening
			//        System.setProperty(hdfsCurrentFilenameProperty, null);
		}

		mWriter = createWriter(ostream);

		if(mWriter != null) {
			LogLog.debug("HdfsLogDestination.openFile(): opened file " + mCurrentFilename);

			updateNextRolloverTime();
		} else {
			LogLog.warn("HdfsLogDestination.openFile(): failed to open file for write " + mCurrentFilename);

			mCurrentFilename = null;
		}

		LogLog.debug("<== HdfsLogDestination.openFile(" + mCurrentFilename + ")");
	}

	private void closeFile() {
		LogLog.debug("==> HdfsLogDestination.closeFile()");

		OutputStreamWriter writer = mWriter;

		mWriter = null;

		if(writer != null) {
			try {
				writer.flush();
				writer.close();
			} catch(IOException excp) {
				LogLog.warn("HdfsLogDestination: failed to close file " + mCurrentFilename, excp);
			}
		}

		LogLog.debug("<== HdfsLogDestination.closeFile()");
	}

	private void rollover() {
		LogLog.debug("==> HdfsLogDestination.rollover()");

		closeFile();

		openFile();

		LogLog.debug("<== HdfsLogDestination.rollover()");
	}

	private OutputStreamWriter createWriter(OutputStream os ) {
	    OutputStreamWriter writer = null;

	    if(os != null) {
			if(mEncoding != null) {
				try {
					writer = new OutputStreamWriter(os, mEncoding);
				} catch(UnsupportedEncodingException excp) {
					LogLog.warn("LocalFileLogBuffer: failed to create output writer.", excp);
				}
			}
	
			if(writer == null) {
				writer = new OutputStreamWriter(os);
			}
	    }

	    return writer;
	}

	private void updateNextRolloverTime() {
		mNextRolloverTime = MiscUtil.getNextRolloverTime(mNextRolloverTime, (mRolloverIntervalSeconds * 1000));
	}
	
	private String appendToFilename(String fileName, String strToAppend) {
		String ret = fileName;
		
		if(strToAppend != null) {
			if(ret == null) {
				ret = "";
			}
	
			int extnPos = ret.lastIndexOf(".");
			
			if(extnPos < 0) {
				ret += strToAppend;
			} else {
				String extn = ret.substring(extnPos);
				
				ret = ret.substring(0, extnPos) + strToAppend + extn;
			}
		}

		return ret;
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
