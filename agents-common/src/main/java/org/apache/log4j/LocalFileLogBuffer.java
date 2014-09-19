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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.dgc.VMID;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeSet;

import org.apache.log4j.LocalFileLogBuffer.MiscUtil;
import org.apache.log4j.helpers.LogLog;


public class LocalFileLogBuffer implements LogBuffer<String> {
	private static final int    DEFAULT_ROLLOVER_INTERVAL = 600;

	private String  mDirectory               = null;
	private String  mFile                    = null;
	private String  mEncoding                = null;
	private boolean mIsAppend                = true;
	private int     mRolloverIntervalSeconds = DEFAULT_ROLLOVER_INTERVAL;
	private String  mArchiveDirectory        = null;
	private int     mArchiveFileCount        = 10;

	private Writer mWriter                  = null;
	private String mCurrentFilename         = null;
	private long   mNextRolloverTime        = 0;

	private DestinationDispatcherThread mDispatcherThread = null;

	public String getDirectory() {
		return mDirectory;
	}

	public void setDirectory(String directory) {
		mDirectory = directory;
	}

	public String getFile() {
		return mFile;
	}

	public void setFile(String file) {
		mFile = file;
	}

	public String getEncoding() {
		return mEncoding;
	}

	public void setEncoding(String encoding) {
		mEncoding = encoding;
	}

	public boolean getIsAppend() {
		return mIsAppend;
	}

	public void setIsAppend(boolean isAppend) {
		mIsAppend = isAppend;
	}

	public int getRolloverIntervalSeconds() {
		return mRolloverIntervalSeconds;
	}

	public void setRolloverIntervalSeconds(int rolloverIntervalSeconds) {
		mRolloverIntervalSeconds = rolloverIntervalSeconds;
	}

	public String getArchiveDirectory() {
		return mArchiveDirectory;
	}

	public void setArchiveDirectory(String archiveDirectory) {
		mArchiveDirectory = archiveDirectory;
	}

	public int getArchiveFileCount() {
		return mArchiveFileCount;
	}

	public void setArchiveFileCount(int archiveFileCount) {
		mArchiveFileCount = archiveFileCount;
	}


	@Override
	public void start(LogDestination<String> destination) {
		LogLog.debug("==> LocalFileLogBuffer.start()");

		openFile();

		mDispatcherThread = new DestinationDispatcherThread(this, destination);

		mDispatcherThread.start();

		LogLog.debug("<== LocalFileLogBuffer.start()");
	}

	@Override
	public void stop() {
		LogLog.debug("==> LocalFileLogBuffer.stop()");
		
		DestinationDispatcherThread dispatcherThread = mDispatcherThread;
		mDispatcherThread = null;

		if(dispatcherThread != null && dispatcherThread.isAlive()) {
			dispatcherThread.stopThread();

			try {
				dispatcherThread.join();
			} catch (InterruptedException e) {
				LogLog.warn("LocalFileLogBuffer.stop(): failed in waiting for DispatcherThread", e);
			}
		}

		closeFile();

		LogLog.debug("<== LocalFileLogBuffer.stop()");
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

		Writer writer = mWriter;

		if(writer != null) {
			try {
				writer.write(log);

				ret = true;
			} catch(IOException excp) {
				LogLog.warn("LocalFileLogBuffer.add(): write failed", excp);
			}
		} else {
			LogLog.warn("LocalFileLogBuffer.add(): writer is null");
		}

		return ret;
	}

	private void openFile() {
		LogLog.debug("==> LocalFileLogBuffer.openFile()");

		closeFile();

		mCurrentFilename = MiscUtil.replaceTokens(mDirectory + File.separator + mFile);

		FileOutputStream ostream = null;
		try {
			ostream = new FileOutputStream(mCurrentFilename, mIsAppend);
		} catch(Exception excp) {
			MiscUtil.createParents(new File(mCurrentFilename));

			try {
				ostream = new FileOutputStream(mCurrentFilename, mIsAppend);
			} catch(Exception ex) {
				// ignore; error printed down
			}
		}

		mWriter = createWriter(ostream);

		if(mWriter != null) {
			LogLog.debug("LocalFileLogBuffer.openFile(): opened file " + mCurrentFilename);

			updateNextRolloverTime();
		} else {
			LogLog.warn("LocalFileLogBuffer.openFile(): failed to open file for write" + mCurrentFilename);

			mCurrentFilename = null;
		}

		LogLog.debug("<== LocalFileLogBuffer.openFile()");
	}

	private void closeFile() {
		LogLog.debug("==> LocalFileLogBuffer.closeFile()");

		Writer writer = mWriter;

		mWriter = null;

		if(writer != null) {
			try {
				writer.close();
			} catch(IOException excp) {
				LogLog.warn("LocalFileLogBuffer: failed to close file " + mCurrentFilename, excp);
			}

			if(mDispatcherThread != null) {
				mDispatcherThread.addLogfile(mCurrentFilename);
			}
		}

		LogLog.debug("<== LocalFileLogBuffer.closeFile()");
	}

	private void rollover() {
		LogLog.debug("==> LocalFileLogBuffer.rollover()");

		closeFile();

		openFile();

		LogLog.debug("<== LocalFileLogBuffer.rollover()");
	}

	public OutputStreamWriter createWriter(OutputStream os ) {
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

	boolean isCurrentFilename(String filename) {
		return mCurrentFilename != null && filename != null && filename.equals(mCurrentFilename);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("LocalFileLogBuffer {");
		sb.append("Directory=").append(mDirectory).append("; ");
		sb.append("File=").append(mFile).append("; ");
		sb.append("RolloverIntervaSeconds=").append(mRolloverIntervalSeconds).append("; ");
		sb.append("ArchiveDirectory=").append(mArchiveDirectory).append("; ");
		sb.append("ArchiveFileCount=").append(mArchiveFileCount);
		sb.append("}");

		return sb.toString();
	}
	
	static class MiscUtil {
		static String TOKEN_HOSTNAME             = "%hostname%";
		static String TOKEN_APP_INSTANCE         = "%app-instance%";
		static String TOKEN_FILE_OPEN_TIME_START = "%file-open-time:";
		static String TOKEN_FILE_OPEN_TIME_END   = "%";
		
		static VMID sJvmID = new VMID();
	
		public static String replaceTokens(String str) {
			if(str == null) {
				return str;
			}
	
			str = replaceHostname(str);
			str = replaceAppInstance(str);
			str = replaceFileOpenTime(str);
	
			return str;
		}
	
		public static String replaceHostname(String str) {
			if(!str.contains(TOKEN_HOSTNAME)) {
				return str;
			}
	
			String hostName = null;
	
			try {
				hostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException excp) {
				LogLog.warn("LocalFileLogBuffer", excp);
			}
	
			if(hostName == null) {
				hostName = "Unknown";
			}
	
			return str.replaceAll(TOKEN_HOSTNAME, hostName);
		}
		
		public static String replaceAppInstance(String str) {
			if(!str.contains(TOKEN_APP_INSTANCE)) {
				return str;
			}
	
			String appInstance = Integer.toString(Math.abs(sJvmID.hashCode()));
	
			return str.replaceAll(TOKEN_APP_INSTANCE, appInstance);
		}
	
		public static String replaceFileOpenTime(String str) {
			Date now = new Date();
	
	        while(str.contains(TOKEN_FILE_OPEN_TIME_START)) {
	            int tagStartPos = str.indexOf(TOKEN_FILE_OPEN_TIME_START);
	            int tagEndPos   = str.indexOf(TOKEN_FILE_OPEN_TIME_END, tagStartPos + TOKEN_FILE_OPEN_TIME_START.length());
	
	            if(tagEndPos <= tagStartPos) {
	            	break;
	            }
	
	            String tag      = str.substring(tagStartPos, tagEndPos+1);
	            String dtFormat = tag.substring(TOKEN_FILE_OPEN_TIME_START.length(), tag.lastIndexOf(TOKEN_FILE_OPEN_TIME_END));
	
	            String replaceStr = "";
	
	            if(dtFormat != null) {
	                SimpleDateFormat sdf = new SimpleDateFormat(dtFormat);
	
	                replaceStr = sdf.format(now);
	            }
	
	            str = str.replaceAll(tag, replaceStr);
	        }
	
	        return str;
		}
	
		public static void createParents(File file) {
			if(file != null) {
				String parentName = file.getParent();
	
				if (parentName != null) {
					File parentDir = new File(parentName);
	
					if(!parentDir.exists()) {
						parentDir.mkdirs();
					}
				}
			}
		}

		public static long getNextRolloverTime(long lastRolloverTime, long interval) {
			long now = System.currentTimeMillis() / 1000 * 1000; // round to second

			if(lastRolloverTime <= 0) {
				// should this be set to the next multiple-of-the-interval from start of the day?
				return now + interval;
			} else if(lastRolloverTime <= now) {
				long nextRolloverTime = now + interval;

				// keep it at 'interval' boundary
				long trimInterval = (nextRolloverTime - lastRolloverTime) % interval;

				return nextRolloverTime - trimInterval;
			} else {
				return lastRolloverTime;
			}
		}
	}
}

class DestinationDispatcherThread extends Thread {
	private TreeSet<String>        mCompletedLogfiles = new TreeSet<String>();
	private boolean                mStopThread        = false;
	private LocalFileLogBuffer     mFileLogBuffer     = null;
	private LogDestination<String> mDestination       = null;

	private String         mCurrentLogfile = null;
	private BufferedReader mReader         = null;

	public DestinationDispatcherThread(LocalFileLogBuffer fileLogBuffer, LogDestination<String> destination) {
		super(DestinationDispatcherThread.class.getSimpleName());

		mFileLogBuffer = fileLogBuffer;
		mDestination   = destination;

		setDaemon(true);
	}

	public void addLogfile(String filename) {
		LogLog.debug("==> DestinationDispatcherThread.addLogfile(" + filename + ")");

		synchronized(mCompletedLogfiles) {
			mCompletedLogfiles.add(filename);
			mCompletedLogfiles.notify();
		}

		LogLog.debug("<== DestinationDispatcherThread.addLogfile(" + filename + ")");
	}

	public String getNextLogfile() {
		synchronized(mCompletedLogfiles) {
			return mCompletedLogfiles.pollFirst();
		}
	}

	public void stopThread() {
		mStopThread = true;
	}

	@Override
	public void run() {
		init();
		
		// destination start() should be from the dispatcher thread
		mDestination.start();

		int pollIntervalInMs = 1000;

		while(! mStopThread) {
			String logMsg = getNextLog();

			if(logMsg == null) { // move to the next file
				synchronized(mCompletedLogfiles) {
					while(mCompletedLogfiles.isEmpty() && !mStopThread) {
						try {
							mCompletedLogfiles.wait(pollIntervalInMs);
						} catch(InterruptedException excp) {
							LogLog.warn("LocalFileLogBuffer.run(): failed to wait for log file", excp);
						}
					}
					
					if(!mCompletedLogfiles.isEmpty()) {
						openNextFile();
					}
				}
			} else { // deliver to the msg to destination
				while(! mDestination.add(logMsg) && !mStopThread) {
					try {
						Thread.sleep(pollIntervalInMs);
					} catch(InterruptedException excp) {
						LogLog.warn("LocalFileLogBuffer.run(): failed to wait for destination to be available", excp);
					}
				}
			} 
		}

		mDestination.stop();
	}

	private void init() {
		LogLog.debug("==> DestinationDispatcherThread.init()");

		String dirName   = MiscUtil.replaceTokens(mFileLogBuffer.getDirectory());
		File   directory = new File(dirName);

		if(directory.exists() && directory.isDirectory()) {
			File[] files = directory.listFiles();

			if(files != null) {
				for(File file : files) {
					if(file.exists() && file.canRead()) {
						String filename = file.getAbsolutePath();
						if(! mFileLogBuffer.isCurrentFilename(filename)) {
							addLogfile(filename);
						}
					}
				}
			}
		}

		openNextFile();

		LogLog.debug("<== DestinationDispatcherThread.init()");
	}
	
	private String getNextLog() {
		String log = null;

		if(mReader != null) {
			try {
				log = mReader.readLine();
			} catch (IOException excp) {
				LogLog.warn("LocalFileLogBuffer.getNextLog(): failed to read from file " + mCurrentLogfile, excp);
			}

			if(log == null) {
				closeCurrentFile();
			}
		}

		return log;
	}

	private void openNextFile() {
		LogLog.debug("==> openNextFile()");

		closeCurrentFile();

		while(mReader == null) {
			mCurrentLogfile = getNextLogfile();
			
			if(mCurrentLogfile != null) {
				try {
					FileReader fr = new FileReader(mCurrentLogfile);
	
					mReader = new BufferedReader(fr);
				} catch(FileNotFoundException excp) {
					LogLog.warn("openNextFile(): error while opening file " + mCurrentLogfile, excp);
				}
			}
		}

		LogLog.debug("<== openNextFile(" + mCurrentLogfile + ")");
	}
	
	private void closeCurrentFile() {
		LogLog.debug("==> closeCurrentFile(" + mCurrentLogfile + ")");

		if(mReader != null) {
			try {
				mReader.close();
			} catch(IOException excp) {
				// ignore
			}
		}
		mReader = null;
		
		archiveCurrentFile();

		LogLog.debug("<== closeCurrentFile(" + mCurrentLogfile + ")");
	}

	private void archiveCurrentFile() {
		if(mCurrentLogfile != null) {
			File   logFile         = new File(mCurrentLogfile);
			String archiveFilename = MiscUtil.replaceTokens(mFileLogBuffer.getArchiveDirectory() + File.separator + logFile.getName());

			try {
				if(logFile.exists()) {
					File archiveFile = new File(archiveFilename);

					MiscUtil.createParents(archiveFile);

					if(! logFile.renameTo(archiveFile)) {
						// TODO: renameTo() does not work in all cases. in case of failure, copy the file contents to the destination and delete the file
					}

					// TODO: ensure no more than mFileLogBuffer.getArchiveFileCount() archive files are kept
				}
			} catch(Exception excp) {
				LogLog.warn("archiveCurrentFile(): faile to move " + mCurrentLogfile + " to archive location " + archiveFilename, excp);
			}
		}
		mCurrentLogfile = null;
	}
}

