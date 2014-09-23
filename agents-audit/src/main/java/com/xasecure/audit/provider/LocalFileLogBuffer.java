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
package com.xasecure.audit.provider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.helpers.LogLog;


public class LocalFileLogBuffer<T> implements LogBuffer<T> {
	private String  mDirectory               = null;
	private String  mFile                    = null;
	private int     mFlushIntervalSeconds    = 1 * 60;
	private String  mEncoding                = null;
	private boolean mIsAppend                = true;
	private int     mRolloverIntervalSeconds = 10 * 60;
	private String  mArchiveDirectory        = null;
	private int     mArchiveFileCount        = 10;

	private Writer mWriter           = null;
	private String mBufferFilename   = null;
	private long   mNextRolloverTime = 0;
	private long   mNextFlushTime    = 0;

	private DestinationDispatcherThread<T> mDispatcherThread = null;
	
	public LocalFileLogBuffer() {
	}

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
	public void start(LogDestination<T> destination) {
		LogLog.debug("==> LocalFileLogBuffer.start()");

		mDispatcherThread = new DestinationDispatcherThread<T>(this, destination);

		mDispatcherThread.start();

		LogLog.debug("<== LocalFileLogBuffer.start()");
	}

	@Override
	public void stop() {
		LogLog.debug("==> LocalFileLogBuffer.stop()");
		
		DestinationDispatcherThread<T> dispatcherThread = mDispatcherThread;
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
	public synchronized boolean add(T log) {
		boolean ret = false;

		checkFileStatus();

		Writer writer = mWriter;

		if(writer != null) {
			try {
				String msg = MiscUtil.stringify(log);

				if(msg.contains(MiscUtil.LINE_SEPARATOR)) {
					msg = msg.replace(MiscUtil.LINE_SEPARATOR, MiscUtil.ESCAPE_STR + MiscUtil.LINE_SEPARATOR);
				}

				writer.write(msg + MiscUtil.LINE_SEPARATOR);

				ret = true;
			} catch(IOException excp) {
				LogLog.warn("LocalFileLogBuffer.add(): write failed", excp);
			}
		} else {
			LogLog.warn("LocalFileLogBuffer.add(): writer is null");
		}

		return ret;
	}

	@Override
	public boolean isEmpty() {
		return mDispatcherThread == null || mDispatcherThread.isIdle();
	}

	private synchronized void openFile() {
		LogLog.debug("==> LocalFileLogBuffer.openFile()");

		closeFile();

		mNextRolloverTime = MiscUtil.getNextRolloverTime(mNextRolloverTime, (mRolloverIntervalSeconds * 1000L));

		long startTime = MiscUtil.getRolloverStartTime(mNextRolloverTime, (mRolloverIntervalSeconds * 1000L));

		mBufferFilename = MiscUtil.replaceTokens(mDirectory + File.separator + mFile, startTime);

		FileOutputStream ostream = null;
		try {
			ostream = new FileOutputStream(mBufferFilename, mIsAppend);
		} catch(Exception excp) {
			MiscUtil.createParents(new File(mBufferFilename));

			try {
				ostream = new FileOutputStream(mBufferFilename, mIsAppend);
			} catch(Exception ex) {
				// ignore; error printed down
			}
		}

		mWriter = createWriter(ostream);

		if(mWriter != null) {
			LogLog.debug("LocalFileLogBuffer.openFile(): opened file " + mBufferFilename);

			mNextFlushTime = System.currentTimeMillis() + (mFlushIntervalSeconds * 1000L);
		} else {
			LogLog.warn("LocalFileLogBuffer.openFile(): failed to open file for write " + mBufferFilename);

			mBufferFilename = null;
		}

		LogLog.debug("<== LocalFileLogBuffer.openFile()");
	}

	private synchronized void closeFile() {
		LogLog.debug("==> LocalFileLogBuffer.closeFile()");

		Writer writer = mWriter;

		mWriter = null;

		if(writer != null) {
			try {
				writer.flush();
				writer.close();
			} catch(IOException excp) {
				LogLog.warn("LocalFileLogBuffer: failed to close file " + mBufferFilename, excp);
			}

			if(mDispatcherThread != null) {
				mDispatcherThread.addLogfile(mBufferFilename);
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

	private void checkFileStatus() {
		long now = System.currentTimeMillis();

		if(now > mNextRolloverTime) {
			rollover();
		} else  if(mWriter == null) {
			openFile();
		} else if(now > mNextFlushTime) {
			try {
				mWriter.flush();

				mNextFlushTime = now + (mFlushIntervalSeconds * 1000L);
			} catch (IOException excp) {
				LogLog.warn("LocalFileLogBuffer: failed to flush", excp);
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
					LogLog.warn("LocalFileLogBuffer: failed to create output writer.", excp);
				}
			}
	
			if(writer == null) {
				writer = new OutputStreamWriter(os);
			}
	    }

	    return writer;
	}

	boolean isCurrentFilename(String filename) {
		return mBufferFilename != null && filename != null && filename.equals(mBufferFilename);
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
	
}

class DestinationDispatcherThread<T> extends Thread {
	private TreeSet<String>        mCompletedLogfiles = new TreeSet<String>();
	private boolean                mStopThread        = false;
	private LocalFileLogBuffer<T>  mFileLogBuffer     = null;
	private LogDestination<T>      mDestination       = null;

	private String         mCurrentLogfile = null;
	private BufferedReader mReader         = null;

	public DestinationDispatcherThread(LocalFileLogBuffer<T> fileLogBuffer, LogDestination<T> destination) {
		super(DestinationDispatcherThread.class.getSimpleName() + "-" + System.currentTimeMillis());

		mFileLogBuffer = fileLogBuffer;
		mDestination   = destination;

		setDaemon(true);
	}

	public void addLogfile(String filename) {
		LogLog.debug("==> DestinationDispatcherThread.addLogfile(" + filename + ")");

		if(filename != null) {
			synchronized(mCompletedLogfiles) {
				mCompletedLogfiles.add(filename);
				mCompletedLogfiles.notify();
			}
		}

		LogLog.debug("<== DestinationDispatcherThread.addLogfile(" + filename + ")");
	}

	public void stopThread() {
		mStopThread = true;
	}

	public boolean isIdle() {
		synchronized(mCompletedLogfiles) {
			return mCompletedLogfiles.isEmpty() && mCurrentLogfile == null;
		}
	}

	@Override
	public void run() {
		UserGroupInformation loginUser = null;

		try {
			loginUser = UserGroupInformation.getLoginUser();
		} catch (IOException excp) {
			LogLog.error("DestinationDispatcherThread.run(): failed to get login user details. Audit files will not be sent to HDFS destination", excp);
		}

		if(loginUser == null) {
			LogLog.error("DestinationDispatcherThread.run(): failed to get login user. Audit files will not be sent to HDFS destination");

			return;
		}

		loginUser.doAs(new PrivilegedAction<Integer>() {
			@Override
			public Integer run() {
				doRun();

				return 0;
			}
		});
	}

	private void doRun() {
		init();

		mDestination.start();

		long pollIntervalInMs = 1000L;

		while(! mStopThread) {
			synchronized(mCompletedLogfiles) {
				while(mCompletedLogfiles.isEmpty() && !mStopThread) {
					try {
						mCompletedLogfiles.wait(pollIntervalInMs);
					} catch(InterruptedException excp) {
						LogLog.warn("LocalFileLogBuffer.run(): failed to wait for log file", excp);
					}
				}
				
				mCurrentLogfile = mCompletedLogfiles.pollFirst();
			}
			
			if(mCurrentLogfile != null) {
				sendCurrentFile();
			}
		}

		mDestination.stop();
	}

	private void init() {
		LogLog.debug("==> DestinationDispatcherThread.init()");

		String dirName   = MiscUtil.replaceTokens(mFileLogBuffer.getDirectory(), 0);
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

		LogLog.debug("<== DestinationDispatcherThread.init()");
	}
	
	private boolean sendCurrentFile() {
		LogLog.debug("==> DestinationDispatcherThread.sendCurrentFile()");

		boolean ret = false;

		long destinationPollIntervalInMs = 1000L;

		openCurrentFile();

		 while(!mStopThread) {
			String log = getNextStringifiedLog();

			if(log == null) { // reached end-of-file
				ret = true;

				break;
			}

			// loop until log is sent successfully
			while(!mStopThread && !mDestination.sendStringified(log)) {
				sleep(destinationPollIntervalInMs, "LocalFileLogBuffer.sendCurrentFile(" + mCurrentLogfile + "): failed to wait for destination to be available");
			}
		}

		closeCurrentFile();

		if(!mStopThread) {
			archiveCurrentFile();
		}

		LogLog.debug("<== DestinationDispatcherThread.sendCurrentFile()");

		return ret;
	}

	private String getNextStringifiedLog() {
		String log = null;

		if(mReader != null) {
			try {
				while(true) {
					String line = mReader.readLine();

					if(line == null) { // reached end-of-file
						break;
					}

					if(line.endsWith(MiscUtil.ESCAPE_STR)) {
						line = line.substring(0, line.length() - MiscUtil.ESCAPE_STR.length());

						if(log == null) {
							log = line;
						} else {
							log += MiscUtil.LINE_SEPARATOR;
							log += line;
						}

						continue;
					} else {
						if(log == null) {
							log = line;
						} else {
							log += line;
						}
						break;
					}
				}
			} catch (IOException excp) {
				LogLog.warn("getNextStringifiedLog.getNextLog(): failed to read from file " + mCurrentLogfile, excp);
			}
		}

		return log;
	}

	private void openCurrentFile() {
		LogLog.debug("==> openCurrentFile(" + mCurrentLogfile + ")");

		if(mCurrentLogfile != null) {
			try {
				FileInputStream inStr = new FileInputStream(mCurrentLogfile);
				
				InputStreamReader strReader = createReader(inStr);
				
				if(strReader != null) {
					mReader = new BufferedReader(strReader);
				}
			} catch(FileNotFoundException excp) {
				LogLog.warn("openNextFile(): error while opening file " + mCurrentLogfile, excp);
			}
		}

		LogLog.debug("<== openCurrentFile(" + mCurrentLogfile + ")");
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

		LogLog.debug("<== closeCurrentFile(" + mCurrentLogfile + ")");
	}

	private void archiveCurrentFile() {
		if(mCurrentLogfile != null) {
			File   logFile         = new File(mCurrentLogfile);
			String archiveDirName  = MiscUtil.replaceTokens(mFileLogBuffer.getArchiveDirectory(), 0);
			String archiveFilename = archiveDirName + File.separator +logFile.getName();

			try {
				if(logFile.exists()) {
					File archiveFile = new File(archiveFilename);

					MiscUtil.createParents(archiveFile);

					if(! logFile.renameTo(archiveFile)) {
						// TODO: renameTo() does not work in all cases. in case of failure, copy the file contents to the destination and delete the file
						LogLog.warn("archiving failed to move file: " + mCurrentLogfile + " ==> " + archiveFilename);
					}

					File   archiveDir = new File(archiveDirName);
					File[] files      = archiveDir.listFiles(new FileFilter() {
											@Override
											public boolean accept(File f) {
												return f.isFile();
											}
										});

					int numOfFilesToDelete = files == null ? 0 : (files.length - mFileLogBuffer.getArchiveFileCount());

					if(numOfFilesToDelete > 0) {
						Arrays.sort(files, new Comparator<File>() {
												@Override
												public int compare(File f1, File f2) {
													return (int)(f1.lastModified() - f2.lastModified());
												}
											});

						for(int i = 0; i < numOfFilesToDelete; i++) {
							if(! files[i].delete()) {
								LogLog.warn("archiving failed to delete file: " + files[i].getAbsolutePath());
							}
						}
					}
				}
			} catch(Exception excp) {
				LogLog.warn("archiveCurrentFile(): faile to move " + mCurrentLogfile + " to archive location " + archiveFilename, excp);
			}
		}
		mCurrentLogfile = null;
	}

	public InputStreamReader createReader(InputStream iStr) {
		InputStreamReader reader = null;

	    if(iStr != null) {
			String encoding = mFileLogBuffer.getEncoding();

			if(encoding != null) {
				try {
					reader = new InputStreamReader(iStr, encoding);
				} catch(UnsupportedEncodingException excp) {
					LogLog.warn("createReader(): failed to create input reader.", excp);
				}
			}

			if(reader == null) {
				reader = new InputStreamReader(iStr);
			}
	    }

	    return reader;
	}

	private void sleep(long sleepTimeInMs, String onFailMsg) {
		try {
			Thread.sleep(sleepTimeInMs);
		} catch(InterruptedException excp) {
			LogLog.warn(onFailMsg, excp);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("DestinationDispatcherThread {");
		sb.append("ThreadName=").append(this.getName()).append("; ");
		sb.append("CompletedLogfiles.size()=").append(mCompletedLogfiles.size()).append("; ");
		sb.append("StopThread=").append(mStopThread).append("; ");
		sb.append("CurrentLogfile=").append(mCurrentLogfile);
		sb.append("}");

		return sb.toString();
	}
}

