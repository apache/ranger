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
package org.apache.ranger.authorization.hadoop.log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.helpers.LogLog;


/********************************
* HdfsFileAppender
* 
**********************************/
public class HdfsFileAppender extends FileAppender {
   
	   
    // Constants for checking the DatePattern 
    public static final String MINUTES ="Min";
	public static final String HOURS ="Hr";
	public static final String DAYS ="Day";
	public static final String WEEKS ="Week";
	public static final String MONTHS ="Month";

    // The code assumes that the following constants are in a increasing sequence.
    public static final int TOP_OF_TROUBLE = -1;
    public static final int TOP_OF_MINUTE = 0;
    public static final int TOP_OF_HOUR = 1;
    public static final int HALF_DAY = 2;
    public static final int TOP_OF_DAY = 3;
    public static final int TOP_OF_WEEK = 4;
    public static final int TOP_OF_MONTH = 5;
    
    /**
     * The date pattern. By default, the pattern is set to "1Day" meaning daily rollover.
     */
    private String hdfsFileRollingInterval = "1Day";
    
    private String fileRollingInterval = "1Day";

     
    private String scheduledFileCache;

    /**
     * The next time we estimate a rollover should occur.
     */
    private long nextCheck = System.currentTimeMillis() - 1;
    
    private long prevnextCheck = nextCheck;

    private long nextCheckLocal = System.currentTimeMillis() -1;
    
    private long prevnextCheckLocal = nextCheckLocal;
    
    private Date now = new Date();
    
    private Date nowLocal = now;

    private SimpleDateFormat sdf;

    private RollingCalendar rc = new RollingCalendar();
    
    private RollingCalendar rcLocal = new RollingCalendar();
    
    private FileOutputStream ostream = null;
    
    private String fileSystemName;
    
    private Layout layout = null;
    
    private String encoding = null;
    
    private String hdfsfileName = null;
    
    private String actualHdfsfileName = null;
    
    private String scheduledHdfsFileName = null;
    
    private String fileCache = null;
    
    private HdfsSink hs = null;  
    
    private Writer cacheWriter = null;
    
	private FileOutputStream cacheOstream = null;
	
	private  boolean hdfsAvailable = false;
	
    private long hdfsNextCheck = System.currentTimeMillis() - 1;
    
    private boolean timeCheck = false; 

    private int hdfsFileRollOffset = 0;    
    
    private int fileRollOffset = 0;
    
    private boolean firstTime = true;
    
    private boolean firstTimeLocal = true;
    
    private String hdfsLiveUpdate = "true";
        
    boolean hdfsUpdateAllowed = true;
    
    private String hdfsCheckInterval=null;
    
    private String processUser = null;
        
    private String datePattern = "'.'yyyy-MM-dd-HH-mm";

     /**
     * The gmtTimeZone is used only in computeCheckPeriod() method.
     */
    private static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT+0");
    
	private static final String DEFAULT_HDFSCHECKINTERVAL = "2min";

    /**
     * The default constructor does nothing.
     */
    public HdfsFileAppender() {
    	
    }


    /**
     * The <b>hdfsFileRollingInterval</b> takes a string like 1min, 5min,... 1hr, 2hrs,.. 1day, 2days... 1week, 2weeks.. 1month, 2months.. for hdfs File rollover schedule.
     */
    public void setHdfsFileRollingInterval(String pattern) {
        hdfsFileRollingInterval = pattern;
    }

    /** Returns the value of the <b>hdfsFileRollingInterval</b> option. */
    public String getHdfsFileRollingInterval() {
        return hdfsFileRollingInterval;
    }
    
    /**
     * The <b>LocalDatePattern</b> takes a string like 1min, 5min,... 1hr, 2hrs,.. 1day, 2days... 1week, 2weeks.. 1month, 2months.. for local cache File rollover schedule.
     */
    public void setFileRollingInterval(String pattern) {
    	fileRollingInterval = pattern;
    }

    /** Returns the value of the <b>FileRollingInterval</b> option. */
    public String getFileRollingInterval() {
        return fileRollingInterval;
    }

    /**
     * This will set liveHdfsUpdate flag , where true  will update hdfs live else false will create local cache files and copy the files to hdfs
     */
    public void setHdfsLiveUpdate(String val) {
    	hdfsLiveUpdate=val;
     }

    /** Returns the value of the <b>FileRollingInterval</b> option. */
    public String getHdfsLiveUpdate() {
        return hdfsLiveUpdate;
    }
    
    public String getHdfsCheckInterval() {
    	return hdfsCheckInterval;
    }
    
    public void setHdfsCheckInterval(String val){
    	hdfsCheckInterval = ( hdfsCheckInterval != null) ? val : DEFAULT_HDFSCHECKINTERVAL;
    }
    
    public String getEncoding() {
        return encoding;
      }

      public void setEncoding(String value) {
        encoding = value;
    }
    
    public void activateOptions() {
        super.activateOptions();
        
        sdf = new SimpleDateFormat(datePattern); 
        processUser=System.getProperties().getProperty("user.name");
       
        if(hdfsFileRollingInterval != null && fileName != null) {
        	now.setTime(System.currentTimeMillis());
        	int type = computeCheckPeriod(hdfsFileRollingInterval);
            hdfsFileRollOffset = getTimeOffset(hdfsFileRollingInterval);
            printHdfsPeriodicity(type,hdfsFileRollOffset);
            rc.setType(type);
            LogLog.debug("File name: " + fileName);
            File file = new File(fileName);
            scheduledHdfsFileName = hdfsfileName+sdf.format(new Date(file.lastModified()));
       	    firstTime = true;
            LogLog.debug("Local and hdfs Files" + scheduledHdfsFileName + " " +scheduledHdfsFileName) ;
           
        } else {
        	LogLog.error("Either File or hdfsFileRollingInterval options are not set for appender [" + name + "].");
        }
        
        // Local Cache File 

        if (fileRollingInterval != null && fileCache != null){
    	   nowLocal.setTime(System.currentTimeMillis());
           int localtype = computeCheckPeriod(fileRollingInterval);
           fileRollOffset = getTimeOffset(fileRollingInterval);
           printLocalPeriodicity(localtype,fileRollOffset);
           rcLocal.setType(localtype);
           LogLog.debug("LocalCacheFile name: " + fileCache);
           File fileCachehandle = new File(fileCache);
           scheduledFileCache = fileCache+sdf.format(new Date(fileCachehandle.lastModified()));
      	   firstTimeLocal = true;
      	   
        } else {
        	LogLog.error("Either File or LocalDatePattern options are not set for appender [" + name + "].");
        }
        
        hdfsUpdateAllowed = Boolean.parseBoolean(hdfsLiveUpdate);
        actualHdfsfileName = hdfsfileName + sdf.format(System.currentTimeMillis());
    }
    
    public static int containsIgnoreCase(String str1, String str2) {
    	return str1.toLowerCase().indexOf(str2.toLowerCase());
    }
        
    
    public int computeCheckPeriod(String timePattern){

    	if(containsIgnoreCase(timePattern, MINUTES) > 0) {
    		return TOP_OF_MINUTE;
    	}

    	if(containsIgnoreCase(timePattern, HOURS) > 0) {
    		return TOP_OF_HOUR;
    	}
    	
    	if(containsIgnoreCase(timePattern, DAYS) > 0) {
    		return TOP_OF_DAY;
    	}
    	
    	if(containsIgnoreCase(timePattern, WEEKS) > 0) {
    		return TOP_OF_WEEK;
    	}
    	
    	if(containsIgnoreCase(timePattern, MONTHS) > 0) {
    		return TOP_OF_MONTH;
    	}
    	
    	return TOP_OF_TROUBLE;
    }
    	
    
    private void printHdfsPeriodicity(int type, int offset) {
        switch(type) {
            case TOP_OF_MINUTE:
            	LogLog.debug("Appender [" + name + "] to be rolled every " + offset +  " minute.");
                break;
            case TOP_OF_HOUR:
            	LogLog.debug("Appender [" + name + "] to be rolled on top of every " + offset +  " hour.");
                break;
            case HALF_DAY:
            	LogLog.debug("Appender [" + name + "] to be rolled at midday and midnight.");
                break;
            case TOP_OF_DAY:
            	LogLog.debug("Appender [" + name + "] to be rolled on top of every " + offset +  " day.");
                break;
            case TOP_OF_WEEK:
            	LogLog.debug("Appender [" + name + "] to be rolled on top of every " + offset +  " week.");
                break;
            case TOP_OF_MONTH:
            	LogLog.debug("Appender [" + name + "] to be rolled at start of every  " + offset +  " month.");
                break;
            default:
            	LogLog.warn("Unknown periodicity for appender [" + name + "].");
        }
    }
    
    
    public int getTimeOffset(String timePattern){
	    int index;	
	    int offset=-1;
	    
		if ((index = containsIgnoreCase(timePattern, MINUTES)) > 0) {
			offset = Integer.parseInt(timePattern.substring(0,index));
		}
		
		if ((index = containsIgnoreCase(timePattern, HOURS)) > 0) {
			offset = Integer.parseInt(timePattern.substring(0,index));
			
		}
		
		if ((index = containsIgnoreCase(timePattern, DAYS)) > 0) {
			offset = Integer.parseInt(timePattern.substring(0,index));
			
		}
		
		if ((index = containsIgnoreCase(timePattern, WEEKS)) > 0) {
			offset = Integer.parseInt(timePattern.substring(0,index));
			
		}
		
		if ((index = containsIgnoreCase(timePattern, MONTHS)) > 0) {
			offset = Integer.parseInt(timePattern.substring(0,index));
			
		}
		
		return offset;
    }
    
    private void printLocalPeriodicity(int type, int offset) {
        switch(type) {
            case TOP_OF_MINUTE:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled every " + offset +  " minute.");
                break;
            case TOP_OF_HOUR:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled on top of every " + offset +  " hour.");
                break;
            case HALF_DAY:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled at midday and midnight.");
                break;
            case TOP_OF_DAY:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled on top of every " + offset +  " day.");
                break;
            case TOP_OF_WEEK:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled on top of every " + offset +  " week.");
                break;
            case TOP_OF_MONTH:
            	LogLog.debug("Appender [" + name + "] Local File to be rolled at start of every  " + offset +  " month.");
                break;
            default:
            	LogLog.warn("Unknown periodicity for appender [" + name + "].");
        }
    }
    
   
    

    /**
     * Rollover the current file to a new file.
     */
    private void rollOver() throws IOException {
        /* Compute filename, but only if hdfsFileRollingInterval is specified */
        if(hdfsFileRollingInterval == null) {
            errorHandler.error("Missing hdfsFileRollingInterval option in rollOver().");
            return;
        }
        
        long epochNow = System.currentTimeMillis();
         
        String datedhdfsFileName = hdfsfileName+sdf.format(epochNow);
        
        LogLog.debug("In rollOver epochNow" + epochNow + "  " + "nextCheck: " + prevnextCheck );
        
      
        // It is too early to roll over because we are still within the bounds of the current interval. Rollover will occur once the next interval is reached.
           
        if (epochNow < prevnextCheck) {
        	return;
        }

        // close current file, and rename it to datedFilename
        this.closeFile();
        
        LogLog.debug("Rolling Over hdfs file to " + scheduledHdfsFileName);
        
                
        if ( hdfsAvailable ) {
           // for hdfs file we don't rollover the fike, we rename the file.
           actualHdfsfileName = hdfsfileName + sdf.format(System.currentTimeMillis());
        }         
         
        try {
            // This will also close the file. This is OK since multiple close operations are safe.
            this.setFile(fileName, false, this.bufferedIO, this.bufferSize);
        } catch(IOException e) {
            errorHandler.error("setFile(" + fileName + ", false) call failed.");
        }
        scheduledHdfsFileName = datedhdfsFileName;
    }
    
    
    /**
     * Rollover the current Local file to a new file.
     */
    private void rollOverLocal() throws IOException {
        /* Compute filename, but only if datePattern is specified */
        if(fileRollingInterval == null) {
            errorHandler.error("Missing LocalDatePattern option in rollOverLocal().");
            return;
        }
        
        long epochNow = System.currentTimeMillis();
       
        String datedCacheFileName = fileCache+sdf.format(epochNow);
        LogLog.debug("In rollOverLocal() epochNow" + epochNow + "  " + "nextCheckLocal: " + prevnextCheckLocal );
        
        // It is too early to roll over because we are still within the bounds of the current interval. Rollover will occur once the next interval is reached.
        if (epochNow < prevnextCheckLocal ) {
        	return;
        }

        if (new File(fileCache).length() != 0 ) {    
	        LogLog.debug("Rolling Local cache to " + scheduledFileCache);
	        
	  	    this.closeCacheWriter();
		  
		    File target  = new File(scheduledFileCache);
	        if (target.exists()) {
	          target.delete();
	        }
	
	        File file = new File(fileCache);
	      
	        boolean result = file.renameTo(target);
	      
	        if(result) {
	          LogLog.debug(fileCache +" -> "+ scheduledFileCache);
	        } else {
	          LogLog.error("Failed to rename cache file ["+fileCache+"] to ["+scheduledFileCache+"].");
	        }
	        setFileCacheWriter();
	        scheduledFileCache =  datedCacheFileName;
        }
    }

    
    /**
     * <p>
     * Sets and <i>opens</i> the file where the log output will go. The specified file must be writable.
     * <p>
     * If there was already an opened file, then the previous file is closed first.
     * <p>
     * <b>Do not use this method directly. To configure a FileAppender or one of its subclasses, set its properties one by one and then call
     * activateOptions.</b>
     * 
     * @param fileName The path to the log file.
     * @param append If true will append to fileName. Otherwise will truncate fileName.
     */
    public void setFile(String file) {
        // Trim spaces from both ends. The users probably does not want
        // trailing spaces in file names.
        String val = file.trim();
        
        fileName=val;
        fileCache=val+".cache";
        
      } 

    @Override
    public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize) throws IOException {
    	LogLog.debug("setFile called: "+fileName+", "+append);

        // It does not make sense to have immediate flush and bufferedIO.
        if(bufferedIO) {
          setImmediateFlush(false);
        }

        reset();
        
        try {
              //
              //   attempt to create file
              //
        	  ostream = new FileOutputStream(fileName, append);
          } catch(FileNotFoundException ex) {
              //
              //   if parent directory does not exist then
              //      attempt to create it and try to create file
              //      see bug 9150
              //
        	   File umFile = new File(fileName);
        	   String parentName = umFile.getParent();
            	
               if (parentName != null) {
                  File parentDir = new File(parentName);
                  if(!parentDir.exists() && parentDir.mkdirs()) {
                   	 ostream = new FileOutputStream(fileName, append);
                  } else {
                     throw ex;
                  }
               } else {
                  throw ex;
               }
        }
        
        Writer fw = createWriter(ostream);
        if(bufferedIO) {
          fw = new BufferedWriter(fw, bufferSize);
        }
        this.setQWForFiles(fw);
        this.fileName = fileName;
        this.fileAppend = append;
        this.bufferedIO = bufferedIO;
        this.bufferSize = bufferSize;
        
        //set cache file
        setFileCacheWriter();
        
        writeHeader();
                
        LogLog.debug("setFile ended");
    }

    public void setHdfsDestination(final String name) {
    	//Setting the fileSystemname
    	
    	String hostName = null;
    	
    	String val = name.trim();
    	
        try {
        	
			hostName = InetAddress.getLocalHost().getHostName();
		    val=val.replaceAll("%hostname%", hostName);
	        String hostStr[] = val.split(":");
	        if ( hostStr.length > 0 ) {
	           fileSystemName = hostStr[0]+":"+hostStr[1]+":"+hostStr[2];
	           
	           hdfsfileName = hostStr[3];
	           
	        } else {
	        	LogLog.error("Failed to set HdfsSystem and File"); 	
	        }
	        			
		} catch (UnknownHostException uhe) {
			LogLog.error("Setting the Hdfs Desitination Failed", uhe);
		}
        
    	LogLog.debug("FileSystemName:" + fileSystemName + "fileName:"+ hdfsfileName);   
     	
     }
    
    /**
     * This method differentiates HdfsFileAppender from its super class.
     * <p>
     * Before actually logging, this method will check whether it is time to do a rollover. If it is, it will schedule the next rollover time and then rollover.
     */
    @Override
    protected void subAppend(LoggingEvent event) {
    	LogLog.debug("Called subAppend for logging into hdfs...");   
       
        long n = System.currentTimeMillis();
        if(n >= nextCheck) {
            now.setTime(n);
            prevnextCheck = nextCheck;
            nextCheck = rc.getNextCheckMillis(now,hdfsFileRollOffset);
            if ( firstTime) {
            	 prevnextCheck = nextCheck;
            	 firstTime = false;
            }
            try {
            	if (hdfsUpdateAllowed) {
            		rollOver();
            	}
            } catch(IOException e) {
            	LogLog.error("rollOver() failed.", e);
            }
        }
   
        long nLocal = System.currentTimeMillis();
        if ( nLocal > nextCheckLocal ) {
    	  nowLocal.setTime(nLocal);
    	  prevnextCheckLocal = nextCheckLocal;
    	  nextCheckLocal = rcLocal.getNextCheckMillis(nowLocal, fileRollOffset);
    	   if ( firstTimeLocal) {
          	 prevnextCheckLocal = nextCheckLocal;
          	 firstTimeLocal = false;
          }
    	  try {
          	rollOverLocal();
          } catch(IOException e) {
          	LogLog.error("rollOverLocal() failed.", e);
          }
        }
        
        this.layout = this.getLayout();
        this.encoding = this.getEncoding();
        
        // Append HDFS
        appendHDFSFileSystem(event); 
        
   
        //super.subAppend(event);
    }
   
    @Override
    protected
    void reset() {
      closeWriter();
      this.qw = null;
      //this.
      this.closeHdfsWriter();
      this.closeCacheWriter();
   }
    
    @Override
    public synchronized void close() {
    	LogLog.debug("Closing all resource..");
        this.closeFile();
        this.closeHdfsWriter();
        this.closeHdfsOstream();
        this.closeFileSystem();
    }

    @Override
    protected void closeFile() {
        try {
            if(this.ostream != null) {
                this.ostream.close();
                this.ostream = null;
            }
        } catch(IOException ie) {
        	LogLog.error("unable to close output stream", ie);
        }
        this.closeHdfsWriter();
        this.closeHdfsOstream();
     }

    @Override
    protected void closeWriter() {
        try {
            if(this.qw != null) {
                this.qw.close();
                this.qw = null;
            }
        } catch(IOException ie) {
            LogLog.error("unable to close writer", ie);
        }
    }
    
    @Override
    public void finalize() {
        super.finalize();
        close();
    }
  
  
 /******* HDFS Appender methods ***********/
     
 private void appendHDFSFileSystem(LoggingEvent event) {

	long currentTime = System.currentTimeMillis();
	
    try {
    	
    	 if ( currentTime >= hdfsNextCheck ) {
    		 
    		LogLog.debug("About to Open fileSystem" + fileSystemName+" "+actualHdfsfileName) ;
	      	hs = openHdfsSink(fileSystemName,actualHdfsfileName,fileCache,fileAppend,bufferedIO,bufferSize,layout,encoding,scheduledFileCache,cacheWriter,hdfsUpdateAllowed,processUser);
	      	if (hdfsUpdateAllowed) {
	      	    // stream into hdfs only when  liveHdfsUpdate flag is true else write to cache file.
	      		hs.setOsteam();
	      		hs.setWriter();
	      		hs.append(event);
		    } else {
		    	 writeToCache(event);
		    }
 	        hdfsAvailable = true;
  	   
    	   } else {
    		// Write the Log To cache file util time to check hdfs availability
 		    hdfsAvailable = false;
 		    LogLog.debug("Hdfs Down..Will check hdfs vailability after " + hdfsNextCheck + "Current Time :" +hdfsNextCheck ) ;
 		    writeToCache(event);
    	  }
       }
 	   catch(Throwable t) {
 		  // Write the Log To cache file if hdfs connect error out.
 		  hdfsAvailable = false;
 		  if ( !timeCheck ) {
 			 int hdfscheckInterval =  getTimeOffset(hdfsCheckInterval);
 			 hdfsNextCheck = System.currentTimeMillis()+(1000*60*hdfscheckInterval);
 			 timeCheck = true;
 		     LogLog.debug("Hdfs Down..Will check hdfs vailability after " + hdfsCheckInterval , t) ;
 		     
 		     }
 		  	 writeToCache(event);
	   }

    }
    
   
    private HdfsSink openHdfsSink(String fileSystemName,String filename, String fileCache, boolean append, boolean bufferedIO,int bufferSize,Layout layout, String encoding, String scheduledCacheFile, Writer cacheWriter,boolean hdfsUpdateAllowed,String processUser) throws Throwable { 
       
        HdfsSink hs = null;
        hs = HdfsSink.getInstance();
        if ( hs != null) 
        	
        	LogLog.debug("Hdfs Sink successfully instatiated");
        	try {
        		hs.init(fileSystemName, filename, fileCache, append, bufferedIO, bufferSize, layout, encoding,scheduledCacheFile,cacheWriter,hdfsUpdateAllowed,processUser);
        		
        	} catch (Throwable t) {
        		 throw t;
        	}
        return hs;
 	
    }
    
    private void closeHdfsOstream() {
    	if (hs != null ){
    		LogLog.debug("Closing hdfs outstream") ;
    		hs.closeHdfsOstream();
       }
    }
    
    private void closeHdfsWriter() {
    	
    	if (hs != null) {
    	 LogLog.debug("Closing hdfs Writer") ;
    	 hs.closeHdfsWriter();
    	}
    }
    
    private void closeFileSystem() {
    	hs.closeHdfsSink();
    }
    
   
    
    /****** Cache File Methods **/
    
    
    public void setFileCacheWriter()  {
     
     try {
    	 setFileCacheOstream(fileCache);
   	 } catch(IOException ie) {
   		 LogLog.error("Logging failed while tring to write into Cache File..", ie);
   	 }
	 LogLog.debug("Setting Cache Writer..");
	 cacheWriter = createCacheFileWriter(cacheOstream);
	 if(bufferedIO) {
		 cacheWriter = new BufferedWriter(cacheWriter, bufferSize);
	  }		  
	}
    
    
    private  void setFileCacheOstream(String fileCache) throws IOException {
    
     try {
          cacheOstream = new FileOutputStream(fileCache, true);
    	 } catch(FileNotFoundException ex) {
          String parentName = new File(fileCache).getParent();
          if (parentName != null) {
             File parentDir = new File(parentName);
             if(!parentDir.exists() && parentDir.mkdirs()) {
            	 cacheOstream = new FileOutputStream(fileName, true);
             } else {
                throw ex;
             }
          } else {
             throw ex;
          }
    	}
    }
    
    
    public OutputStreamWriter createCacheFileWriter(OutputStream os ) {
	    OutputStreamWriter retval = null;
	  
	    if(encoding != null) {
	      try {
	    	   retval = new OutputStreamWriter(os, encoding);
	      	   } catch(IOException ie) {
	      	     LogLog.warn("Error initializing output writer.");
	      	     LogLog.warn("Unsupported encoding?");
	      	   }
	        }
	     if(retval == null) {
	        retval = new OutputStreamWriter(os);
	     }
	    return retval;
	  }
  
    
   public void writeToCache(LoggingEvent event) {
	 
	   try {  
		     LogLog.debug("Writing log to Cache.." + "layout: "+ this.layout.format(event) + "ignoresThowable: "+layout.ignoresThrowable() + "Writer:" + cacheWriter.toString());
		    		     
		     cacheWriter.write(this.layout.format(event));
		     cacheWriter.flush();
		     
		     if(layout.ignoresThrowable()) {
		      	 String[] s = event.getThrowableStrRep();
		      	 if (s != null) {
			  	   int len = s.length;
			   	   for(int i = 0; i < len; i++) {
				  	  LogLog.debug("Log:" + s[i]);
				  	  cacheWriter.write(s[i]);
				  	  cacheWriter.write(Layout.LINE_SEP);
				  	  cacheWriter.flush();
				  }
		       }
		     }
          } catch (IOException ie) {
            LogLog.error("Unable to log event message to hdfs:", ie);  
          }
  }
   
  public void rollOverCacheFile() {
	  
	  if (new File(fileCache).length() != 0 ) {
		  
		  long epochNow = System.currentTimeMillis();
	       
	      String datedCacheFileName = fileCache + "." + epochNow;             
		  LogLog.debug("Rolling over remaining cache File to new file"+ datedCacheFileName);
		  closeCacheWriter();
		  
		  File target  = new File(datedCacheFileName);
	      if (target.exists()) {
	        target.delete();
	      }
	
	      File file = new File(fileCache);
	      
	      boolean result = file.renameTo(target);
	      
	      if(result) {
	        LogLog.debug(fileCache +" -> "+ datedCacheFileName);
	      } else {
	        LogLog.error("Failed to rename cache file ["+fileCache+"] to ["+datedCacheFileName+"].");
	      }
	  }
  }
    
   public void closeCacheWriter() {
		  try {
	            if(cacheWriter != null) {
	            	cacheWriter.close();
	            	cacheWriter = null;
	            }
	        } catch(IOException ie) {
	        	 LogLog.error("unable to close cache writer", ie);
	        }
	}
}

/**
 * RollingCalendar is a helper class to HdfsFileAppender. Given a periodicity type and the current time, it computes the start of the next interval.
 */

class RollingCalendar extends GregorianCalendar {
    private static final long serialVersionUID = 1L;
    
    private int type = HdfsFileAppender.TOP_OF_TROUBLE;

    RollingCalendar() {
        super();
    }

    RollingCalendar(TimeZone tz, Locale locale) {
        super(tz, locale);
    }

    void setType(int type) {
        this.type = type;
    }

    public long getNextCheckMillis(Date now, int offset) {
        return getNextCheckDate(now,offset).getTime();
    }

    public Date getNextCheckDate(Date now,int offset) {
        this.setTime(now);

        switch(this.type) {
            case HdfsFileAppender.TOP_OF_MINUTE:
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                this.add(Calendar.MINUTE, offset);
                break;
            case HdfsFileAppender.TOP_OF_HOUR:
                this.set(Calendar.MINUTE, 0);
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                this.add(Calendar.HOUR_OF_DAY, offset);
                break;
            case HdfsFileAppender.HALF_DAY:
                this.set(Calendar.MINUTE, 0);
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                int hour = get(Calendar.HOUR_OF_DAY);
                if(hour < 12) {
                    this.set(Calendar.HOUR_OF_DAY, 12);
                } else {
                    this.set(Calendar.HOUR_OF_DAY, 0);
                    this.add(Calendar.DAY_OF_MONTH, 1);
                }
                break;
            case HdfsFileAppender.TOP_OF_DAY:
                this.set(Calendar.HOUR_OF_DAY, 0);
                this.set(Calendar.MINUTE, 0);
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                this.add(Calendar.DATE, offset);
                break;
            case HdfsFileAppender.TOP_OF_WEEK:
                this.set(Calendar.DAY_OF_WEEK, getFirstDayOfWeek());
                this.set(Calendar.HOUR_OF_DAY, 0);
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                this.add(Calendar.WEEK_OF_YEAR, offset);
                break;
            case HdfsFileAppender.TOP_OF_MONTH:
                this.set(Calendar.DATE, 1);
                this.set(Calendar.HOUR_OF_DAY, 0);
                this.set(Calendar.SECOND, 0);
                this.set(Calendar.MILLISECOND, 0);
                this.add(Calendar.MONTH, offset);
                break;
            default:
                throw new IllegalStateException("Unknown periodicity type.");
        }
        return getTime();
    }
    
  
}


/*************
 * Hdfs Sink
 *  
 *************/

class HdfsSink {

	  private static final String DS_REPLICATION_VAL = "1";
	  private static final String DS_REPLICATION_KEY = "dfs.replication";
	  private static final String FS_DEFAULT_NAME_KEY = "fs.default.name";
	  private Configuration conf = null;
	  private FileSystem fs= null;
      private Path pt = null;
	  private FSDataOutputStream hdfsostream = null;
	  private String fsName = null;
	  private String fileName = null;
	  private String fileCache = null;
	  private Layout layout = null;
	  private String encoding = null;
	  private Writer hdfswriter = null;
	  private int bufferSize;
	  private boolean bufferedIO=false;
	  private static int fstime=0;
	  private CacheFileWatcher cfw = null;
	  private boolean hdfsUpdateAllowed=true;
	  private String processUser=null;

	  
	  HdfsSink() {
	  } 
      
	  private static final ThreadLocal<HdfsSink> hdfssink = new ThreadLocal<HdfsSink>() {
	     	  protected HdfsSink initialValue()  {
	          return new HdfsSink();
			 }
 	  };
	
	 public static  HdfsSink getInstance() {
		return hdfssink.get();
	 }
  
	 public void init(String fileSystemName, String fileName, String fileCache,boolean append, boolean bufferedIO, int bufferSize, Layout layout, String encoding, String scheduledCacheFile, Writer cacheWriter, boolean hdfsUpdateAllowed, String processUser) throws Exception{
		   
		   this.fsName=fileSystemName;
		   this.fileName=fileName;
		   this.layout=layout;
		   this.encoding=encoding;
		   this.bufferSize=bufferSize;
		   this.bufferedIO=bufferedIO;
		   this.fileCache=fileCache;
		   this.hdfsUpdateAllowed=hdfsUpdateAllowed;
		   this.processUser=processUser;
		   
		   final Configuration conf= new Configuration();
      	   conf.set(DS_REPLICATION_KEY,DS_REPLICATION_VAL);
      	   conf.set(FS_DEFAULT_NAME_KEY, fsName);
      	   
           try {
        	    if ( fs == null) {
        		 LogLog.debug("Opening Connection to hdfs Sytem" + this.fsName);
        		         		        		 
        		 UserGroupInformation ugi = UserGroupInformation.createProxyUser(this.processUser, UserGroupInformation.getLoginUser());
        		 fs = ugi.doAs( new PrivilegedExceptionAction<FileSystem>() {
    		    	  public FileSystem run() throws Exception {
    		    		 FileSystem filesystem = FileSystem.get(conf); 
    		    		 LogLog.debug("Inside UGI.."  + fsName + " " + filesystem);
    		    		 return filesystem;
    		    	 }
    		     });
        		 
        		 if ( cfw == null) {
      	           // Start the CacheFileWatcher to move the Cache file.
              	      	   
      	          LogLog.debug("About to run CacheFilWatcher...");
      	          Path hdfsfilePath = getParent();
      	          cfw = new CacheFileWatcher(this.fs,this.fileCache,hdfsfilePath,cacheWriter,this.hdfsUpdateAllowed,conf);
      	          cfw.start();
                 }
        	     
        	    }
        	   
	           } catch(Exception ie) {
	        	 
            	 LogLog.error("Unable to Create hdfs logfile:" + ie.getMessage());  
            	 throw ie;
	       }
           
           LogLog.debug("HdfsSystem up: " + fsName + "FS Object:" + fs);
	  }
	  
	  public int getfstime() {
		  return fstime;
	  }
	  public FileSystem getFileSystem() {
		  return fs;
	  }
	  
	  public Path getPath() {
		  return pt;
	  }
	  
	  public Path getParent() {
		  Path pt = new Path(this.fileName);
		  return pt.getParent();
	  }	 
	  
	  public void setOsteam() throws IOException {
	    try {
	    	  pt = new Path(this.fileName);
	    	  // if file Exist append it
	    	  if(fs.exists(pt)) {
	        	  LogLog.debug("Appending File: "+ this.fsName+":"+this.fileName+fs);  
		      	  if (hdfsostream !=null) {
		      		  hdfsostream.close();
		          }
		      	  hdfsostream=fs.append(pt);	
		      	  
	           } else {
	        	   LogLog.debug("Creating File directories in hdfs if not present.."+ this.fsName+":"+this.fileName + fs);  
		           String parentName = new Path(this.fileName).getParent().toString();
		           if(parentName != null) {
		              Path parentDir = new Path(parentName);
		              if (!fs.exists(parentDir) ) {
		               	 LogLog.debug("Creating Parent Directory: " + parentDir );  
		               	 fs.mkdirs(parentDir);
		                 }	
		             }
	           	     hdfsostream = fs.create(pt);
	           }  
	      }  catch (IOException ie) {
	    	 LogLog.debug("Error While appending hdfsd file." + ie); 
             throw ie;
          }
	 }
	 	  
	  public void setWriter() {
		  LogLog.debug("Setting Writer..");
		  hdfswriter = createhdfsWriter(hdfsostream);
		  if(bufferedIO) {
			  hdfswriter = new BufferedWriter(hdfswriter, bufferSize);
		  }		  
	  }
	  
	  public Writer getWriter() {
		  return hdfswriter;
	  }	  	  
	  
	  public void append(LoggingEvent event) throws IOException { 
		try {  
		     LogLog.debug("Writing log to HDFS." + "layout: "+ this.layout.format(event) + "ignoresThowable: "+layout.ignoresThrowable() + "Writer:" + hdfswriter.toString());
		    
			 hdfswriter.write(this.layout.format(event));
			 hdfswriter.flush();
		     if(layout.ignoresThrowable()) {
		      String[] s = event.getThrowableStrRep();
		      if (s != null) {
			  int len = s.length;
			  for(int i = 0; i < len; i++) {
				  LogLog.debug("Log:" + s[i]);
				  hdfswriter.write(s[i]);
				  hdfswriter.write(Layout.LINE_SEP);
				  hdfswriter.flush();
				  }
		       }
		     }
            } catch (IOException ie) {
              LogLog.error("Unable to log event message to hdfs:", ie);  
              throw ie;
            }
       }
	  
	   public void writeHeader() throws IOException {
		 LogLog.debug("Writing log header...");
		 try {
		    if(layout != null) {
		      String h = layout.getHeader();
		      if(h != null && hdfswriter != null)
		    	  LogLog.debug("Log header:" + h);
		    	  hdfswriter.write(h);
		    	  hdfswriter.flush();
		      }
		 } catch (IOException ie) {
             LogLog.error("Unable to log header message to hdfs:", ie); 
             throw ie;
         }
	   }
	   
	   public
	   void writeFooter() throws IOException{
		LogLog.debug("Writing footer header...");
	    try {
	    	if(layout != null) {
	    		String f = layout.getFooter();
	    		if(f != null && hdfswriter != null) {
	    			LogLog.debug("Log:" + f);
	    			hdfswriter.write(f);
	    			hdfswriter.flush();
	    	    }
	    	}
	      } catch (IOException ie) {
            LogLog.debug("Unable to log header message to hdfs:", ie);  
            throw ie;
          }
	   
	   }

	   public void closeHdfsOstream() {
	        try {
	            if(this.hdfsostream != null) {
	                this.hdfsostream.close();
	                this.hdfsostream = null;
	            }
	        } catch(IOException ie) {
	        	 LogLog.error("unable to close output stream", ie);
	        }
	        
	     }
	   
	   public void closeHdfsWriter() {
		  try {
	            if(hdfswriter != null) {
	            	hdfswriter.close();
	            	hdfswriter = null;
	            }
	        } catch(IOException ie) {
	        	 LogLog.error("unable to hfds writer", ie);
	        }
	   }
	   
	   public void closeHdfsSink() {
		  try {
			  if (fs !=null) {
			  		fs.close();
			  	}		
		  	} catch (IOException ie) {
			 LogLog.error("Unable to close hdfs " + fs ,ie);
		}
	   }
		  
	
	  public OutputStreamWriter createhdfsWriter(FSDataOutputStream os ) {
		    OutputStreamWriter retval = null;
		  
		    if(encoding != null) {
		      try {
		    	   retval = new OutputStreamWriter(os, encoding);
		      	   } catch(IOException ie) {
		      	     LogLog.warn("Error initializing output writer.");
		      	     LogLog.warn("Unsupported encoding?");
		      	   }
		        }
		     if(retval == null) {
		        retval = new OutputStreamWriter(os);
		     }
		    return retval;
		   }	  
	 	  
	
  }


// CacheFileWatcher Thread 

class CacheFileWatcher extends Thread {
	
	long CACHEFILE_WATCHER_SLEEP_TIME = 1000*60*2;
	
	Configuration conf = null;
	private FileSystem fs = null;
	private String cacheFile = null;
	private File parentDir = null;
	private File[] files = null;
	private Path fsPath = null;
	private Path hdfsfilePath = null;
	private Writer cacheWriter = null;

	private boolean hdfsUpdateAllowed=true;
	private boolean cacheFilesCopied = false;
	
	CacheFileWatcher(FileSystem fs, String cacheFile, Path hdfsfilePath, Writer cacheWriter, boolean hdfsUpdateAllowed, Configuration conf) {
		this.fs = fs;
		this.cacheFile = cacheFile;
		this.conf = conf;
		this.hdfsfilePath = hdfsfilePath;
		this.cacheWriter = cacheWriter;
		this.hdfsUpdateAllowed = hdfsUpdateAllowed;
	}
	

	public void run(){
		
	LogLog.debug("CacheFileWatcher Started");		
	    
		while (!cacheFilesCopied ){
			
			if (hdfsUpdateAllowed) {
			   rollRemainingCacheFile();
			}
			
		  	if ( !cacheFilePresent(cacheFile) ) {
	  		
				try {
					Thread.sleep(CACHEFILE_WATCHER_SLEEP_TIME);
				} catch (InterruptedException ie) {
					LogLog.error("Unable to complete the CatchFileWatcher Sleep", ie);
				}
			} else {
					try {
						copyCacheFilesToHdfs();
						if (hdfsUpdateAllowed) {
						cacheFilesCopied = true;
						} else {
						cacheFilesCopied = false;
						}
		    	   } catch (Throwable t) {
			    		// Error While copying the file to hdfs and thread goes for sleep and check later
			    		cacheFilesCopied = false;
			    		LogLog. error("Error while copying Cache Files to hdfs..Sleeping for next try",t);		
			    		
				    	try {
				    		Thread.sleep(CACHEFILE_WATCHER_SLEEP_TIME);
				    	} catch (InterruptedException ie) {
				    		LogLog.error("Unable to complete the CatchFileWatcher Sleep", ie);
			    	}
		        }
	        }
	    }
	}
		
	public boolean cacheFilePresent(String filename) {
	    String parent = new File(filename).getParent();
	    if ( parent != null ) {
	    	parentDir = new File(parent);
	    	fsPath = new Path(parent);
	    	files = parentDir.listFiles(new FilenameFilter() {
	    	@Override
	    	     public boolean accept(File parentDir, String name) {
	    	        return name.matches(".*cache.+");
	    	    }
	    	});
	    	if ( files.length > 0) {
	    		LogLog.debug("CacheFile Present..");
	    		return true;
	    	}
	    }
		return false;
	 }
	

	public void copyCacheFilesToHdfs() throws Throwable{
		
		 try {
		     
			 if (!fs.exists(hdfsfilePath) ) {
			  	 LogLog.debug("Creating Parent Directory: " + hdfsfilePath );  
			   	 fs.mkdirs(hdfsfilePath);
			 }	
		 } catch ( Throwable  t) {
		   throw t;
		 }
		 
		 
		 for ( File cacheFile : files) {
			 try {
				LogLog.debug("Copying Files..." + "File Path: " + fsPath + "CacheFile: " +cacheFile + "HDFS Path:" + hdfsfilePath);
				    FileUtil.copy(cacheFile, this.fs, this.hdfsfilePath, true, this.conf);
			    } catch (Throwable t) {
			  
			   throw t;
			} 
		 }
	}
	
	public void rollRemainingCacheFile() {
	  String datePattern = "'.'yyyy-MM-dd-HH-mm";
	  SimpleDateFormat sdf = new SimpleDateFormat(datePattern); 
	  if (new File(cacheFile).length() != 0 ) {
		  long epochNow = System.currentTimeMillis();
	       
	      String datedCacheFileName = cacheFile + sdf.format(epochNow);
	      
		  LogLog.debug("Rolling over remaining cache File "+ datedCacheFileName);
		  closeCacheFile();
		  
		  File target  = new File(datedCacheFileName);
	      if (target.exists()) {
	        target.delete();
	      }
	
	      File file = new File(cacheFile);
	      
	      boolean result = file.renameTo(target);
	      
	      if(result) {
	        LogLog.debug(cacheFile +" -> "+ datedCacheFileName);
	      } else {
	        LogLog.error("Failed to rename cache file ["+cacheFile+"] to ["+datedCacheFileName+"].");
	      }
	     		      
	  }
  }
	
   public void closeCacheFile() {
	  try {
            if(cacheWriter != null) {
            	cacheWriter.close();
            	cacheWriter = null;
            }
        } catch(IOException ie) {
        	 LogLog.error("unable to close cache writer", ie);
        }
	}
}

  
