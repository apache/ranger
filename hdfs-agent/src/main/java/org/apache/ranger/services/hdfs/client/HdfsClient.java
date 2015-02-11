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

 package org.apache.ranger.services.hdfs.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;

public class HdfsClient extends BaseClient {

	private static final Log LOG = LogFactory.getLog(HdfsClient.class) ;

  public HdfsClient(String serviceName) {
		super(serviceName) ;
	}
	
	public HdfsClient(String serviceName, Map<String,String> connectionProperties) {
		super(serviceName,connectionProperties, "hdfs-client") ;
	}
	
	private List<String> listFilesInternal(String baseDir, String fileMatching, final List<String> pathList) {
		List<String> fileList = new ArrayList<String>() ;
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		try {
			Thread.currentThread().setContextClassLoader(getConfigHolder().getClassLoader());
			String dirPrefix = (baseDir.endsWith("/") ? baseDir : (baseDir + "/")) ;
			String filterRegEx = null;
			if (fileMatching != null && fileMatching.trim().length() > 0) {
				filterRegEx = fileMatching.trim() ;
			}
			
			Configuration conf = new Configuration() ;
			UserGroupInformation.setConfiguration(conf);
			
			FileSystem fs = null ;
			try {
				fs = FileSystem.get(conf) ;
				
				FileStatus[] fileStats = fs.listStatus(new Path(baseDir)) ;
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HdfsClient fileStatus : " + fileStats + " PathList :" + pathList) ;
				}
				
				if (fileStats != null) {
					for(FileStatus stat : fileStats) {
						Path path = stat.getPath() ;
						String pathComponent = path.getName() ;
						String prefixedPath = dirPrefix + pathComponent;
				        if ( pathList != null && pathList.contains(prefixedPath)) {
				        	continue;
				        }
						if (filterRegEx == null) {
							fileList.add(prefixedPath) ;
						}
						else if (FilenameUtils.wildcardMatch(pathComponent, fileMatching)) {
							fileList.add(prefixedPath) ;
						}
					}
				}
			} catch (UnknownHostException uhe) {
				String msgDesc = "listFilesInternal: Unable to connect using given config parameters"
						+ " of Hadoop environment [" + getSerivceName() + "].";
				HadoopException hdpException = new HadoopException(msgDesc, uhe);
				hdpException.generateResponseDataMap(false, getMessage(uhe),
						msgDesc + errMsg, null, null);
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HdfsClient listFilesInternal Error : " + uhe) ;
				}
				throw hdpException;
			} catch (FileNotFoundException fne) {
				String msgDesc = "listFilesInternal: Unable to locate files using given config parameters "
						+ "of Hadoop environment [" + getSerivceName() + "].";
				HadoopException hdpException = new HadoopException(msgDesc, fne);
				hdpException.generateResponseDataMap(false, getMessage(fne),
						msgDesc + errMsg, null, null);
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== HdfsClient listFilesInternal Error : " + fne) ;
				}
				
				throw hdpException;
			}
			finally {
			}
		} catch (IOException ioe) {
			String msgDesc = "listFilesInternal: Unable to get listing of files for directory "
					+ baseDir + fileMatching 
					+ "] from Hadoop environment ["
					+ getSerivceName()
					+ "].";
			HadoopException hdpException = new HadoopException(msgDesc, ioe);
			hdpException.generateResponseDataMap(false, getMessage(ioe),
					msgDesc + errMsg, null, null);
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== HdfsClient listFilesInternal Error : " + ioe) ;
			}
			throw hdpException;

		} catch (IllegalArgumentException iae) {
			String msgDesc = "Unable to get listing of files for directory ["
					+ baseDir + "] from Hadoop environment [" + getSerivceName()
					+ "].";
			HadoopException hdpException = new HadoopException(msgDesc, iae);
			hdpException.generateResponseDataMap(false, getMessage(iae),
					msgDesc + errMsg, null, null);
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== HdfsClient listFilesInternal Error : " + iae) ;
			}
			throw hdpException;
		}
		finally {
			Thread.currentThread().setContextClassLoader(prevCl);
		}
		return fileList ;
	}


	public List<String> listFiles(final String baseDir, final String fileMatching, final List<String> pathList) {

		PrivilegedAction<List<String>> action = new PrivilegedAction<List<String>>() {
			@Override
			public List<String> run() {
				return listFilesInternal(baseDir, fileMatching, pathList) ;
			}
			
		};
		return Subject.doAs(getLoginSubject(),action) ;
	}
	
	public static final void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("USAGE: java " + HdfsClient.class.getName() + " repositoryName  basedirectory  [filenameToMatch]") ;
			System.exit(1) ;
		}
		
		String repositoryName = args[0] ;
		String baseDir = args[1] ;
		String fileNameToMatch = (args.length == 2 ? null : args[2]) ;
		
		HdfsClient fs = new HdfsClient(repositoryName) ;
		List<String> fsList = fs.listFiles(baseDir, fileNameToMatch,null) ;
		if (fsList != null && fsList.size() > 0) {
			for(String s : fsList) {
				System.out.println(s) ;
			}
		}
		else {
			System.err.println("Unable to get file listing for [" + baseDir + (baseDir.endsWith("/") ? "" : "/") + fileNameToMatch + "]  in repository [" + repositoryName + "]") ;
		}
	}

	public static HashMap<String, Object> testConnection(String serviceName,
			Map<String, String> configs) {

		HashMap<String, Object> responseData = new HashMap<String, Object>();
		boolean connectivityStatus = false;
		HdfsClient connectionObj = new HdfsClient(serviceName, configs);
		if (connectionObj != null) {
			List<String> testResult = connectionObj.listFiles("/", null,null);
			if (testResult != null && testResult.size() != 0) {
				connectivityStatus = true;
			}
		}
		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			generateResponseDataMap(connectivityStatus, successMsg, successMsg,
					null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any files using given parameters, "
					+ "You can still save the repository and start creating policies, "
					+ "but you would not be able to use autocomplete for resource names. "
					+ "Check xa_portal.log for more info.";
			generateResponseDataMap(connectivityStatus, failureMsg, failureMsg,
					null, null, responseData);
		}
		return responseData;
	}

}
