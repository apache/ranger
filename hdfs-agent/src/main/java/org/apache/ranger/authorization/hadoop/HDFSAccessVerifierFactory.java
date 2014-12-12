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
package org.apache.ranger.authorization.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;

public class HDFSAccessVerifierFactory {
	
	private static final Log LOG = LogFactory.getLog(HDFSAccessVerifierFactory.class) ;

	private static HDFSAccessVerifier hdfsAccessVerifier = null ;
	
	public static HDFSAccessVerifier getInstance() {
		if (hdfsAccessVerifier == null) {
			synchronized(HDFSAccessVerifierFactory.class) {
				HDFSAccessVerifier temp = hdfsAccessVerifier ;
				if (temp == null) {
					
					String hdfsAccessVerifierClassName = RangerConfiguration.getInstance().get(RangerHadoopConstants.HDFS_ACCESS_VERIFIER_CLASS_NAME_PROP, RangerHadoopConstants.HDFS_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE) ;
					if (hdfsAccessVerifierClassName != null) {
						try {
							hdfsAccessVerifierClassName = hdfsAccessVerifierClassName.trim();
							hdfsAccessVerifier = (HDFSAccessVerifier) (Class.forName(hdfsAccessVerifierClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + hdfsAccessVerifierClassName + "] for HDFS Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create HdfsAccessVerifier Verifier: [" +  hdfsAccessVerifierClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create HdfsAccessVerifier Verifier: [" +  hdfsAccessVerifierClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create HdfsAccessVerifier Verifier: [" +  hdfsAccessVerifierClassName + "]", e);
						}
					}
				}
			}
		}
		return hdfsAccessVerifier ;
		
	}
}
