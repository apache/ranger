/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.authorization.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class HDFSAccessVerifierFactory {
	
	private static final Log LOG = LogFactory.getLog(HDFSAccessVerifierFactory.class) ;

	private static HDFSAccessVerifier hdfsAccessVerifier = null ;
	
	public static HDFSAccessVerifier getInstance() {
		if (hdfsAccessVerifier == null) {
			synchronized(HDFSAccessVerifierFactory.class) {
				HDFSAccessVerifier temp = hdfsAccessVerifier ;
				if (temp == null) {
					
					String hdfsAccessVerifierClassName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.HDFS_ACCESS_VERIFIER_CLASS_NAME_PROP, XaSecureHadoopConstants.HDFS_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE) ;
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
