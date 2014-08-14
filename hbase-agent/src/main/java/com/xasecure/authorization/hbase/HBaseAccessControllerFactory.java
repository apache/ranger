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

package com.xasecure.authorization.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class HBaseAccessControllerFactory {
	
	private static final Log LOG = LogFactory.getLog(HBaseAccessControllerFactory.class) ;

	private static HBaseAccessController hBaseAccessController = null ;
	
	public static HBaseAccessController getInstance() {
		if (hBaseAccessController == null) {
			synchronized(HBaseAccessControllerFactory.class) {
				HBaseAccessController temp = hBaseAccessController ;
				if (temp == null) {
					
					String hBaseAccessControllerClassName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.HBASE_ACCESS_VERIFIER_CLASS_NAME_PROP, XaSecureHadoopConstants.HBASE_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE) ;
					if (hBaseAccessControllerClassName != null) {
						try {
							hBaseAccessControllerClassName = hBaseAccessControllerClassName.trim();
							hBaseAccessController = (HBaseAccessController) (Class.forName(hBaseAccessControllerClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + hBaseAccessControllerClassName + "] for HBase Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create HBaseAccessController : [" +  hBaseAccessControllerClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create HBaseAccessController : [" +  hBaseAccessControllerClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create HBaseAccessController : [" +  hBaseAccessControllerClassName + "]", e);
						}
					}
				}
			}
		}
		return hBaseAccessController ;
		
	}


}
