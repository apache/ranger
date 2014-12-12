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

package org.apache.ranger.authorization.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;

public class HBaseAccessControllerFactory {
	
	private static final Log LOG = LogFactory.getLog(HBaseAccessControllerFactory.class) ;

	private static HBaseAccessController hBaseAccessController = null ;
	
	public static HBaseAccessController getInstance() {
		if (hBaseAccessController == null) {
			synchronized(HBaseAccessControllerFactory.class) {
				HBaseAccessController temp = hBaseAccessController ;
				if (temp == null) {
					
					String hBaseAccessControllerClassName = RangerConfiguration.getInstance().get(RangerHadoopConstants.HBASE_ACCESS_VERIFIER_CLASS_NAME_PROP, RangerHadoopConstants.HBASE_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE) ;
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
