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

 package org.apache.ranger.authorization.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;

public class RangerHiveAccessVerifierFactory {

	private static final Log LOG = LogFactory.getLog(RangerHiveAccessVerifierFactory.class) ;

	private static RangerHiveAccessVerifier hiveAccessVerififer = null ;
	
	public static RangerHiveAccessVerifier getInstance() {
		if (hiveAccessVerififer == null) {
			synchronized(RangerHiveAccessVerifierFactory.class) {
				RangerHiveAccessVerifier temp = hiveAccessVerififer ;
				if (temp == null) {
					String hiveAccessVerifierClassName = RangerConfiguration.getInstance().get(RangerHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_PROP, RangerHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE ) ;

					if (hiveAccessVerifierClassName != null) {
						LOG.info("Hive Access Verification class [" + hiveAccessVerifierClassName + "] - Being built");
						try {
							hiveAccessVerififer = (RangerHiveAccessVerifier) (Class.forName(hiveAccessVerifierClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + hiveAccessVerifierClassName + "] for Hive Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", e);
						} catch (Throwable t) {
							LOG.error("Unable to create HiveAccess Verifier: [" +  hiveAccessVerifierClassName + "]", t);
						}
						finally {
							LOG.info("Created a new instance of class: [" + hiveAccessVerifierClassName + "] for Hive Access verification. (" + hiveAccessVerififer + ")");
						}
					}
				}
				else {
					LOG.error("Unable to obtain hiveAccessVerifier [" +  RangerHadoopConstants.HIVE_ACCESS_VERIFIER_CLASS_NAME_PROP + "]");
				}
			}
		}
		return hiveAccessVerififer ;
	}
}
