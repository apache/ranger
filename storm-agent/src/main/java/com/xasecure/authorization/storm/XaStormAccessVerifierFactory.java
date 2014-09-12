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

 package com.xasecure.authorization.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class XaStormAccessVerifierFactory {

	private static final Log LOG = LogFactory.getLog(XaStormAccessVerifierFactory.class) ;

	private static XaStormAccessVerifier stormAccessVerififer = null ;
	
	public static XaStormAccessVerifier getInstance() {
		if (stormAccessVerififer == null) {
			synchronized(XaStormAccessVerifierFactory.class) {
				XaStormAccessVerifier temp = stormAccessVerififer ;
				if (temp == null) {
					String stormAccessVerifierClassName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.STORM_ACCESS_VERIFIER_CLASS_NAME_PROP, 
														XaSecureHadoopConstants.STORM_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE ) ;

					if (stormAccessVerifierClassName != null) {
						LOG.info("Storm Access Verification class [" + stormAccessVerifierClassName + "] - Being build");
						try {
							stormAccessVerififer = (XaStormAccessVerifier) (Class.forName(stormAccessVerifierClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + stormAccessVerifierClassName + "] for Storm Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create StormAccess Verifier: [" +  stormAccessVerifierClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create StormAccess Verifier: [" +  stormAccessVerifierClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create StormAccess Verifier: [" +  stormAccessVerifierClassName + "]", e);
						} catch (Throwable t) {
							LOG.error("Unable to create StormAccess Verifier: [" +  stormAccessVerifierClassName + "]", t);
						}
						finally {
							LOG.info("Created a new instance of class: [" + stormAccessVerifierClassName + "] for StormAccess verification. (" + stormAccessVerififer + ")");
						}
					}
				}
				else {
					LOG.error("Unable to obtain StormAccess verifier [" +  XaSecureHadoopConstants.STORM_ACCESS_VERIFIER_CLASS_NAME_PROP + "]");
				}
			}
		}
		return stormAccessVerififer ;
	}

}
