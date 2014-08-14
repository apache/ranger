/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xasecure.authorization.knox;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class KnoxAccessVerifierFactory {

	private static final Log LOG = LogFactory.getLog(KnoxAccessVerifierFactory.class) ;

	private static KnoxAccessVerifier knoxAccessVerififer = null ;
	
	public static KnoxAccessVerifier getInstance() {
		if (knoxAccessVerififer == null) {
			synchronized(KnoxAccessVerifierFactory.class) {
				KnoxAccessVerifier temp = knoxAccessVerififer ;
				if (temp == null) {
					String knoxAccessVerifierClassName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.KNOX_ACCESS_VERIFIER_CLASS_NAME_PROP, XaSecureHadoopConstants.KNOX_ACCESS_VERIFIER_CLASS_NAME_DEFAULT_VALUE ) ;

					if (knoxAccessVerifierClassName != null) {
						LOG.info("Knox Access Verification class [" + knoxAccessVerifierClassName + "] - Being build");
						try {
							knoxAccessVerififer = (KnoxAccessVerifier) (Class.forName(knoxAccessVerifierClassName).newInstance()) ;
							LOG.info("Created a new instance of class: [" + knoxAccessVerifierClassName + "] for Knox Access verification.");
						} catch (InstantiationException e) {
							LOG.error("Unable to create KnoxAccess Verifier: [" +  knoxAccessVerifierClassName + "]", e);
						} catch (IllegalAccessException e) {
							LOG.error("Unable to create KnoxAccess Verifier: [" +  knoxAccessVerifierClassName + "]", e);
						} catch (ClassNotFoundException e) {
							LOG.error("Unable to create KnoxAccess Verifier: [" +  knoxAccessVerifierClassName + "]", e);
						} catch (Throwable t) {
							LOG.error("Unable to create KnoxAccess Verifier: [" +  knoxAccessVerifierClassName + "]", t);
						}
						finally {
							LOG.info("Created a new instance of class: [" + knoxAccessVerifierClassName + "] for Knox Access verification. (" + knoxAccessVerififer + ")");
						}
					}
				}
				else {
					LOG.error("Unable to obtain knoxAccessVerifier [" +  XaSecureHadoopConstants.KNOX_ACCESS_VERIFIER_CLASS_NAME_PROP + "]");
				}
			}
		}
		return knoxAccessVerififer ;
	}
}
