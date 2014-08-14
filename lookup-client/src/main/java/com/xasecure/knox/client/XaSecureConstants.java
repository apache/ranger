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

package com.xasecure.knox.client;

public class XaSecureConstants {
	
	// xasecure 2-way ssl configuration 

	public static final String XASECURE_KNOX_CLIENT_KEY_FILE 						  = "xasecure.knoxclient.ssl.keystore";	
	public static final String XASECURE_KNOX_CLIENT_KEY_FILE_PASSWORD				  = "xasecure.knoxclien.tssl.keystore.password";	
	public static final String XASECURE_KNOX_CLIENT_KEY_FILE_TYPE 					  = "xasecure.knoxclient.ssl.keystore.type";	

	public static final String XASECURE_KNOX_CLIENT_KEY_FILE_TYPE_DEFAULT 			  = "jks";	

	public static final String XASECURE_KNOX_CLIENT_TRUSTSTORE_FILE					  = "xasecure.knoxclient.ssl.truststore";	
	public static final String XASECURE_KNOX_CLIENT_TRUSTSTORE_FILE_PASSWORD		  = "xasecure.knoxclient.ssl.truststore.password";	
	public static final String XASECURE_KNOX_CLIENT_TRUSTSTORE_FILE_TYPE			  = "xasecure.knoxclient.ssl.truststore.type";	

	public static final String XASECURE_KNOX_CLIENT_TRUSTSTORE_FILE_TYPE_DEFAULT	  = "jks";	
	
	
	public static final String XASECURE_SSL_KEYMANAGER_ALGO_TYPE					  = "SunX509" ;
	public static final String XASECURE_SSL_TRUSTMANAGER_ALGO_TYPE					  = "SunX509" ;
	public static final String XASECURE_SSL_CONTEXT_ALGO_TYPE						  = "SSL" ;

}
