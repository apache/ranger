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

 package com.xasecure.authorization.hadoop.utils;

import java.lang.String;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class XaSecureCredentialProvider {

  private static Log LOG = LogFactory.getLog(XaSecureCredentialProvider.class);

  private static XaSecureCredentialProvider  me = null;

  
  public static XaSecureCredentialProvider getInstance()  {
	  if ( me == null) {
		  synchronized(XaSecureCredentialProvider.class) {
			  XaSecureCredentialProvider temp = me;
			  if ( temp == null){
				  me = new XaSecureCredentialProvider();
			  }
		  }
	  }
	return me;
  }
  
  public char[] getCredentialString(String url, String alias)  {
   List<CredentialProvider> providers =  getCredentialProviders(url); 
   
   if(providers != null) {
	   for(  CredentialProvider provider: providers) {
		   try {
			 CredentialProvider.CredentialEntry credEntry = provider.getCredentialEntry(alias);

	         if (credEntry != null) {
	            return credEntry.getCredential();
	         }
	        } catch(Exception ie) {
	        	LOG.error("Unable to get the Credential Provider from the Configuration", ie);	 
	       }
	   }
   }
   return null;
  }
  
  List<CredentialProvider>  getCredentialProviders(String url){
   try {
	   Configuration conf = new Configuration();

	   conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, url);

	   return CredentialProviderFactory.getProviders(conf);   
      } catch(Exception ie) {
    	  LOG.error("Unable to get the Credential Provider from the Configuration", ie);
      }     
   return null;
  }

}