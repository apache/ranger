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

package org.apache.ranger.pdp.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.pdp.config.gson.PolicyExclusionStrategy;
import org.apache.ranger.pdp.constants.RangerConstants;
import org.apache.ranger.pdp.model.PolicyContainer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

public abstract class ConfigWatcher extends Thread {

	private static final Log LOG = LogFactory.getLog(ConfigWatcher.class);

	public static final String EXPECTED_MIME_TYPE = "application/json" ;

	// public static final String EXPECTED_MIME_TYPE = "application/octet-stream";

	private static final String LASTUPDATED_PARAM = "epoch";
	private static final String POLICY_COUNT_PARAM = "policyCount";
	private static final String AGENT_NAME_PARAM = "agentId" ;

	private static final int MAX_AGENT_NAME_LEN = 255 ;


	private String url;

	private long intervalInMilliSeconds;

	private long lastModifiedTime = 0;

	private boolean shutdownFlag = false;
	
	private String lastStoredFileName = null;

	protected PolicyContainer policyContainer = null;

	private static PolicyExclusionStrategy policyExclusionStrategy = new PolicyExclusionStrategy();

	private static RangerCredentialProvider rangerCp = null;
	
	
	public abstract void doOnChange();
	
	private String keyStoreFile =  null ;
	private String keyStoreFilepwd = null; 
	private String keyStoreURL = null;
	private String keyStoreAlias = null;
	private String trustStoreFile = null ;
	private String trustStoreFilepwd = null ;
	private String trustStoreURL = null;
	private String trustStoreAlias = null;
	private String keyStoreType = null ;
	private String trustStoreType = null ;
	private SSLContext sslContext = null ;
	private HostnameVerifier hv =  null ;
	private String agentName = "unknown" ;
	
	private String sslConfigFileName = null ;
	
	public boolean policyCacheLoadedOnce = false;
	
	public boolean cacheModfied = false;

	public ConfigWatcher(String url, long aIntervalInMilliSeconds,String sslConfigFileName,String lastStoredFileName) {
		super("RangerConfigURLWatcher");
		setDaemon(true);
		this.url = url;
		intervalInMilliSeconds = aIntervalInMilliSeconds;
		this.sslConfigFileName = sslConfigFileName ;
		this.agentName = getAgentName(this.url) ;
		this.lastStoredFileName = lastStoredFileName; 
		if (LOG.isInfoEnabled()) {
			LOG.info("Creating PolicyRefreshser with url: " + url +
				", refreshInterval(milliSeconds): " + aIntervalInMilliSeconds +
				", sslConfigFileName: " + sslConfigFileName +
				", lastStoredFileName: " + lastStoredFileName);
	    }
		init();
		validateAndRun();
		LOG.debug("Created new ConfigWatcher for URL [" + url + "]");
	}
	
	
	public void init() {
		if (sslConfigFileName != null) {
			LOG.debug("Loading SSL Configuration from [" + sslConfigFileName + "]");
			InputStream in =  null ;
			try {
				Configuration conf = new Configuration() ;
				in = getFileInputStream(sslConfigFileName) ;
				if (in != null) {
					conf.addResource(in);
				}
				
				rangerCp = RangerCredentialProvider.getInstance();
				
				keyStoreFile = conf.get(RangerConstants.RANGER_POLICYMGR_CLIENT_KEY_FILE);
				
				keyStoreURL=conf.get(RangerConstants.RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL);
				keyStoreAlias=RangerConstants.RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS;
				
				char[] v_keyStoreFilePwd = getCredential(keyStoreURL,keyStoreAlias);
				if (  v_keyStoreFilePwd == null ) {
					keyStoreFilepwd = null;
				} else {
					keyStoreFilepwd = new String(v_keyStoreFilePwd);
				}
				
				trustStoreFile = conf.get(RangerConstants.RANGER_POLICYMGR_TRUSTSTORE_FILE);
				
				trustStoreURL=conf.get(RangerConstants.RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL);
				trustStoreAlias=RangerConstants.RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS;
				
				char[] v_TrustStoreFilePwd = getCredential(trustStoreURL,trustStoreAlias);
				if (  v_TrustStoreFilePwd == null ) {
					trustStoreFilepwd = null;
				} else {
					trustStoreFilepwd = new String(v_TrustStoreFilePwd);
				}
			
				
				keyStoreType = conf.get(RangerConstants.RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE, RangerConstants.RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT);
				trustStoreType = conf.get(RangerConstants.RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE, RangerConstants.RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT);
			}
			catch(IOException ioe) {
				LOG.error("Unable to load SSL Config FileName: [" + sslConfigFileName + "]", ioe);
			}
			finally {
				if (in != null) {
					try {
						in.close() ;
					} catch (IOException e) {
						LOG.error("Unable to close SSL Config FileName: [" + sslConfigFileName + "]", e) ;
					}
				}
			}
			
			LOG.debug("Keystore filename:[" + keyStoreFile + "]");
			LOG.debug("TrustStore filename:[" + trustStoreFile + "]");
			
		}
	}

	public String getURL() {
		return url;
	}

	public long getIntervalInMilliSeconds() {
		return intervalInMilliSeconds;
	}

	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void run() {
		while (!shutdownFlag) {
			validateAndRun();
			try {
				Thread.sleep(intervalInMilliSeconds);
			} catch (InterruptedException e) {
				LOG.error("Unable to complete  sleep for [" + intervalInMilliSeconds + "]", e);
			}
		}
	}

	private void validateAndRun() {
		if (isFileChanged()) {
			LOG.debug("Policy has been changed from " + url + " ... RELOADING");
			try {
				doOnChange();
			} catch (Exception e) {
				LOG.error("Unable to complete  doOnChange() method on file change  [" + url + "]", e);
			}
		} else {
			LOG.debug("No Change found in the policy from " + url);
		}
	}
	
	public boolean iscacheModfied() {
		   return cacheModfied;
	}

	private boolean isFileChanged() {
		boolean isChanged = false;
		
		cacheModfied = false;
		try {	
			
			Client client = null;
			ClientResponse response = null;

			try {

				int policyCount = getPolicyCount(policyContainer);

				if (url.contains("https")) {
					// build SSL Client
					client = buildSSLClient();
				}

				if (client == null) {
					client = Client.create();
				}

				WebResource webResource = client.resource(url)
							.queryParam(LASTUPDATED_PARAM, String.valueOf(lastModifiedTime))
							.queryParam(POLICY_COUNT_PARAM, String.valueOf(policyCount))
							.queryParam(AGENT_NAME_PARAM, agentName);

				response = webResource.accept(EXPECTED_MIME_TYPE).get(ClientResponse.class);
				
             
				if (response != null) {
					
					boolean responsePresent = true;
					int	responseStatus = response.getStatus();
					
					if ( fetchPolicyfromCahce(responsePresent,responseStatus,lastStoredFileName) ) {
						/* If the response is other than 200 and 304 load the policy from the cache */
						isChanged = true;
						
					} else {     
		            	   /* If Policy Manager is available fetch the policy from it */
		            	   if (response.getStatus() == 200) {
								String jsonString = response.getEntity(String.class);
								Gson gson = new GsonBuilder().setPrettyPrinting().addDeserializationExclusionStrategy(policyExclusionStrategy).create();
								PolicyContainer newPolicyContainer = gson.fromJson(jsonString, PolicyContainer.class);
								if ((newPolicyContainer.getLastUpdatedTimeInEpoc() > lastModifiedTime) || (getPolicyCount(newPolicyContainer) != policyCount)) {
									policyContainer = newPolicyContainer;
									lastModifiedTime = policyContainer.getLastUpdatedTimeInEpoc();
									isChanged = true;
									policyCacheLoadedOnce = false;
									cacheModfied = true;
									if (LOG.isDebugEnabled()) {
										LOG.debug("Got response: 200 with {change in lastupdatedTime}\n" + gson.toJson(newPolicyContainer));
									}
								} else {
									if (LOG.isDebugEnabled()) {
										LOG.debug("Got response: 200 with {no-change in lastupdatedTime}\n" + gson.toJson(newPolicyContainer));
									}
									isChanged = false;
								}
							} else if (response.getStatus() == 304) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("Got response: 304 ");
								}
								isChanged = false; // No Change has been there since our
													// earlier request
							} else {
								LOG.error("Unable to get a valid response for isFileChanged()  call for [" + url + "] = response code found [" + response.getStatus() + "]");
							}
						}
					    					 
					 } else {						           
						LOG.error("Unable to get a valid response for isFileChanged()  call for [" + url + "] - got null response.");
						// force the policy update to get fresh copy
						lastModifiedTime = 0;
					}
				 
			} finally {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.destroy();
				}
			}
		} catch (Throwable t) {
			
			boolean responsePresent = false;
			int	responseStatus = -1;
			if (LOG.isDebugEnabled()) {
				LOG.debug("Policy Manager Failed",t);
			}
			if ( fetchPolicyfromCahce(responsePresent,responseStatus,lastStoredFileName) ) {
	 	    /* Successfully found the Policy Cache file and loaded */
		  	     isChanged = false;
		     } else {
		    	 LOG.error("Unable to complete isFileChanged()  call for [" + url + "]", t);
				 // force the policy update to get fresh copy
				 lastModifiedTime = 0;
			     LOG.error("Policy file Cache not found..");
			    throw new RuntimeException("Unable to find Enterprise Policy Storage");
			 }
				
		} finally {
			if (isChanged) {
				LOG.info("URL: [" + url + "], isModified: " + isChanged + ", lastModifiedTime:" + lastModifiedTime);
			} else if (LOG.isDebugEnabled()) {
				LOG.debug("URL: [" + url + "], isModified: " + isChanged + ", lastModifiedTime:" + lastModifiedTime);
			}
		}
		return isChanged;
	}

	public PolicyContainer getPolicyContainer() {
		return policyContainer;
	}

	private int getPolicyCount(PolicyContainer aPolicyContainer) {
		return (aPolicyContainer == null ? 0 : (aPolicyContainer.getAcl() == null ? 0 : aPolicyContainer.getAcl().size()));
	}

	
	public synchronized Client buildSSLClient() {
		Client client = null;
		try {

			ClientConfig config = new DefaultClientConfig();
			
			if (sslContext == null) {

				KeyManager[] kmList = null;
				TrustManager[] tmList = null;
	
				if (keyStoreFile != null && keyStoreFilepwd != null) {
	
					KeyStore keyStore = KeyStore.getInstance(keyStoreType);
					InputStream in = null ;
					try {
						in = getFileInputStream(keyStoreFile) ;
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + keyStoreFile + "]");
							return client ;
						}
						keyStore.load(in, keyStoreFilepwd.toCharArray());
						KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RangerConstants.RANGER_SSL_KEYMANAGER_ALGO_TYPE);
						keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());
						kmList = keyManagerFactory.getKeyManagers();
					}
					finally {
						if (in != null) {
							in.close(); 
						}
					}
					 
				}
	
				if (trustStoreFile != null && trustStoreFilepwd != null) {
	
					KeyStore trustStore = KeyStore.getInstance(trustStoreType);
					InputStream in = null ;
					try {
						in = getFileInputStream(trustStoreFile) ;
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + trustStoreFile + "]");
							return client ;
						}
						trustStore.load(in, trustStoreFilepwd.toCharArray());
						TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RangerConstants.RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);
						trustManagerFactory.init(trustStore);
						tmList = trustManagerFactory.getTrustManagers();
					}
					finally {
						if (in != null) {
							in.close() ;
						}
					}
				}

				sslContext = SSLContext.getInstance(RangerConstants.RANGER_SSL_CONTEXT_ALGO_TYPE);
	
				sslContext.init(kmList, tmList, new SecureRandom());

				hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						return session.getPeerHost().equals(urlHostName);
					}
				};

			}

			config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hv, sslContext));

			client = Client.create(config);

		} catch (KeyStoreException e) {
			LOG.error("Unable to obtain from KeyStore", e);
		} catch (NoSuchAlgorithmException e) {
			LOG.error("SSL algorithm is available in the environment", e);
		} catch (CertificateException e) {
			LOG.error("Unable to obtain the requested certification ", e);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to find the necessary SSL Keystore and TrustStore Files", e);
		} catch (IOException e) {
			LOG.error("Unable to read the necessary SSL Keystore and TrustStore Files", e);
		} catch (KeyManagementException e) {
			LOG.error("Unable to initials the SSLContext", e);
		} catch (UnrecoverableKeyException e) {
			LOG.error("Unable to recover the key from keystore", e);
		}
		return client;
	}
	
	private InputStream getFileInputStream(String fileName)  throws IOException {
		InputStream in = null ;
		
		File f = new File(fileName) ;
		
		if (f.exists()) {
			in = new FileInputStream(f) ;
		}
		else {
			in = ClassLoader.getSystemResourceAsStream(fileName) ;
		}
		return in ;
	}
		
	public static String getAgentName(String aUrl) {
		String hostName = null ;
		String repoName = null ;
		try {
			hostName = InetAddress.getLocalHost().getHostName() ;
		} catch (UnknownHostException e) {
			LOG.error("ERROR: Unable to find hostname for the agent ", e);
			hostName = "unknownHost" ;
		}
		
		String[] tokens = aUrl.split("/") ;
		
		if ( tokens.length > 0 ) {
			repoName = tokens[tokens.length-1] ;
		}
		else {
			repoName = "unknownRepo" ;
		}
		
		String agentName  = hostName + "-" + repoName ;
		
		if (agentName.length() > MAX_AGENT_NAME_LEN ) {
			agentName = agentName.substring(0,MAX_AGENT_NAME_LEN) ;
		}
		
		return agentName  ;
	}
	
	private boolean fetchPolicyfromCahce( boolean responsePresent, int responseStatus, String lastStoredFileName){
	
		boolean cacheFound = false;
		
		if (  ( responsePresent == false ) || ( responseStatus != 200 && responseStatus != 304)  ) {
		
			/* Policy Manager not available read the policy from the last enforced one */
			
			if (policyCacheLoadedOnce) {
				cacheFound = true;
				return cacheFound;
			}
			
			try {
	    		/* read the last stored policy file and load the PolicyContainer */
					LOG.info("Policy Manager not available, using the last stored Policy File" + this.lastStoredFileName );
					LOG.debug("LastStoredFileName when policymgr was available" + this.lastStoredFileName);
					
		    		BufferedReader jsonString = new BufferedReader(new FileReader(this.lastStoredFileName));	                		
		        	Gson gson = new GsonBuilder().setPrettyPrinting().addDeserializationExclusionStrategy(policyExclusionStrategy).create();	                    	
		        	PolicyContainer newPolicyContainer = gson.fromJson(jsonString, PolicyContainer.class);	 
		        	policyContainer = newPolicyContainer;
					lastModifiedTime = policyContainer.getLastUpdatedTimeInEpoc();
					if (LOG.isDebugEnabled()) {
						LOG.debug("Policy Manager not available.Got response =" + responseStatus +"\n" + gson.toJson(newPolicyContainer));	
					}
					
					cacheFound = true;
					policyCacheLoadedOnce = true;
	        	
	    	 	} catch( FileNotFoundException fe ){
	    	 		
		    		/* unable to get the last stored policy, raise warning for unavailability of policy cache file and continue...*/
		    		if ( this.lastStoredFileName == null ) {
		    			LOG.info("Policy cache file not found...XAagent authorization not enabled");
		    		}
		    		else {
		    			LOG.info("Unable to access Policy cache file...XAagent authorization not enabled");
		    		}
	   	    }
			
		}
	
		return cacheFound;
	}
	
	private char[] getCredential(String url, String alias) {
		char[] credStr=rangerCp.getCredentialString(url,alias);
		return credStr;
	}
}
 
