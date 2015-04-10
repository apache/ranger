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

package org.apache.ranger.plugin.util;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class PolicyRefresher extends Thread {
	private static final Log LOG = LogFactory.getLog(PolicyRefresher.class);

	private final RangerBasePlugin  plugIn;
	private final String            serviceType;
	private final String            serviceName;
	private final RangerAdminClient rangerAdmin;
	private final String            cacheFile;
	private final Gson              gson;

	private long pollingIntervalMs = 30 * 1000;
	private long lastKnownVersion  = -1;



	public PolicyRefresher(RangerBasePlugin plugIn, String serviceType, String appId, String serviceName, RangerAdminClient rangerAdmin, long pollingIntervalMs, String cacheDir) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").PolicyRefresher()");
		}

		this.plugIn            = plugIn;
		this.serviceType       = serviceType;
		this.serviceName       = serviceName;
		this.rangerAdmin       = rangerAdmin;
		this.pollingIntervalMs = pollingIntervalMs;

		if(StringUtils.isEmpty(appId)) {
			appId = serviceType;
		}

		String cacheFilename = String.format("%s_%s.json", appId, serviceName);
		cacheFilename = cacheFilename.replace(File.separatorChar,  '_');
		cacheFilename = cacheFilename.replace(File.pathSeparatorChar,  '_');

		this.cacheFile = cacheDir == null ? null : (cacheDir + File.separator + cacheFilename);

		Gson gson = null;
		try {
			gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
		} catch(Throwable excp) {
			LOG.fatal("PolicyRefresher(): failed to create GsonBuilder object", excp);
		}
		this.gson = gson;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").PolicyRefresher()");
		}
	}

	/**
	 * @return the plugIn
	 */
	public RangerBasePlugin getPlugin() {
		return plugIn;
	}

	/**
	 * @return the serviceType
	 */
	public String getServiceType() {
		return serviceType;
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * @return the rangerAdmin
	 */
	public RangerAdminClient getRangerAdminClient() {
		return rangerAdmin;
	}

	/**
	 * @return the pollingIntervalMilliSeconds
	 */
	public long getPollingIntervalMs() {
		return pollingIntervalMs;
	}

	/**
	 * @param pollingIntervalMilliSeconds the pollingIntervalMilliSeconds to set
	 */
	public void setPollingIntervalMilliSeconds(long pollingIntervalMilliSeconds) {
		this.pollingIntervalMs = pollingIntervalMilliSeconds;
	}


	public void startRefresher() {
		loadFromCache();

		super.start();
	}

	public void stopRefresher() {
		super.interrupt();

	    try {
	        super.join();
	      } catch (InterruptedException excp) {
	        LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): error while waiting for thread to exit", excp);
	      }
	}

	public void run() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").run()");
		}

		while(true) {
			try {
				ServicePolicies svcPolicies = rangerAdmin.getServicePoliciesIfUpdated(lastKnownVersion);

				boolean isUpdated = svcPolicies != null;

				if(isUpdated) {
					long newVersion = svcPolicies.getPolicyVersion() == null ? -1 : svcPolicies.getPolicyVersion().longValue();

		        	if(!StringUtils.equals(serviceName, svcPolicies.getServiceName())) {
		        		LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): ignoring unexpected serviceName '" + svcPolicies.getServiceName() + "' in service-store");

		        		svcPolicies.setServiceName(serviceName);
		        	}

					LOG.info("PolicyRefresher(serviceName=" + serviceName + "): found updated version. lastKnownVersion=" + lastKnownVersion + "; newVersion=" + newVersion);

					saveToCache(svcPolicies);

		        	lastKnownVersion = newVersion;

					plugIn.setPolicies(svcPolicies);
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug("PolicyRefresher(serviceName=" + serviceName + ").run(): no update found. lastKnownVersion=" + lastKnownVersion);
					}
				}
			} catch(Exception excp) {
				LOG.error("PolicyRefresher(serviceName=" + serviceName + "): failed to refresh policies. Will continue to use last known version of policies (" + lastKnownVersion + ")", excp);
			}

			try {
				Thread.sleep(pollingIntervalMs);
			} catch(InterruptedException excp) {
				LOG.info("PolicyRefresher(serviceName=" + serviceName + ").run(): interrupted! Exiting thread", excp);

				break;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").run()");
		}
	}

	private void loadFromCache() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}

		RangerBasePlugin plugIn = this.plugIn;

		if(plugIn != null) {
	    	File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

	    	if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
	    		Reader reader = null;

	    		try {
		        	reader = new FileReader(cacheFile);

			        ServicePolicies policies = gson.fromJson(reader, ServicePolicies.class);

			        if(policies != null) {
			        	if(!StringUtils.equals(serviceName, policies.getServiceName())) {
			        		LOG.warn("ignoring unexpected serviceName '" + policies.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

			        		policies.setServiceName(serviceName);
			        	}

			        	lastKnownVersion = policies.getPolicyVersion() == null ? -1 : policies.getPolicyVersion().longValue();

			        	plugIn.setPolicies(policies);
			        }
		        } catch (Exception excp) {
		        	LOG.error("failed to load policies from cache file " + cacheFile.getAbsolutePath(), excp);
		        } finally {
		        	if(reader != null) {
		        		try {
		        			reader.close();
		        		} catch(Exception excp) {
		        			LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
		        		}
		        	}
		        }
			} else {
				LOG.warn("cache file does not exist or not readble '" + (cacheFile == null ? null : cacheFile.getAbsolutePath()) + "'");
			}
		} else {
			LOG.warn("policyEngine is null");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}
	}

	private void saveToCache(ServicePolicies policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}

		if(policies != null) {
	    	File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

	    	if(cacheFile != null) {
				Writer writer = null;
	
				try {
					writer = new FileWriter(cacheFile);
	
			        gson.toJson(policies, writer);
		        } catch (Exception excp) {
		        	LOG.error("failed to save policies to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
		        } finally {
		        	if(writer != null) {
		        		try {
		        			writer.close();
		        		} catch(Exception excp) {
		        			LOG.error("error while closing opened cache file '" + cacheFile.getAbsolutePath() + "'", excp);
		        		}
		        	}
		        }
	    	}
		} else {
			LOG.info("policies is null. Nothing to save in cache");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}
	}
}
