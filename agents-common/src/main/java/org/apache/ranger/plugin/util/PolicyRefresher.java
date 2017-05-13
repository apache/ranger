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
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class PolicyRefresher extends Thread {
	private static final Log LOG = LogFactory.getLog(PolicyRefresher.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");

	private final RangerBasePlugin  plugIn;
	private final String            serviceType;
	private final String            serviceName;
	private final RangerAdminClient rangerAdmin;
	private final String            cacheFileName;
	private final String            cacheDir;
	private final Gson              gson;
	private final boolean           disableCacheIfServiceNotFound;

	private long 	pollingIntervalMs   = 30 * 1000;
	private long 	lastKnownVersion    = -1L;
	private long	lastActivationTimeInMillis;
	private boolean policiesSetInPlugin;
	private boolean serviceDefSetInPlugin;

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

		this.cacheFileName = cacheFilename;
		this.cacheDir = cacheDir;

		Gson gson = null;
		try {
			gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
		} catch(Throwable excp) {
			LOG.fatal("PolicyRefresher(): failed to create GsonBuilder object", excp);
		}
		this.gson = gson;

		String propertyPrefix    = "ranger.plugin." + serviceType;
		disableCacheIfServiceNotFound = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".disable.cache.if.servicenotfound", true);

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

	public long getLastActivationTimeInMillis() {
		return lastActivationTimeInMillis;
	}

	public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
		this.lastActivationTimeInMillis = lastActivationTimeInMillis;
	}

	public void startRefresher() {

		loadPolicy();

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
			loadPolicy();
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

	private void loadPolicy() {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadPolicy()");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadPolicy(serviceName=" + serviceName + ")");
			long freeMemory = Runtime.getRuntime().freeMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory-freeMemory) + ", Free memory:" + freeMemory);
		}

		try {
			//load policy from PolicyAdmin
			ServicePolicies svcPolicies = loadPolicyfromPolicyAdmin();

			if (svcPolicies == null) {
				//if Policy fetch from Policy Admin Fails, load from cache
				if (!policiesSetInPlugin) {
					svcPolicies = loadFromCache();
				}
			} else {
				saveToCache(svcPolicies);
			}

			RangerPerfTracer.log(perf);

			if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
				long freeMemory = Runtime.getRuntime().freeMemory();
				long totalMemory = Runtime.getRuntime().totalMemory();
				PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
			}

			if (svcPolicies != null) {
				plugIn.setPolicies(svcPolicies);
				policiesSetInPlugin = true;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = svcPolicies.getPolicyVersion();
			} else {
				if (!policiesSetInPlugin && !serviceDefSetInPlugin) {
					plugIn.setPolicies(null);
					serviceDefSetInPlugin = true;
				}
			}
		} catch (RangerServiceNotFoundException snfe) {
			if (disableCacheIfServiceNotFound) {
				disableCache();
				plugIn.setPolicies(null);
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = -1;
				serviceDefSetInPlugin = true;
			}
		} catch (Exception excp) {
			LOG.error("Encountered unexpected exception, ignoring..", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadPolicy()");
		}
	}

	private ServicePolicies loadPolicyfromPolicyAdmin() throws RangerServiceNotFoundException {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadPolicyfromPolicyAdmin()");
		}

		ServicePolicies svcPolicies = null;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadPolicyFromPolicyAdmin(serviceName=" + serviceName + ")");
		}

		try {
			svcPolicies = rangerAdmin.getServicePoliciesIfUpdated(lastKnownVersion, lastActivationTimeInMillis);

			boolean isUpdated = svcPolicies != null;

			if(isUpdated) {
				long newVersion = svcPolicies.getPolicyVersion() == null ? -1 : svcPolicies.getPolicyVersion().longValue();

				if(!StringUtils.equals(serviceName, svcPolicies.getServiceName())) {
					LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): ignoring unexpected serviceName '" + svcPolicies.getServiceName() + "' in service-store");

					svcPolicies.setServiceName(serviceName);
				}

				LOG.info("PolicyRefresher(serviceName=" + serviceName + "): found updated version. lastKnownVersion=" + lastKnownVersion + "; newVersion=" + newVersion);

			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("PolicyRefresher(serviceName=" + serviceName + ").run(): no update found. lastKnownVersion=" + lastKnownVersion);
				}
			}
		} catch (RangerServiceNotFoundException snfe) {
			LOG.error("PolicyRefresher(serviceName=" + serviceName + "): failed to find service. Will clean up local cache of policies (" + lastKnownVersion + ")", snfe);
			throw snfe;
		} catch (Exception excp) {
			LOG.error("PolicyRefresher(serviceName=" + serviceName + "): failed to refresh policies. Will continue to use last known version of policies (" + lastKnownVersion + ")", excp);
			svcPolicies = null;
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadPolicyfromPolicyAdmin()");
		 }

		 return svcPolicies;
	}


	private ServicePolicies loadFromCache() {

		ServicePolicies policies = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

    	if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
    		Reader reader = null;

    		RangerPerfTracer perf = null;

    		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
    			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.loadFromCache(serviceName=" + serviceName + ")");
    		}

    		try {
	        	reader = new FileReader(cacheFile);

		        policies = gson.fromJson(reader, ServicePolicies.class);

		        if(policies != null) {
		        	if(!StringUtils.equals(serviceName, policies.getServiceName())) {
		        		LOG.warn("ignoring unexpected serviceName '" + policies.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

		        		policies.setServiceName(serviceName);
		        	}

		        	lastKnownVersion = policies.getPolicyVersion() == null ? -1 : policies.getPolicyVersion().longValue();
		         }
	        } catch (Exception excp) {
	        	LOG.error("failed to load policies from cache file " + cacheFile.getAbsolutePath(), excp);
	        } finally {
	        	RangerPerfTracer.log(perf);

	        	if(reader != null) {
	        		try {
	        			reader.close();
	        		} catch(Exception excp) {
	        			LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
	        		}
	        	}
	        }
		} else {
			LOG.warn("cache file does not exist or not readable '" + (cacheFile == null ? null : cacheFile.getAbsolutePath()) + "'");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadFromCache()");
		}

		return policies;
	}
	
	private void saveToCache(ServicePolicies policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}

		if(policies != null) {
			File cacheFile = null;
			if (cacheDir != null) {
				// Create the cacheDir if it doesn't already exist
				File cacheDirTmp = new File(cacheDir);
				if (cacheDirTmp.exists()) {
					cacheFile =  new File(cacheDir + File.separator + cacheFileName);
				} else {
					try {
						cacheDirTmp.mkdirs();
						cacheFile =  new File(cacheDir + File.separator + cacheFileName);
					} catch (SecurityException ex) {
						LOG.error("Cannot create cache directory", ex);
					}
				}
			}
			
	    	if(cacheFile != null) {

				RangerPerfTracer perf = null;

				if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.saveToCache(serviceName=" + serviceName + ")");
				}

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

				RangerPerfTracer.log(perf);

	    	}
		} else {
			LOG.info("policies is null. Nothing to save in cache");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}
	}

	private void disableCache() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher.disableCache(serviceName=" + serviceName + ")");
		}

		File cacheFile = cacheDir == null ? null : new File(cacheDir + File.separator + cacheFileName);

		if(cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
			LOG.warn("Cleaning up local cache");
			String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();
			if (!cacheFile.renameTo(new File(renamedCacheFile))) {
				LOG.error("Failed to move " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			} else {
				LOG.warn("Moved " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No local policy cache found. No need to disable it!");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher.disableCache(serviceName=" + serviceName + ")");
		}
	}
}
