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
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyRefresher extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(PolicyRefresher.class);

	private static final Logger PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");

	private final RangerBasePlugin               plugIn;
	private final String                         serviceType;
	private final String                         serviceName;
	private final RangerAdminClient              rangerAdmin;
	private final RangerRolesProvider            rolesProvider;
	private final long                           pollingIntervalMs;
	private final String                         cacheFileName;
	private final String                         cacheDir;
	private final BlockingQueue<DownloadTrigger> policyDownloadQueue = new LinkedBlockingQueue<>();
	private       Timer                          policyDownloadTimer;
	private       long                           lastKnownVersion    = -1L;
	private       long                           lastActivationTimeInMillis;
	private       boolean                        policiesSetInPlugin;
	private       boolean                        serviceDefSetInPlugin;


	public PolicyRefresher(RangerBasePlugin plugIn) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + plugIn.getServiceName() + ").PolicyRefresher()");
		}

		RangerPluginConfig pluginConfig   = plugIn.getConfig();
		String             propertyPrefix = pluginConfig.getPropertyPrefix();

		this.plugIn      = plugIn;
		this.serviceType = plugIn.getServiceType();
		this.serviceName = plugIn.getServiceName();
		this.cacheDir    = pluginConfig.get(propertyPrefix + ".policy.cache.dir");

		String appId         = StringUtils.isEmpty(plugIn.getAppId()) ? serviceType : plugIn.getAppId();
		String cacheFilename = String.format("%s_%s.json", appId, serviceName);

		cacheFilename = cacheFilename.replace(File.separatorChar,  '_');
		cacheFilename = cacheFilename.replace(File.pathSeparatorChar,  '_');

		this.cacheFileName = cacheFilename;

		RangerPluginContext pluginContext  = plugIn.getPluginContext();
		RangerAdminClient   adminClient    = pluginContext.getAdminClient();
		this.rangerAdmin                   = (adminClient != null) ? adminClient : pluginContext.createAdminClient(pluginConfig);
		this.rolesProvider                 = new RangerRolesProvider(getServiceType(), appId, getServiceName(), rangerAdmin,  cacheDir, pluginConfig);
		this.pollingIntervalMs             = pluginConfig.getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000L);

		setName("PolicyRefresher(serviceName=" + serviceName + ")-" + getId());

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

	public long getLastActivationTimeInMillis() {
		return lastActivationTimeInMillis;
	}

	public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
		this.lastActivationTimeInMillis = lastActivationTimeInMillis;
	}

	public void startRefresher() {
		loadRoles();
		loadPolicy();

		super.start();

		policyDownloadTimer = new Timer("policyDownloadTimer", true);

		try {
			policyDownloadTimer.schedule(new DownloaderTask(policyDownloadQueue), pollingIntervalMs, pollingIntervalMs);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Scheduled policyDownloadRefresher to download policies every " + pollingIntervalMs + " milliseconds");
			}
		} catch (IllegalStateException exception) {
			LOG.error("Error scheduling policyDownloadTimer:", exception);
			LOG.error("*** Policies will NOT be downloaded every " + pollingIntervalMs + " milliseconds ***");

			policyDownloadTimer = null;
		}

	}

	public void stopRefresher() {

		Timer policyDownloadTimer = this.policyDownloadTimer;

		this.policyDownloadTimer = null;

		if (policyDownloadTimer != null) {
			policyDownloadTimer.cancel();
		}

		if (super.isAlive()) {
			super.interrupt();

			boolean setInterrupted = false;
			boolean isJoined = false;

			while (!isJoined) {
				try {
					super.join();
					isJoined = true;
				} catch (InterruptedException excp) {
					LOG.warn("PolicyRefresher(serviceName=" + serviceName + "): error while waiting for thread to exit", excp);
					LOG.warn("Retrying Thread.join(). Current thread will be marked as 'interrupted' after Thread.join() returns");
					setInterrupted = true;
				}
			}
			if (setInterrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public void run() {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").run()");
		}

		while(true) {
			DownloadTrigger trigger = null;
			try {
				trigger = policyDownloadQueue.take();
				loadRoles();
				loadPolicy();
			} catch(InterruptedException excp) {
				LOG.info("PolicyRefresher(serviceName=" + serviceName + ").run(): interrupted! Exiting thread", excp);
				break;
			} finally {
				if (trigger != null) {
					trigger.signalCompletion();
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").run()");
		}
	}

	public void syncPoliciesWithAdmin(DownloadTrigger token) throws InterruptedException {
		policyDownloadQueue.put(token);
		token.waitForCompletion();
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
			}

			if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
				long freeMemory = Runtime.getRuntime().freeMemory();
				long totalMemory = Runtime.getRuntime().totalMemory();
				PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
			}

			if (svcPolicies != null) {
				plugIn.setPolicies(svcPolicies);
				policiesSetInPlugin = true;
				serviceDefSetInPlugin = false;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = svcPolicies.getPolicyVersion() != null ? svcPolicies.getPolicyVersion() : -1L;
			} else {
				if (!policiesSetInPlugin && !serviceDefSetInPlugin) {
					plugIn.setPolicies(null);
					serviceDefSetInPlugin = true;
				}
			}
		} catch (RangerServiceNotFoundException snfe) {
			if (!serviceDefSetInPlugin) {
				disableCache();
				plugIn.setPolicies(null);
				serviceDefSetInPlugin = true;
				setLastActivationTimeInMillis(System.currentTimeMillis());
				lastKnownVersion = -1;
			}
		} catch (Exception excp) {
			LOG.error("Encountered unexpected exception, ignoring..", excp);
		}

		RangerPerfTracer.log(perf);

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

				policies = JsonUtils.jsonToObject(reader, ServicePolicies.class);

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
	
	public void saveToCache(ServicePolicies policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}
		boolean doPreserveDeltas = plugIn.getConfig().getBoolean(plugIn.getConfig().getPropertyPrefix() + ".preserve.deltas", false);

		if(policies != null) {
			File cacheFile = null;
			File backupCacheFile = null;
			if (cacheDir != null) {
				String realCacheDirName = CollectionUtils.isNotEmpty(policies.getPolicyDeltas()) ? cacheDir + File.separator + "deltas" : cacheDir;
				String backupCacheFileName = cacheFileName + "_" + policies.getPolicyVersion();
				String realCacheFileName = CollectionUtils.isNotEmpty(policies.getPolicyDeltas()) ? backupCacheFileName : cacheFileName;

				// Create the cacheDir if it doesn't already exist
				File cacheDirTmp = new File(realCacheDirName);
				if (cacheDirTmp.exists()) {
					cacheFile =  new File(realCacheDirName + File.separator + realCacheFileName);
				} else {
					try {
						cacheDirTmp.mkdirs();
						cacheFile =  new File(realCacheDirName + File.separator + realCacheFileName);
					} catch (SecurityException ex) {
						LOG.error("Cannot create cache directory", ex);
					}
				}
				if (CollectionUtils.isEmpty(policies.getPolicyDeltas())) {
					backupCacheFile = new File(realCacheDirName + File.separator + backupCacheFileName);
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
					JsonUtils.objectToWriter(writer, policies);
		        } catch (Exception excp) {
		        	LOG.error("failed to save policies to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
		        } finally {
					if (writer != null) {
						try {
							writer.close();
							deleteOldestVersionCacheFileInCacheDirectory(cacheFile.getParentFile());
						} catch (Exception excp) {
							LOG.error("error while closing opened cache file '" + cacheFile.getAbsolutePath() + "'", excp);
						}
					}
				}

				RangerPerfTracer.log(perf);

	    	}

			if (doPreserveDeltas) {
				if (backupCacheFile != null) {

					RangerPerfTracer perf = null;

					if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "PolicyRefresher.saveToCache(serviceName=" + serviceName + ")");
					}

					try (Writer writer = new FileWriter(backupCacheFile)) {
						JsonUtils.objectToWriter(writer, policies);
					} catch (Exception excp) {
						LOG.error("failed to save policies to cache file '" + backupCacheFile.getAbsolutePath() + "'", excp);
					}

					RangerPerfTracer.log(perf);

				}
			}
		} else {
			LOG.info("policies is null. Nothing to save in cache");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").saveToCache()");
		}
	}

	private void deleteOldestVersionCacheFileInCacheDirectory(File cacheDirectory) {
		int maxVersionsToPreserve = plugIn.getConfig().getInt(plugIn.getConfig().getPropertyPrefix() + "max.versions.to.preserve", 1);
		FileFilter logFileFilter = (file) -> file.getName().matches(".+json_.+");

		File[] filesInParent = cacheDirectory.listFiles(logFileFilter);
		List<Long> policyVersions = new ArrayList<>();

		if (filesInParent != null && filesInParent.length > 0) {
			for (File f : filesInParent) {
				String fileName = f.getName();
				// Extract the part after json_
				int policyVersionIdx = fileName.lastIndexOf("json_");
				String policyVersionStr = fileName.substring(policyVersionIdx + 5);
				Long policyVersion = Long.valueOf(policyVersionStr);
				policyVersions.add(policyVersion);
			}
		} else {
			LOG.info("No files matching '.+json_*' found");
		}

		if (!policyVersions.isEmpty()) {
			policyVersions.sort(new Comparator<Long>() {
				@Override
				public int compare(Long o1, Long o2) {
					if (o1.equals(o2)) return 0;
					return o1 < o2 ? -1 : 1;
				}
			});
		}

		if (policyVersions.size() > maxVersionsToPreserve) {
			String fileName = this.cacheFileName + "_" + Long.toString(policyVersions.get(0));
			String pathName = cacheDirectory.getAbsolutePath() + File.separator + fileName;
			File toDelete = new File(pathName);
			if (toDelete.exists()) {
				boolean isDeleted = toDelete.delete();
				if (LOG.isDebugEnabled()) {
					LOG.debug("file :[" + pathName + "] is deleted" + isDeleted);
				}
			} else {
				LOG.info("File: " + pathName + " does not exist!");
			}
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

	private void loadRoles() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyRefresher(serviceName=" + serviceName + ").loadRoles()");
		}

		//Load the Ranger UserGroup Roles
		rolesProvider.loadUserGroupRoles(plugIn);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyRefresher(serviceName=" + serviceName + ").loadRoles()");
		}
	}
}
