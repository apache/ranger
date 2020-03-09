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

package org.apache.ranger.plugin.contextenricher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.DownloaderTask;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;

import java.io.File;
import java.io.Reader;
import java.io.Writer;
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RangerUserStoreEnricher extends RangerAbstractContextEnricher {
    private static final Log LOG = LogFactory.getLog(RangerUserStoreEnricher.class);

    private static final Log PERF_SET_USERSTORE_LOG      = RangerPerfTracer.getPerfLogger("userstoreenricher.setuserstore");


    private static final String USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION = "userStoreRefresherPollingInterval";
    private static final String USERSTORE_RETRIEVER_CLASSNAME_OPTION       = "userStoreRetrieverClassName";

    private RangerUserStoreRefresher                 userStoreRefresher;
    private RangerUserStoreRetriever                 userStoreRetriever;
    private RangerUserStore							 rangerUserStore;
    private boolean                                  disableCacheIfServiceNotFound = true;

    private final BlockingQueue<DownloadTrigger>     userStoreDownloadQueue = new LinkedBlockingQueue<>();
    private Timer                                    userStoreDownloadTimer;

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerUserStoreEnricher.init()");
        }

        super.init();

        String userStoreRetrieverClassName = getOption(USERSTORE_RETRIEVER_CLASSNAME_OPTION);

        long pollingIntervalMs = getLongOption(USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION, 3600 * 1000);

        if (StringUtils.isNotBlank(userStoreRetrieverClassName)) {

            try {
                @SuppressWarnings("unchecked")
                Class<RangerUserStoreRetriever> userStoreRetriverClass = (Class<RangerUserStoreRetriever>) Class.forName(userStoreRetrieverClassName);

                userStoreRetriever = userStoreRetriverClass.newInstance();

            } catch (ClassNotFoundException exception) {
                LOG.error("Class " + userStoreRetrieverClassName + " not found, exception=" + exception);
            } catch (ClassCastException exception) {
                LOG.error("Class " + userStoreRetrieverClassName + " is not a type of RangerUserStoreRetriever, exception=" + exception);
            } catch (IllegalAccessException exception) {
                LOG.error("Class " + userStoreRetrieverClassName + " illegally accessed, exception=" + exception);
            } catch (InstantiationException exception) {
                LOG.error("Class " + userStoreRetrieverClassName + " could not be instantiated, exception=" + exception);
            }

            if (userStoreRetriever != null) {
                String propertyPrefix    = "ranger.plugin." + serviceDef.getName();
                disableCacheIfServiceNotFound = getBooleanConfig(propertyPrefix + ".disable.cache.if.servicenotfound", true);
                String cacheDir      = getConfig(propertyPrefix + ".policy.cache.dir", null);
                String cacheFilename = String.format("%s_%s_userstore.json", appId, serviceName);

                cacheFilename = cacheFilename.replace(File.separatorChar,  '_');
                cacheFilename = cacheFilename.replace(File.pathSeparatorChar,  '_');

                String cacheFile = cacheDir == null ? null : (cacheDir + File.separator + cacheFilename);

                userStoreRetriever.setServiceName(serviceName);
                userStoreRetriever.setServiceDef(serviceDef);
                userStoreRetriever.setAppId(appId);
                userStoreRetriever.setPluginConfig(getPluginConfig());
                userStoreRetriever.init(enricherDef.getEnricherOptions());

                userStoreRefresher = new RangerUserStoreRefresher(userStoreRetriever, this, -1L, userStoreDownloadQueue, cacheFile);

                try {
                    userStoreRefresher.populateUserStoreInfo();
                } catch (Throwable exception) {
                    LOG.error("Exception when retrieving userstore information for this enricher", exception);
                }

                userStoreRefresher.setDaemon(true);
                userStoreRefresher.startRefresher();

                userStoreDownloadTimer = new Timer("userStoreDownloadTimer", true);

                try {
                    userStoreDownloadTimer.schedule(new DownloaderTask(userStoreDownloadQueue), pollingIntervalMs, pollingIntervalMs);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Scheduled userStoreDownloadRefresher to download userstore every " + pollingIntervalMs + " milliseconds");
                    }
                } catch (IllegalStateException exception) {
                    LOG.error("Error scheduling userStoreDownloadTimer:", exception);
                    LOG.error("*** UserStore information will NOT be downloaded every " + pollingIntervalMs + " milliseconds ***");
                    userStoreDownloadTimer = null;
                }
            }
        } else {
            LOG.error("No value specified for " + USERSTORE_RETRIEVER_CLASSNAME_OPTION + " in the RangerUserStoreEnricher options");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerUserStoreEnricher.init()");
        }
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerUserStoreEnricher.enrich(" + request + ")");
        }

        enrich(request, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerUserStoreEnricher.enrich(" + request + ")");
        }
    }

    @Override
    public void enrich(RangerAccessRequest request, Object dataStore) {

        // Unused by Solr plugin as document level authorization gets RangerUserStore from AuthContext
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerUserStoreEnricher.enrich(" + request + ") with dataStore:[" + dataStore + "]");
        }
        final RangerUserStore rangerUserStore;

        if (dataStore instanceof RangerUserStore) {
            rangerUserStore = (RangerUserStore) dataStore;
        } else {
            rangerUserStore = this.rangerUserStore;

            if (dataStore != null) {
                LOG.warn("Incorrect type of dataStore :[" + dataStore.getClass().getName() + "], falling back to original enrich");
            }
        }

        RangerAccessRequestUtil.setRequestUserStoreInContext(request.getContext(), rangerUserStore);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerUserStoreEnricher.enrich(" + request + ") with dataStore:[" + dataStore + "])");
        }
    }

    public RangerUserStore getRangerUserStore() {return this.rangerUserStore;}

    public void setRangerUserStore(final RangerUserStore rangerUserStore) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerUserStoreEnricher.setRangerUserStore(rangerUserStore=" + rangerUserStore + ")");
        }

        if (rangerUserStore == null) {
            LOG.info("UserStore information is null for service " + serviceName);
            this.rangerUserStore = null;
        } else  {
            RangerPerfTracer perf = null;

            if(RangerPerfTracer.isPerfTraceEnabled(PERF_SET_USERSTORE_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_SET_USERSTORE_LOG, "RangerUserStoreEnricher.setRangerUserStore(newUserStoreVersion=" + rangerUserStore.getUserStoreVersion() + ")");
            }

            this.rangerUserStore = rangerUserStore;
            RangerPerfTracer.logAlways(perf);
        }

        setRangerUserStoreInPlugin();
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerUserStoreEnricher.setRangerUserStore(rangerUserStore=" + rangerUserStore + ")");
        }

    }

    @Override
    public boolean preCleanup() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerUserStoreEnricher.preCleanup()");
        }

        super.preCleanup();

        if (userStoreDownloadTimer != null) {
            userStoreDownloadTimer.cancel();
            userStoreDownloadTimer = null;
        }

        if (userStoreRefresher != null) {
            userStoreRefresher.cleanup();
            userStoreRefresher = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerUserStoreEnricher.preCleanup() : result=" + true);
        }
        return true;
    }

    private void setRangerUserStoreInPlugin() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> setRangerUserStoreInPlugin()");
        }

        RangerAuthContext authContext = getAuthContext();

        if (authContext != null) {
            authContext.addOrReplaceRequestContextEnricher(this, rangerUserStore);

            notifyAuthContextChanged();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== setRangerUserStoreInPlugin()");
        }
    }

    static class RangerUserStoreRefresher extends Thread {
        private static final Log LOG = LogFactory.getLog(RangerUserStoreRefresher.class);
        private static final Log PERF_REFRESHER_INIT_LOG = RangerPerfTracer.getPerfLogger("userstore.init");

        //private final RangerAdminClient adminClient;
        private final RangerUserStoreRetriever userStoreRetriever;
        private final RangerUserStoreEnricher userStoreEnricher;
        private long lastKnownVersion;
        private final BlockingQueue<DownloadTrigger> userStoreDownloadQueue;
        private long lastActivationTimeInMillis;

        private final String cacheFile;
        private boolean hasProvidedUserStoreToReceiver;
        private Gson gson;

        RangerUserStoreRefresher(RangerUserStoreRetriever userStoreRetriever, RangerUserStoreEnricher userStoreEnricher,
                                 long lastKnownVersion, BlockingQueue<DownloadTrigger> userStoreDownloadQueue,
                                 String cacheFile) {
            this.userStoreRetriever = userStoreRetriever;
            this.userStoreEnricher = userStoreEnricher;
            this.lastKnownVersion = lastKnownVersion;
            this.userStoreDownloadQueue = userStoreDownloadQueue;
            this.cacheFile = cacheFile;
            try {
                gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
            } catch(Throwable excp) {
                LOG.fatal("failed to create GsonBuilder object", excp);
            }
        }

        public long getLastActivationTimeInMillis() {
            return lastActivationTimeInMillis;
        }

        public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
            this.lastActivationTimeInMillis = lastActivationTimeInMillis;
        }

        @Override
        public void run() {

            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerUserStoreRefresher().run()");
            }

            while (true) {

                try {
                    RangerPerfTracer perf = null;

                    if(RangerPerfTracer.isPerfTraceEnabled(PERF_REFRESHER_INIT_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_REFRESHER_INIT_LOG,
                                "RangerUserStoreRefresher.run(lastKnownVersion=" + lastKnownVersion + ")");
                    }
                    DownloadTrigger trigger = userStoreDownloadQueue.take();
                    populateUserStoreInfo();
                    trigger.signalCompletion();

                    RangerPerfTracer.log(perf);

                } catch (InterruptedException excp) {
                    LOG.debug("RangerUserStoreRefresher().run() : interrupted! Exiting thread", excp);
                    break;
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerUserStoreRefresher().run()");
            }
        }

        private void populateUserStoreInfo() throws InterruptedException {

            RangerUserStore rangerUserStore = null;
            if (userStoreEnricher != null) {
                try {
                    rangerUserStore = userStoreRetriever.retrieveUserStoreInfo(lastKnownVersion, lastActivationTimeInMillis);

                    if (rangerUserStore == null) {
                        if (!hasProvidedUserStoreToReceiver) {
                            rangerUserStore = loadFromCache();
                        }
                    }

                    if (rangerUserStore != null) {
                        userStoreEnricher.setRangerUserStore(rangerUserStore);
                        if (rangerUserStore.getUserStoreVersion() != -1L) {
                            saveToCache(rangerUserStore);
                        }
                        LOG.info("RangerUserStoreRefresher.populateUserStoreInfo() - Updated userstore-cache to new version, lastKnownVersion=" + lastKnownVersion + "; newVersion="
                                + (rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion()));
                        hasProvidedUserStoreToReceiver = true;
                        lastKnownVersion = rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion();
                        setLastActivationTimeInMillis(System.currentTimeMillis());
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("RangerUserStoreRefresher.populateUserStoreInfo() - No need to update userstore-cache. lastKnownVersion=" + lastKnownVersion);
                        }
                    }
                } catch (RangerServiceNotFoundException snfe) {
                    LOG.error("Caught ServiceNotFound exception :", snfe);

                    // Need to clean up local userstore cache
                    if (userStoreEnricher.disableCacheIfServiceNotFound) {
                        disableCache();
                        setLastActivationTimeInMillis(System.currentTimeMillis());
                        lastKnownVersion = -1L;
                    }
                } catch (InterruptedException interruptedException) {
                    throw interruptedException;
                } catch (Exception e) {
                    LOG.error("Encountered unexpected exception. Ignoring", e);
                }
            } else {
                LOG.error("RangerUserStoreRefresher.populateUserStoreInfo() - no userstore receiver to update userstore-cache");
            }
        }

        public void cleanup() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerUserStoreRefresher.cleanup()");
            }

            stopRefresher();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerUserStoreRefresher.cleanup()");
            }
        }

        public void startRefresher() {
            try {
                super.start();
            } catch (Exception excp) {
                LOG.error("RangerUserStoreRefresher.startRetriever() - failed to start, exception=" + excp);
            }
        }

        public void stopRefresher() {

            if (super.isAlive()) {
                super.interrupt();

                try {
                    super.join();
                } catch (InterruptedException excp) {
                    LOG.error("RangerUserStoreRefresher.stopRefresher(): error while waiting for thread to exit", excp);
                }
            }
        }


        private RangerUserStore loadFromCache() {
            RangerUserStore rangerUserStore = null;

            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerUserStoreRefreher.loadFromCache()");
            }

            File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

            if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
                Reader reader = null;

                try {
                    reader = new FileReader(cacheFile);

                    rangerUserStore = gson.fromJson(reader, RangerUserStore.class);

                } catch (Exception excp) {
                    LOG.error("failed to load userstore information from cache file " + cacheFile.getAbsolutePath(), excp);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (Exception excp) {
                            LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
                        }
                    }
                }
            } else {
                LOG.warn("cache file does not exist or not readable '" + (cacheFile == null ? null : cacheFile.getAbsolutePath()) + "'");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerUserStoreRefreher.loadFromCache()");
            }

            return rangerUserStore;
        }

        public void saveToCache(RangerUserStore rangerUserStore) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerUserStoreRefreher.saveToCache()");
            }

            if (rangerUserStore != null) {
                File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

                if (cacheFile != null) {
                    Writer writer = null;

                    try {
                        writer = new FileWriter(cacheFile);

                        gson.toJson(rangerUserStore, writer);
                    } catch (Exception excp) {
                        LOG.error("failed to save userstore information to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
                    } finally {
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (Exception excp) {
                                LOG.error("error while closing opened cache file '" + cacheFile.getAbsolutePath() + "'", excp);
                            }
                        }
                    }
                }
            } else {
                LOG.info("userstore information is null. Nothing to save in cache");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerUserStoreRefreher.saveToCache()");
            }
        }

        private void disableCache() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerUserStoreRefreher.disableCache()");
            }

            File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);
            if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
                LOG.warn("Cleaning up local userstore cache");
                String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();
                if (!cacheFile.renameTo(new File(renamedCacheFile))) {
                    LOG.error("Failed to move " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
                } else {
                    LOG.warn("moved " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No local userstore cache found. No need to disable it!");
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RangerUserStoreRefreher.disableCache()");
            }
        }
    }

}