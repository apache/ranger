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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.gds.GdsAccessResult;
import org.apache.ranger.plugin.policyengine.gds.GdsPolicyEngine;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.DownloaderTask;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RangerGdsEnricher extends RangerAbstractContextEnricher {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsEnricher.class);

    public static final String REFRESHER_POLLINGINTERVAL_OPTION = "refresherPollingInterval";
    public static final String RETRIEVER_CLASSNAME_OPTION       = "retrieverClassName";

    private RangerGdsInfoRetriever gdsInfoRetriever;
    private RangerGdsInfoRefresher gdsInfoRefresher;
    private RangerServiceDefHelper serviceDefHelper;
    private GdsPolicyEngine        gdsPolicyEngine;

    @Override
    public void init() {
        LOG.debug("==> RangerGdsEnricher.init()");

        super.init();

        String propertyPrefix     = "ranger.plugin." + serviceDef.getName();
        String retrieverClassName = getOption(RETRIEVER_CLASSNAME_OPTION);
        long   pollingIntervalMs  = getLongOption(REFRESHER_POLLINGINTERVAL_OPTION, 60 * 1000L);
        String cacheFile          = null;

        serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);

        if (StringUtils.isNotBlank(retrieverClassName)) {
            try {
                @SuppressWarnings("unchecked")
                Class<RangerGdsInfoRetriever> retriverClass = (Class<RangerGdsInfoRetriever>) Class.forName(retrieverClassName);

                gdsInfoRetriever = retriverClass.newInstance();
            } catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException excp) {
                LOG.error("Failed to instantiate retriever (className={})", retrieverClassName, excp);
            }
        }

        if (gdsInfoRetriever != null) {
            String cacheDir      = getConfig(propertyPrefix + ".policy.cache.dir", null);
            String cacheFilename = String.format("%s_%s_gds.json", appId, serviceName);

            cacheFilename = cacheFilename.replace(File.separatorChar, '_');
            cacheFilename = cacheFilename.replace(File.pathSeparatorChar, '_');

            cacheFile = cacheDir == null ? null : (cacheDir + File.separator + cacheFilename);

            gdsInfoRetriever.setServiceName(serviceName);
            gdsInfoRetriever.setServiceDef(serviceDef);
            gdsInfoRetriever.setAppId(appId);
            gdsInfoRetriever.setPluginConfig(getPluginConfig());
            gdsInfoRetriever.setPluginContext(getPluginContext());
            gdsInfoRetriever.init(enricherDef.getEnricherOptions());

            gdsInfoRefresher = new RangerGdsInfoRefresher(gdsInfoRetriever, pollingIntervalMs, cacheFile, -1L);

            try {
                gdsInfoRefresher.populateGdsInfo();
            } catch (Throwable excp) {
                LOG.error("RangerGdsEnricher.init(): failed to retrieve gdsInfo", excp);
            }

            gdsInfoRefresher.setDaemon(true);
            gdsInfoRefresher.startRefresher();
        } else {
            LOG.error("No value specified for {} in the RangerGdsEnricher options", RETRIEVER_CLASSNAME_OPTION);
        }

        LOG.info("RangerGdsEnricher.init(): retrieverClassName={}, pollingIntervalMs={}, cacheFile={}", retrieverClassName, pollingIntervalMs, cacheFile);

        LOG.debug("<== RangerGdsEnricher.init()");
    }

    @Override
    public void enrich(RangerAccessRequest request, Object dataStore) {
        LOG.debug("==> RangerGdsEnricher.enrich({}, {})", request, dataStore);

        GdsPolicyEngine policyEngine = (dataStore instanceof GdsPolicyEngine) ? (GdsPolicyEngine) dataStore : this.gdsPolicyEngine;

        LOG.debug("RangerGdsEnricher.enrich(): using policyEngine={}", policyEngine);

        GdsAccessResult result = policyEngine != null ? policyEngine.evaluate(request) : null;

        RangerAccessRequestUtil.setGdsResultInContext(request, result);

        LOG.debug("<== RangerGdsEnricher.enrich({}, {})", request, dataStore);
    }

    @Override
    public boolean preCleanup() {
        LOG.debug("==> RangerGdsEnricher.preCleanup()");

        super.preCleanup();

        RangerGdsInfoRefresher gdsInfoRefresher = this.gdsInfoRefresher;

        this.gdsInfoRefresher = null;

        if (gdsInfoRefresher != null) {
            LOG.debug("Trying to clean up RangerGdsInfoRefresher({})", gdsInfoRefresher.getName());

            gdsInfoRefresher.cleanup();
        }

        LOG.debug("<== RangerGdsEnricher.preCleanup(): result={}", true);

        return true;
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        LOG.debug("==> RangerGdsEnricher.enrich({})", request);

        enrich(request, null);

        LOG.debug("<== RangerGdsEnricher.enrich({})", request);
    }

    public void setGdsInfo(ServiceGdsInfo gdsInfo) {
        this.gdsPolicyEngine = new GdsPolicyEngine(gdsInfo, serviceDefHelper, getPluginContext());

        setGdsInfoInPlugin();
    }

    public RangerServiceDefHelper getServiceDefHelper() {
        return serviceDefHelper;
    }

    public GdsPolicyEngine getGdsPolicyEngine() {
        return gdsPolicyEngine;
    }

    private void setGdsInfoInPlugin() {
        LOG.debug("==> setGdsInfoInPlugin()");

        RangerAuthContext authContext = getAuthContext();

        if (authContext != null) {
            authContext.addOrReplaceRequestContextEnricher(this, gdsPolicyEngine);

            notifyAuthContextChanged();
        }

        LOG.debug("<== setGdsInfoInPlugin()");
    }

    private class RangerGdsInfoRefresher extends Thread {
        private final RangerGdsInfoRetriever         retriever;
        private final long                           pollingIntervalMs;
        private final String                         cacheFile;
        private       Long                           lastKnownVersion;
        private       long                           lastActivationTimeInMillis;
        private       Timer                          downloadTimer;
        private       BlockingQueue<DownloadTrigger> downloadQueue;
        private       boolean                        gdsInfoSetInPlugin;

        public RangerGdsInfoRefresher(RangerGdsInfoRetriever retriever, long pollingIntervalMs, String cacheFile, long lastKnownVersion) {
            super("RangerGdsInfoRefresher");

            this.retriever         = retriever;
            this.pollingIntervalMs = pollingIntervalMs;
            this.cacheFile         = cacheFile;
            this.lastKnownVersion  = lastKnownVersion;
        }

        @Override
        public void run() {
            while (true) {
                BlockingQueue<DownloadTrigger> downloadQueue = this.downloadQueue;
                DownloadTrigger                trigger       = null;

                try {
                    if (downloadQueue == null) {
                        LOG.error("RangerGdsInfoRefresher(serviceName={}).run(): downloadQueue is null", serviceName);

                        break;
                    }

                    trigger = downloadQueue.take();

                    populateGdsInfo();
                } catch (InterruptedException excp) {
                    LOG.info("RangerGdsInfoRefresher(serviceName={}).run() interrupted! Exiting thread", serviceName, excp);
                    break;
                } catch (Exception excp) {
                    LOG.error("RangerGdsInfoRefresher(serviceName={}).run() failed to download gdsInfo. Will retry again", serviceName, excp);
                } finally {
                    if (trigger != null) {
                        trigger.signalCompletion();
                    }
                }
            }
        }

        final void startRefresher() {
            try {
                downloadTimer = new Timer("gdsInfoDownloadTimer", true);
                downloadQueue = new LinkedBlockingQueue<>();

                super.start();

                downloadTimer.schedule(new DownloaderTask(downloadQueue), pollingIntervalMs, pollingIntervalMs);

                LOG.debug("Scheduled timer to download gdsInfo every {} milliseconds", pollingIntervalMs);
            } catch (IllegalStateException exception) {
                LOG.error("Error scheduling gdsInfo download", exception);
                LOG.error("*** GdsInfo will NOT be downloaded every {} milliseconds ***", pollingIntervalMs);

                stopRefresher();
            }
        }

        void cleanup() {
            LOG.debug("==> RangerGdsInfoRefresher.cleanup()");

            stopRefresher();

            LOG.debug("<== RangerGdsInfoRefresher.cleanup()");
        }

        ServiceGdsInfo loadFromCache() {
            LOG.debug("==> RangerGdsInfoRefresher(serviceName={}).loadFromCache()", getServiceName());

            ServiceGdsInfo ret       = null;
            File           cacheFile = org.apache.commons.lang.StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

            if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
                try (Reader reader = new FileReader(cacheFile)) {
                    ret = JsonUtilsV2.readValue(reader, ServiceGdsInfo.class);
                } catch (Exception excp) {
                    LOG.error("failed to load gdsInfo from cache file {}", cacheFile.getAbsolutePath(), excp);
                }
            } else {
                LOG.warn("cache file does not exist or not readable '{}'", (cacheFile == null ? null : cacheFile.getAbsolutePath()));
            }

            LOG.debug("<== RangerGdsInfoRefresher(serviceName={}).loadFromCache()", getServiceName());

            return ret;
        }

        void saveToCache(ServiceGdsInfo gdsInfo) {
            LOG.debug("==> RangerGdsInfoRefresher(serviceName={}).saveToCache()", getServiceName());

            if (gdsInfo != null) {
                File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

                if (cacheFile != null) {
                    try (Writer writer = new FileWriter(cacheFile)) {
                        JsonUtilsV2.writeValue(writer, gdsInfo);
                    } catch (Exception excp) {
                        LOG.error("failed to save gdsInfo to cache file '{}'", cacheFile.getAbsolutePath(), excp);
                    }
                }
            } else {
                LOG.info("gdsInfo is null for service={}. Nothing to save in cache", getServiceName());
            }

            LOG.debug("<== RangerGdsInfoRefresher(serviceName={}).saveToCache()", getServiceName());
        }

        private void stopRefresher() {
            Timer downloadTimer = this.downloadTimer;

            this.downloadTimer = null;
            this.downloadQueue = null;

            if (downloadTimer != null) {
                downloadTimer.cancel();
            }

            if (super.isAlive()) {
                super.interrupt();

                boolean setInterrupted = false;
                boolean isJoined       = false;

                while (!isJoined) {
                    try {
                        super.join();

                        isJoined = true;

                        LOG.debug("RangerGdsInfoRefresher({}) is stopped", getName());
                    } catch (InterruptedException excp) {
                        LOG.warn("RangerGdsInfoRefresher({}).stopRefresher(): Error while waiting for thread to exit", getName(), excp);
                        LOG.warn("Retrying Thread.join(). Current thread will be marked as 'interrupted' after Thread.join() returns");

                        setInterrupted = true;
                    }
                }

                if (setInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void populateGdsInfo() throws Exception {
            ServiceGdsInfo gdsInfo = retriever.retrieveGdsInfo(lastKnownVersion != null ? lastKnownVersion : -1, lastActivationTimeInMillis);

            if (gdsInfo == null && !gdsInfoSetInPlugin) {
                gdsInfo = loadFromCache();
            }

            if (gdsInfo != null) {
                setGdsInfo(gdsInfo);
                saveToCache(gdsInfo);

                gdsInfoSetInPlugin         = true;
                lastKnownVersion           = gdsInfo.getGdsVersion();
                lastActivationTimeInMillis = System.currentTimeMillis();
            }
        }
    }
}
