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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.ClosedByInterruptException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class RangerUserStoreRefresher extends Thread {
    private static final Logger LOG                     = LoggerFactory.getLogger(RangerUserStoreRefresher.class);
    private static final Logger PERF_REFRESHER_INIT_LOG = RangerPerfTracer.getPerfLogger("userstore.init");

    private final RangerUserStoreRetriever       userStoreRetriever;
    private final RangerUserStoreEnricher        userStoreEnricher;
    private final BlockingQueue<DownloadTrigger> userStoreDownloadQueue;
    private final String                         cacheFile;
    private       long                           lastKnownVersion;
    private       long                           lastActivationTimeInMillis;
    private       boolean                        hasProvidedUserStoreToReceiver;
    private final RangerRESTClient               rangerRESTClient;

    public RangerUserStoreRefresher(RangerUserStoreRetriever userStoreRetriever, RangerUserStoreEnricher userStoreEnricher, RangerRESTClient restClient, long lastKnownVersion, BlockingQueue<DownloadTrigger> userStoreDownloadQueue, String cacheFile) {
        this.userStoreRetriever     = userStoreRetriever;
        this.userStoreEnricher      = userStoreEnricher;
        this.rangerRESTClient       = restClient;
        this.lastKnownVersion       = lastKnownVersion;
        this.userStoreDownloadQueue = userStoreDownloadQueue;
        this.cacheFile              = cacheFile;

        setName("RangerUserStoreRefresher(serviceName=" + userStoreRetriever.getServiceName() + ")-" + getId());
    }

    public long getLastActivationTimeInMillis() {
        return lastActivationTimeInMillis;
    }

    public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
        this.lastActivationTimeInMillis = lastActivationTimeInMillis;
    }

    @Override
    public void run() {
        LOG.debug("==> RangerUserStoreRefresher().run()");

        while (true) {
            DownloadTrigger trigger = null;

            try {
                RangerPerfTracer perf = null;

                if (RangerPerfTracer.isPerfTraceEnabled(PERF_REFRESHER_INIT_LOG)) {
                    perf = RangerPerfTracer.getPerfTracer(PERF_REFRESHER_INIT_LOG, "RangerUserStoreRefresher.run(lastKnownVersion=" + lastKnownVersion + ")");
                }

                trigger = userStoreDownloadQueue.take();

                populateUserStoreInfo();

                RangerPerfTracer.log(perf);
            } catch (InterruptedException excp) {
                LOG.debug("RangerUserStoreRefresher().run() : interrupted! Exiting thread", excp);

                break;
            } finally {
                if (trigger != null) {
                    trigger.signalCompletion();
                }
            }
        }

        LOG.debug("<== RangerUserStoreRefresher().run()");
    }

    public RangerUserStore populateUserStoreInfo() throws InterruptedException {
        RangerUserStore rangerUserStore = null;

        if (userStoreEnricher != null && userStoreRetriever != null) {
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

                    LOG.info("RangerUserStoreRefresher.populateUserStoreInfo() - Updated userstore-cache to new version, lastKnownVersion={}; newVersion={}",
                            lastKnownVersion, (rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion()));

                    hasProvidedUserStoreToReceiver = true;
                    lastKnownVersion               = rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion();

                    setLastActivationTimeInMillis(System.currentTimeMillis());
                } else {
                    LOG.debug("RangerUserStoreRefresher.populateUserStoreInfo() - No need to update userstore-cache. lastKnownVersion={}", lastKnownVersion);
                }
            } catch (RangerServiceNotFoundException snfe) {
                LOG.error("Caught ServiceNotFound exception :", snfe);

                // Need to clean up local userstore cache
                if (userStoreEnricher.isDisableCacheIfServiceNotFound()) {
                    disableCache();
                    setLastActivationTimeInMillis(System.currentTimeMillis());

                    lastKnownVersion = -1L;
                }
            } catch (InterruptedException interruptedException) {
                throw interruptedException;
            } catch (Exception e) {
                LOG.error("Encountered unexpected exception. Ignoring", e);
            }
        } else if (rangerRESTClient != null) {
            LOG.debug("RangerUserStoreRefresher.populateUserStoreInfo() for Ranger Raz");

            try {
                rangerUserStore = retrieveUserStoreInfo();

                if (rangerUserStore == null) {
                    if (!hasProvidedUserStoreToReceiver) {
                        rangerUserStore = loadFromCache();
                    }
                }

                if (rangerUserStore != null) {
                    if (rangerUserStore.getUserStoreVersion() != -1L) {
                        saveToCache(rangerUserStore);
                    }

                    LOG.info("RangerUserStoreRefresher.populateUserStoreInfo() - Updated userstore-cache for raz to new version, lastKnownVersion={}; newVersion={}",
                            lastKnownVersion, (rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion()));

                    hasProvidedUserStoreToReceiver = true;
                    lastKnownVersion               = rangerUserStore.getUserStoreVersion() == null ? -1L : rangerUserStore.getUserStoreVersion();

                    setLastActivationTimeInMillis(System.currentTimeMillis());
                } else {
                    LOG.debug("RangerUserStoreRefresher.populateUserStoreInfo() - No need to update userstore-cache for raz. lastKnownVersion={}", lastKnownVersion);
                }
            } catch (InterruptedException interruptedException) {
                throw interruptedException;
            } catch (Exception e) {
                LOG.error("Encountered unexpected exception. Ignoring", e);
            }
        } else {
            LOG.error("RangerUserStoreRefresher.populateUserStoreInfo() - no userstore receiver to update userstore-cache");
        }

        return rangerUserStore;
    }

    public void cleanup() {
        LOG.debug("==> RangerUserStoreRefresher.cleanup()");

        stopRefresher();

        LOG.debug("<== RangerUserStoreRefresher.cleanup()");
    }

    public void startRefresher() {
        try {
            super.start();
        } catch (Exception excp) {
            LOG.error("RangerUserStoreRefresher.startRetriever() - failed to start, exception={}", excp.toString());
        }
    }

    public void stopRefresher() {
        if (super.isAlive()) {
            super.interrupt();

            boolean setInterrupted = false;
            boolean isJoined       = false;

            while (!isJoined) {
                try {
                    super.join();

                    isJoined = true;
                } catch (InterruptedException excp) {
                    LOG.warn("RangerUserStoreRefresher(): Error while waiting for thread to exit", excp);
                    LOG.warn("Retrying Thread.join(). Current thread will be marked as 'interrupted' after Thread.join() returns");

                    setInterrupted = true;
                }
            }

            if (setInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void saveToCache(RangerUserStore rangerUserStore) {
        LOG.debug("==> RangerUserStoreRefreher.saveToCache()");

        if (rangerUserStore != null) {
            File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

            if (cacheFile != null) {
                Writer writer = null;

                try {
                    writer = new FileWriter(cacheFile);

                    JsonUtils.objectToWriter(writer, rangerUserStore);
                } catch (Exception excp) {
                    LOG.error("failed to save userstore information to cache file '{}'", cacheFile.getAbsolutePath(), excp);
                } finally {
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (Exception excp) {
                            LOG.error("error while closing opened cache file '{}'", cacheFile.getAbsolutePath(), excp);
                        }
                    }
                }
            }
        } else {
            LOG.info("userstore information is null. Nothing to save in cache");
        }

        LOG.debug("<== RangerUserStoreRefreher.saveToCache()");
    }

    private RangerUserStore loadFromCache() {
        RangerUserStore rangerUserStore = null;

        LOG.debug("==> RangerUserStoreRefreher.loadFromCache()");

        File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

        if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
            Reader reader = null;

            try {
                reader = new FileReader(cacheFile);

                rangerUserStore = JsonUtils.jsonToObject(reader, RangerUserStore.class);
            } catch (Exception excp) {
                LOG.error("failed to load userstore information from cache file {}", cacheFile.getAbsolutePath(), excp);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception excp) {
                        LOG.error("error while closing opened cache file {}", cacheFile.getAbsolutePath(), excp);
                    }
                }
            }
        } else {
            LOG.warn("cache file does not exist or not readable '{}'", (cacheFile == null ? null : cacheFile.getAbsolutePath()));
        }

        LOG.debug("<== RangerUserStoreRefreher.loadFromCache()");

        return rangerUserStore;
    }

    private void disableCache() {
        LOG.debug("==> RangerUserStoreRefreher.disableCache()");

        File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

        if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
            LOG.warn("Cleaning up local userstore cache");

            String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();

            if (!cacheFile.renameTo(new File(renamedCacheFile))) {
                LOG.error("Failed to move {} to {}", cacheFile.getAbsolutePath(), renamedCacheFile);
            } else {
                LOG.warn("moved {} to {}", cacheFile.getAbsolutePath(), renamedCacheFile);
            }
        } else {
            LOG.debug("No local userstore cache found. No need to disable it!");
        }

        LOG.debug("<== RangerUserStoreRefreher.disableCache()");
    }

    private RangerUserStore retrieveUserStoreInfo() throws Exception {
        RangerUserStore rangerUserStore = null;

        try {
            rangerUserStore = getUserStoreIfUpdated(lastKnownVersion, lastActivationTimeInMillis);
        } catch (ClosedByInterruptException closedByInterruptException) {
            LOG.error("UserStore-retriever for raz thread was interrupted while blocked on I/O");

            throw new InterruptedException();
        } catch (Exception e) {
            LOG.error("UserStore-retriever for raz encounterd exception, exception=", e);
            LOG.error("Returning null userstore info");
        }

        return rangerUserStore;
    }

    private RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion, long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerUserStoreRefreher.getUserStoreIfUpdated({}, {})", lastKnownUserStoreVersion, lastActivationTimeInMillis);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
        final ClientResponse       response;
        final Map<String, String>  queryParams = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_USERSTORE_VERSION, Long.toString(lastKnownUserStoreVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));

        if (isSecureMode) {
            LOG.debug("Checking UserStore updated as user : {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE;

                    return rangerRESTClient.get(relativeURL, queryParams);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            LOG.debug("Checking UserStore updated as user : {}", user);

            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE;

            response = rangerRESTClient.get(relativeURL, queryParams);
        }

        final RangerUserStore ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("Error getting UserStore; Received NULL response!!. secureMode={}, user={}", isSecureMode, user);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.debug("No change in UserStore. secureMode={}, user={}, response={}, lastKnownUserStoreVersion={}, lastActivationTimeInMillis={}",
                        isSecureMode, user, resp, lastKnownUserStoreVersion, lastActivationTimeInMillis);
            }

            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, RangerUserStore.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting UserStore; service not found. secureMode={}, user={}, response={}, lastKnownUserStoreVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), lastKnownUserStoreVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting UserStore. secureMode={}, user={}, response={}", isSecureMode, user, resp);

            ret = null;
        }

        LOG.debug("<== RangerUserStoreRefreher.getUserStoreIfUpdated({}, {}): ", lastKnownUserStoreVersion, lastActivationTimeInMillis);

        return ret;
    }
}
