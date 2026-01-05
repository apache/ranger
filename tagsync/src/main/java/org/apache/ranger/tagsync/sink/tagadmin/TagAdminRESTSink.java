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

package org.apache.ranger.tagsync.sink.tagadmin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TagAdminRESTSink implements TagSink, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TagAdminRESTSink.class);

    private static final String REST_PREFIX   = "/service";
    private static final String MODULE_PREFIX = "/tags";
    private static final String REST_URL_IMPORT_SERVICETAGS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/importservicetags/";

    List<NewCookie> cookieList = new ArrayList<>();

    private long                          rangerAdminConnectionCheckInterval;
    private Cookie                        sessionId;
    private boolean                       isValidRangerCookie;
    private boolean                       isRangerCookieEnabled;
    private String                        rangerAdminCookieName;
    private RangerRESTClient              tagRESTClient;
    private boolean                       isKerberized;
    private BlockingQueue<UploadWorkItem> uploadWorkItems;
    private Thread                        myThread;

    @Override
    public boolean initialize(Properties properties) {
        LOG.debug("==> TagAdminRESTSink.initialize()");

        boolean ret           = false;
        String  restUrl       = TagSyncConfig.getTagAdminRESTUrl(properties);
        String  sslConfigFile = TagSyncConfig.getTagAdminRESTSslConfigFile(properties);
        String  userName      = TagSyncConfig.getTagAdminUserName(properties);
        String  password      = TagSyncConfig.getTagAdminPassword(properties);

        rangerAdminConnectionCheckInterval = TagSyncConfig.getTagAdminConnectionCheckInterval(properties);
        isKerberized                       = TagSyncConfig.getTagsyncKerberosIdentity(properties) != null;
        isRangerCookieEnabled              = TagSyncConfig.isTagSyncRangerCookieEnabled(properties);
        rangerAdminCookieName              = TagSyncConfig.getRangerAdminCookieName(properties);
        sessionId                          = null;

        LOG.debug("restUrl={}", restUrl);
        LOG.debug("sslConfigFile={}", sslConfigFile);
        LOG.debug("userName={}", userName);
        LOG.debug("rangerAdminConnectionCheckInterval={}", rangerAdminConnectionCheckInterval);
        LOG.debug("isKerberized={}", isKerberized);

        if (StringUtils.isNotBlank(restUrl)) {
            tagRESTClient = new RangerRESTClient(restUrl, sslConfigFile, TagSyncConfig.getInstance());

            if (!isKerberized) {
                tagRESTClient.setBasicAuthInfo(userName, password);
            }

            // Build and cache REST client. This will catch any errors in building REST client up-front
            tagRESTClient.getClient();

            uploadWorkItems = new LinkedBlockingQueue<>();
            ret             = true;
        } else {
            LOG.error("No value specified for property 'ranger.tagsync.tagadmin.rest.url'!");
        }

        LOG.debug("<== TagAdminRESTSink.initialize(), result={}", ret);

        return ret;
    }

    @Override
    public ServiceTags upload(ServiceTags toUpload) throws Exception {
        LOG.debug("==> upload() ");

        UploadWorkItem uploadWorkItem = new UploadWorkItem(toUpload);

        uploadWorkItems.put(uploadWorkItem);

        // Wait until message is successfully delivered
        ServiceTags ret = uploadWorkItem.waitForUpload();

        LOG.debug("<== upload()");

        return ret;
    }

    @Override
    public boolean start() {
        myThread = new Thread(this);

        myThread.setDaemon(true);
        myThread.start();

        return true;
    }

    @Override
    public void stop() {
        if (myThread != null && myThread.isAlive()) {
            myThread.interrupt();
        }
    }

    @Override
    public void run() {
        LOG.debug("==> TagAdminRESTSink.run()");

        while (true) {
            if (TagSyncConfig.isTagSyncServiceActive()) {
                UploadWorkItem uploadWorkItem;

                try {
                    uploadWorkItem = uploadWorkItems.take();

                    ServiceTags toUpload = uploadWorkItem.getServiceTags();

                    boolean doRetry;

                    do {
                        doRetry = false;

                        try {
                            ServiceTags uploaded = doUpload(toUpload);

                            if (uploaded == null) { // Treat this as if an Exception is thrown by doUpload
                                doRetry = true;
                                TimeUnit.MILLISECONDS.sleep(rangerAdminConnectionCheckInterval);
                            } else {
                                // ServiceTags uploaded successfully
                                uploadWorkItem.uploadCompleted(uploaded);
                            }
                        } catch (InterruptedException interrupted) {
                            LOG.error("Caught exception..: ", interrupted);
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Exception exception) {
                            LOG.error("Upload failed, retrying...", exception);
                            doRetry = true;
                            TimeUnit.MILLISECONDS.sleep(rangerAdminConnectionCheckInterval);
                        }
                    } while (doRetry);
                } catch (InterruptedException exception) {
                    LOG.error("Interrupted..: ", exception);
                    Thread.currentThread().interrupt();
                    return;
                }
            } else {
                try {
                    TimeUnit.MILLISECONDS.sleep(rangerAdminConnectionCheckInterval);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for service to become active", e);
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private ServiceTags doUpload(ServiceTags serviceTags) throws Exception {
        if (isKerberized) {
            try {
                UserGroupInformation userGroupInformation = UserGroupInformation.getLoginUser();

                if (userGroupInformation != null) {
                    try {
                        userGroupInformation.checkTGTAndReloginFromKeytab();
                    } catch (IOException ioe) {
                        LOG.error("Error renewing TGT and relogin", ioe);

                        userGroupInformation = null;
                    }
                }

                if (userGroupInformation != null) {
                    LOG.debug("Using Principal = {}", userGroupInformation.getUserName());

                    return userGroupInformation.doAs((PrivilegedExceptionAction<ServiceTags>) () -> {
                        try {
                            return uploadServiceTags(serviceTags);
                        } catch (Exception e) {
                            LOG.error("Upload of service-tags failed with message ", e);
                            throw e;
                        }
                    });
                } else {
                    LOG.error("Failed to get UserGroupInformation.getLoginUser()");
                    return null; // This will cause retries !!!
                }
            } catch (Exception e) {
                LOG.error("Upload of service-tags failed with message ", e);
                throw e;
            }
        } else {
            return uploadServiceTags(serviceTags);
        }
    }

    private ServiceTags uploadServiceTags(ServiceTags serviceTags) throws Exception {
        LOG.debug("==> uploadServiceTags()");

        Response response;

        if (isRangerCookieEnabled) {
            response = uploadServiceTagsUsingCookie(serviceTags);
        } else {
            response = tagRESTClient.put(REST_URL_IMPORT_SERVICETAGS_RESOURCE, null, serviceTags);
        }

        if (response == null || response.getStatus() != HttpServletResponse.SC_NO_CONTENT) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("Upload of service-tags failed with message {}", resp.getMessage());

            if (response == null || resp.getHttpStatusCode() != HttpServletResponse.SC_BAD_REQUEST) {
                // NOT an application error
                String statusMsg = response == null ? "null" : String.valueOf(response.getStatus());
                throw new Exception("Upload of service-tags failed with response: " + statusMsg + ", message: " + resp.getMessage());
            }
        }

        LOG.debug("<== uploadServiceTags()");

        return serviceTags;
    }

    private Response uploadServiceTagsUsingCookie(ServiceTags serviceTags) {
        LOG.debug("==> uploadServiceTagCache()");

        Response clientResponse;
        if (sessionId != null && isValidRangerCookie) {
            clientResponse = tryWithCookie(serviceTags);
        } else {
            clientResponse = tryWithCred(serviceTags);
        }

        LOG.debug("<== uploadServiceTagCache()");

        return clientResponse;
    }

    private Response tryWithCred(ServiceTags serviceTags) {
        LOG.debug("==> tryWithCred");

        Response clientResponsebyCred = uploadTagsWithCred(serviceTags);

        if (clientResponsebyCred != null && clientResponsebyCred.getStatus() != HttpServletResponse.SC_NO_CONTENT && clientResponsebyCred.getStatus() != HttpServletResponse.SC_BAD_REQUEST && clientResponsebyCred.getStatus() != HttpServletResponse.SC_OK) {
            sessionId            = null;
            clientResponsebyCred = null;
        }

        LOG.debug("<== tryWithCred");

        return clientResponsebyCred;
    }

    private Response tryWithCookie(ServiceTags serviceTags) {
        Response clientResponsebySessionId = uploadTagsWithCookie(serviceTags);

        if (clientResponsebySessionId != null && clientResponsebySessionId.getStatus() != HttpServletResponse.SC_NO_CONTENT && clientResponsebySessionId.getStatus() != HttpServletResponse.SC_BAD_REQUEST && clientResponsebySessionId.getStatus() != HttpServletResponse.SC_OK) {
            sessionId                 = null;
            isValidRangerCookie       = false;
            clientResponsebySessionId = null;
        }

        return clientResponsebySessionId;
    }

    private synchronized Response uploadTagsWithCred(ServiceTags serviceTags) {
        if (sessionId == null) {
            tagRESTClient.resetClient();

            Response response = null;

            try {
                response = tagRESTClient.put(REST_URL_IMPORT_SERVICETAGS_RESOURCE, null, serviceTags);
            } catch (Exception e) {
                LOG.error("Failed to get response, Error is : {}", e.getMessage(), e);
            }

            if (response != null) {
                if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
                    // This will be handled by the status check
                } else if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                    LOG.warn("Credentials response from ranger is 401.");
                    sessionId = null; // Clear session on unauthorized
                    isValidRangerCookie = false;
                } else if (response.getStatus() == HttpServletResponse.SC_OK || response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
                    Cookie                 newCookie = null;
                    Map<String, NewCookie> cookieMap = response.getCookies();
                    if (cookieMap != null && cookieMap.containsKey(rangerAdminCookieName)) {
                        newCookie = cookieMap.get(rangerAdminCookieName);
                    }

                    if (sessionId == null || newCookie != null) {
                        sessionId = newCookie;
                        isValidRangerCookie = true;
                    } else {
                        if (response.getHeaders().get("Set-Cookie") != null) {
                            List<NewCookie> respCookieList = new ArrayList<>();
                            response.getHeaders().get("Set-Cookie").forEach(headerValue -> {
                                if (headerValue.toString().contains(rangerAdminCookieName)) {
                                    respCookieList.add(NewCookie.valueOf(headerValue.toString()));
                                }
                            });
                            // save cookie received from credentials session login
                            for (NewCookie cookie : respCookieList) {
                                if (cookie.getName().equalsIgnoreCase(rangerAdminCookieName)) {
                                    sessionId           = cookie.toCookie();
                                    isValidRangerCookie = true;
                                    break;
                                } else {
                                    isValidRangerCookie = false;
                                }
                            }
                        }
                    }
                }
            }

            return response;
        } else {
            Response clientResponsebySessionId = uploadTagsWithCookie(serviceTags);
            return clientResponsebySessionId;
        }
    }

    private Response uploadTagsWithCookie(ServiceTags serviceTags) {
        LOG.debug("==> uploadTagsWithCookie");

        Response response = null;

        try {
            response = tagRESTClient.put(REST_URL_IMPORT_SERVICETAGS_RESOURCE, serviceTags, sessionId);
        } catch (Exception e) {
            LOG.error("Failed to get response, Error is : {}", e.getMessage(), e);
        }

        if (response != null) {
            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                sessionId = null;
                isValidRangerCookie = false;
            } else if (response.getStatus() == HttpServletResponse.SC_NO_CONTENT || response.getStatus() == HttpServletResponse.SC_OK) {
                Cookie                 newCookie = null;
                Map<String, NewCookie> cookieMap = response.getCookies();
                if (cookieMap != null && cookieMap.containsKey(rangerAdminCookieName)) {
                    newCookie = cookieMap.get(rangerAdminCookieName);
                }

                if (sessionId == null || newCookie != null) {
                    sessionId = newCookie;
                    isValidRangerCookie = true;
                } else {
                    if (response.getHeaders().get("Set-Cookie") != null) {
                        List<NewCookie> respCookieList = new ArrayList<>();
                        response.getHeaders().get("Set-Cookie").forEach(headerValue -> {
                            if (headerValue.toString().contains(rangerAdminCookieName)) {
                                respCookieList.add(NewCookie.valueOf(headerValue.toString()));
                            }
                        });
                        // save cookie received from credentials session login
                        for (NewCookie respCookie : respCookieList) {
                            if (respCookie.getName().equalsIgnoreCase(rangerAdminCookieName)) {
                                if (!(sessionId.getValue().equalsIgnoreCase(respCookie.toCookie().getValue()))) {
                                    sessionId = respCookie.toCookie();
                                }

                                isValidRangerCookie = true;
                                break;
                            }
                        }
                    }
                }
            } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
                sessionId = null;
                isValidRangerCookie = false;
            }
        }

        LOG.debug("<== uploadTagsWithCookie");

        return response;
    }

    static class UploadWorkItem {
        private       ServiceTags                serviceTags;
        private final BlockingQueue<ServiceTags> uploadedServiceTags;

        UploadWorkItem(ServiceTags serviceTags) {
            setServiceTags(serviceTags);

            uploadedServiceTags = new ArrayBlockingQueue<>(1);
        }

        ServiceTags getServiceTags() {
            return serviceTags;
        }

        void setServiceTags(ServiceTags serviceTags) {
            this.serviceTags = serviceTags;
        }

        ServiceTags waitForUpload() throws InterruptedException {
            return uploadedServiceTags.take();
        }

        void uploadCompleted(ServiceTags uploaded) throws InterruptedException {
            // ServiceTags uploaded successfully
            uploadedServiceTags.put(uploaded);
        }
    }
}
