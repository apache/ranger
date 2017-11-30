/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.hdfs;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A test implementation of the RangerAdminClient interface that just reads policies in from a file and returns them
 */
public class RangerAdminClientImpl implements RangerAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminClientImpl.class);
    private final static String cacheFilename = "hdfs-policies.json";
    private final static String tagFilename = "hdfs-policies-tag.json";
    private Gson gson;

    public void init(String serviceName, String appId, String configPropertyPrefix) {
        Gson gson = null;
        try {
            gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        } catch(Throwable excp) {
            LOG.error("RangerAdminClientImpl: failed to create GsonBuilder object", excp);
        }
        this.gson = gson;
    }

    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
	    String hdfsVersion = RangerConfiguration.getInstance().get("hdfs.version", "");

        final String relativePath;
        if (StringUtils.isNotBlank(hdfsVersion)) {
            relativePath = "/src/test/resources/" + hdfsVersion + "/";
        } else {
            relativePath = "/src/test/resources/";
        }

        java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, relativePath + cacheFilename);
        byte[] cacheBytes = Files.readAllBytes(cachePath);

        return gson.fromJson(new String(cacheBytes), ServicePolicies.class);
    }

    public void grantAccess(GrantRevokeRequest request) throws Exception {

    }

    public void revokeAccess(GrantRevokeRequest request) throws Exception {

    }

    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        String hdfsVersion = RangerConfiguration.getInstance().get("hdfs.version", "");

        final String relativePath;
        if (StringUtils.isNotBlank(hdfsVersion)) {
            relativePath = "/src/test/resources/" + hdfsVersion + "/";
        } else {
            relativePath = "/src/test/resources/";
        }
        java.nio.file.Path cachePath = FileSystems.getDefault().getPath(basedir, relativePath + tagFilename);

        byte[] cacheBytes = Files.readAllBytes(cachePath);

        return gson.fromJson(new String(cacheBytes), ServiceTags.class);
    }

    public List<String> getTagTypes(String tagTypePattern) throws Exception {
        return null;
    }


}
