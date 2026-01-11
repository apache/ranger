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

package org.apache.ranger.admin.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;

import java.io.File;
import java.io.FileReader;

// this implementation loads policies, roles, tags, userstore and gds info from the given local filesystem paths:
//   {path}/{appId}_{serviceName}.json           -> policies
//   {path}/{appId}_{serviceName}_roles.json     -> roles
//   {path}/{appId}_{serviceName}_tag.json       -> tags
//   {path}/{appId}_{serviceName}_userstore.json -> userstore
//   {path}/{appId}_{serviceName}_gds.json       -> gds info
public class LocalFolderPolicySource extends AbstractRangerAdminClient {
    private ServicePolicies policies;
    private RangerRoles     roles;
    private RangerUserStore userStore;
    private ServiceTags     tags;
    private ServiceGdsInfo  gdsInfo;

    private String policiesPath;
    private String rolesPath;
    private String userStorePath;
    private String tagsPath;
    private String gdsInfoPath;
    private long   lastPoliciesFileModifiedTime  = -1;
    private long   lastRolesFileModifiedTime     = -1;
    private long   lastUserStoreFileModifiedTime = -1;
    private long   lastTagsFileModifiedTime      = -1;
    private long   lastGdsInfoFileModifiedTime   = -1;

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        super.init(serviceName, appId, configPropertyPrefix, config);

        String directory  = config.get(configPropertyPrefix + ".policy.source.local_folder.path");
        String pathPrefix = (directory == null ? "" : directory) + File.separator + appId + "_" + serviceName;

        this.policiesPath  = pathPrefix + ".json";
        this.rolesPath     = pathPrefix + "_roles.json";
        this.userStorePath = pathPrefix + "_userstore.json";
        this.tagsPath      = pathPrefix + "_tag.json";
        this.gdsInfoPath   = pathPrefix + "_gds.json";
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadPolicies();

        return (lastKnownVersion == -1 || policies == null || policies.getPolicyVersion() == null || !policies.getPolicyVersion().equals(lastKnownVersion)) ? policies : null;
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastKnownVersion, long lastActivationTimeInMills) throws Exception {
        loadRoles();

        return (lastKnownVersion == -1 || roles == null || roles.getRoleVersion() == null || !roles.getRoleVersion().equals(lastKnownVersion)) ? roles : null;
    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadTags();

        return (lastKnownVersion == -1 || tags == null || tags.getTagVersion() == null || !tags.getTagVersion().equals(lastKnownVersion)) ? tags : null;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadUserStore();

        return (lastKnownVersion == -1 || userStore == null || userStore.getUserStoreVersion() == null || !userStore.getUserStoreVersion().equals(lastKnownVersion)) ? userStore : null;
    }

    @Override
    public ServiceGdsInfo getGdsInfoIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        loadGdsInfo();

        return (lastKnownVersion == -1 || gdsInfo == null || gdsInfo.getGdsVersion() == null || !gdsInfo.getGdsVersion().equals(lastKnownVersion)) ? gdsInfo : null;
    }

    private void loadPolicies() throws Exception {
        File srcFile = new File(policiesPath);

        if (!srcFile.exists() || !srcFile.canRead()) {
            throw new Exception(policiesPath + ": policies file not found or not readable");
        }

        if (policies == null || srcFile.lastModified() != lastPoliciesFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                policies = gson.fromJson(reader, ServicePolicies.class);

                lastPoliciesFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadRoles() throws Exception {
        File srcFile = new File(rolesPath);

        if (!srcFile.exists() || !srcFile.canRead()) {
            throw new Exception(rolesPath + ": roles file not found or not readable");
        }

        if (roles == null || srcFile.lastModified() != lastRolesFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                roles = gson.fromJson(reader, RangerRoles.class);

                lastRolesFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadTags() throws Exception {
        File srcFile = new File(tagsPath);

        if (!srcFile.exists() || !srcFile.canRead()) {
            throw new Exception(tagsPath + ": tags file not found or not readable");
        }

        if (tags == null || srcFile.lastModified() != lastTagsFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                tags = gson.fromJson(reader, ServiceTags.class);

                lastTagsFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadUserStore() throws Exception {
        File srcFile = new File(userStorePath);

        if (!srcFile.exists() || !srcFile.canRead()) {
            throw new Exception(userStorePath + ": userStore file not found or not readable");
        }

        if (userStore == null || srcFile.lastModified() != lastUserStoreFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                userStore = gson.fromJson(reader, RangerUserStore.class);

                lastUserStoreFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadGdsInfo() throws Exception {
        File srcFile = new File(gdsInfoPath);

        if (!srcFile.exists() || !srcFile.canRead()) {
            throw new Exception(gdsInfoPath + ": gdsInfo file not found or not readable");
        }

        if (gdsInfo == null || srcFile.lastModified() != lastGdsInfoFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                gdsInfo = JsonUtilsV2.readValue(reader, ServiceGdsInfo.class);

                lastGdsInfoFileModifiedTime = srcFile.lastModified();
            }
        }
    }
}
