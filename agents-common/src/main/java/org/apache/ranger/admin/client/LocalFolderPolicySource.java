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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;

// this implementation loads policies, roles, tags, userstore and gds info from the given local filesystem paths:
//   policies:  {path}/{serviceName}.json           or {path}/{appId}_{serviceName}.json
//   roles:     {path}/{serviceName}_roles.json     or {path}/{appId}_{serviceName}_roles.json
//   tags:      {path}/{serviceName}_tag.json       or {path}/{appId}_{serviceName}_tag.json
//   userstore: {path}/{serviceName}_userstore.json or {path}/{appId}_{serviceName}_userstore.json
//   gds:       {path}/{serviceName}_gds.json       or {path}/{appId}_{serviceName}_gds.json
public class LocalFolderPolicySource extends RangerPolicySource {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFolderPolicySource.class);

    private String prefix;
    private String prefixWithAppId;

    private ServicePolicies policies;
    private RangerRoles     roles;
    private RangerUserStore userStore;
    private ServiceTags     tags;
    private ServiceGdsInfo  gdsInfo;

    private long lastPoliciesFileModifiedTime  = -1;
    private long lastRolesFileModifiedTime     = -1;
    private long lastUserStoreFileModifiedTime = -1;
    private long lastTagsFileModifiedTime      = -1;
    private long lastGdsInfoFileModifiedTime   = -1;

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
        super.init(serviceName, appId, configPropertyPrefix, config);

        String directory = config.get(configPropertyPrefix + ".policy.source.local_folder.path");

        if (StringUtils.isBlank(directory)) {
            directory = "";
        } else if (!directory.endsWith(File.separator)) {
            directory += File.separator;
        }

        prefix          = directory + serviceName;
        prefixWithAppId = StringUtils.isBlank(appId) ? null : (directory + appId + "_" + serviceName);
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
        File srcFile = getSourceFile(SUFFIX_POLICIES_FILE);

        if (policies == null || srcFile.lastModified() != lastPoliciesFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                policies = gson.fromJson(reader, ServicePolicies.class);

                lastPoliciesFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadRoles() throws Exception {
        File srcFile = getSourceFile(SUFFIX_ROLES_FILE);

        if (roles == null || srcFile.lastModified() != lastRolesFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                roles = gson.fromJson(reader, RangerRoles.class);

                lastRolesFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadTags() throws Exception {
        File srcFile = getSourceFile(SUFFIX_TAG_FILE);

        if (tags == null || srcFile.lastModified() != lastTagsFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                tags = gson.fromJson(reader, ServiceTags.class);

                lastTagsFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadUserStore() throws Exception {
        File srcFile = getSourceFile(SUFFIX_USERSTORE_FILE);

        if (userStore == null || srcFile.lastModified() != lastUserStoreFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                userStore = gson.fromJson(reader, RangerUserStore.class);

                lastUserStoreFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private void loadGdsInfo() throws Exception {
        File srcFile = getSourceFile(SUFFIX_GDS_FILE);

        if (gdsInfo == null || srcFile.lastModified() != lastGdsInfoFileModifiedTime) {
            try (FileReader reader = new FileReader(srcFile)) {
                gdsInfo = JsonUtilsV2.readValue(reader, ServiceGdsInfo.class);

                lastGdsInfoFileModifiedTime = srcFile.lastModified();
            }
        }
    }

    private File getSourceFile(String suffix) throws Exception {
        if (StringUtils.isBlank(prefix)) {
            throw new Exception(LocalFolderPolicySource.class.getName() + ": not initialized");
        }

        File    src        = new File(prefix + suffix);
        boolean isReadable = src.exists() && src.canRead();

        if (!isReadable) {
            if (StringUtils.isNotBlank(prefixWithAppId)) {
                src        = new File(prefixWithAppId + suffix);
                isReadable = src.exists() && src.canRead();
            }
        }

        if (!isReadable) {
            LOG.error("{}{}: file not found or not readable", prefix, suffix);

            throw new Exception(prefix + suffix + ": file not found or not readable");
        }

        return src;
    }
}
