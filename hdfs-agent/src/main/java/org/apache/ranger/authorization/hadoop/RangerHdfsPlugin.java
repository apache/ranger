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

package org.apache.ranger.authorization.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.plugin.resourcematcher.RangerPathResourceMatcher;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;

public class RangerHdfsPlugin extends RangerBasePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsPlugin.class);

    private static String fileNameExtensionSeparator = RangerHdfsAuthorizer.DEFAULT_FILENAME_EXTENSION_SEPARATOR;

    private final boolean     authzOptimizationEnabled;
    private final boolean     hadoopAuthEnabled;
    private final boolean     optimizeSubAccessAuthEnabled;
    private final String      randomizedWildcardPathName;
    private final String      hadoopModuleName;
    private final Set<String> excludeUsers = new HashSet<>();
    private final boolean     useLegacySubAccessAuthorization;

    public RangerHdfsPlugin(Path addlConfigFile) {
        super("hdfs", "hdfs");

        RangerPluginConfig config = getConfig();

        if (addlConfigFile != null) {
            config.addResource(addlConfigFile);
        }

        String random = generateString("^&#@!%()-_+=@:;'<>`~abcdefghijklmnopqrstuvwxyz01234567890");

        RangerHdfsPlugin.fileNameExtensionSeparator = config.get(RangerHdfsAuthorizer.RANGER_FILENAME_EXTENSION_SEPARATOR_PROP, RangerHdfsAuthorizer.DEFAULT_FILENAME_EXTENSION_SEPARATOR);

        this.hadoopAuthEnabled        = config.getBoolean(RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_PROP, RangerHadoopConstants.RANGER_ADD_HDFS_PERMISSION_DEFAULT);
        this.authzOptimizationEnabled = config.getBoolean("ranger.hdfs.authz.enable.optimization", false);

        config.setIsFallbackSupported(this.hadoopAuthEnabled);

        this.optimizeSubAccessAuthEnabled = config.getBoolean(RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_PROP, RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_DEFAULT);
        this.randomizedWildcardPathName   = RangerPathResourceMatcher.WILDCARD_ASTERISK + random + RangerPathResourceMatcher.WILDCARD_ASTERISK;
        this.hadoopModuleName             = config.get(RangerHadoopConstants.AUDITLOG_HADOOP_MODULE_ACL_NAME_PROP, RangerHadoopConstants.DEFAULT_HADOOP_MODULE_ACL_NAME);

        String excludeUserList = config.get(RangerHadoopConstants.AUDITLOG_HDFS_EXCLUDE_LIST_PROP, RangerHadoopConstants.AUDITLOG_EMPTY_STRING);

        this.useLegacySubAccessAuthorization = config.getBoolean(RangerHadoopConstants.RANGER_USE_LEGACY_SUBACCESS_AUTHORIZATION_PROP, RangerHadoopConstants.RANGER_USE_LEGACY_SUBACCESS_AUTHORIZATION_DEFAULT);

        if (excludeUserList != null && !excludeUserList.trim().isEmpty()) {
            for (String excludeUser : excludeUserList.trim().split(",")) {
                excludeUser = excludeUser.trim();

                LOG.debug("Adding exclude user [{}]", excludeUser);

                excludeUsers.add(excludeUser);
            }
        }

        LOG.info("AUTHZ_OPTIMIZATION_ENABLED:[{}]", authzOptimizationEnabled);
    }

    public static String getFileNameExtensionSeparator() {
        return fileNameExtensionSeparator;
    }

    public boolean isAuthzOptimizationEnabled() {
        return authzOptimizationEnabled;
    }

    public boolean isHadoopAuthEnabled() {
        return hadoopAuthEnabled;
    }

    public boolean isOptimizeSubAccessAuthEnabled() {
        return optimizeSubAccessAuthEnabled;
    }

    public String getRandomizedWildcardPathName() {
        return randomizedWildcardPathName;
    }

    public String getHadoopModuleName() {
        return hadoopModuleName;
    }

    public Set<String> getExcludedUsers() {
        return excludeUsers;
    }

    public boolean isUseLegacySubAccessAuthorization() {
        return useLegacySubAccessAuthorization;
    }

    // Build random string of length between 56 and 112 characters
    private static String generateString(String source) {
        SecureRandom rng   = new SecureRandom();
        byte[]       bytes = new byte[1];

        rng.nextBytes(bytes);

        int length = bytes[0];

        length = length < 56 ? 56 : length;
        length = length > 112 ? 112 : length;

        char[] text = new char[length];

        for (int i = 0; i < length; i++) {
            text[i] = source.charAt(rng.nextInt(source.length()));
        }

        return new String(text);
    }
}
