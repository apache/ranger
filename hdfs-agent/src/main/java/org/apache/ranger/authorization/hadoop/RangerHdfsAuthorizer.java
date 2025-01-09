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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerHdfsAuthorizer extends INodeAttributeProvider {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHdfsAuthorizer.class);

    public static final String KEY_FILENAME                             = "FILENAME";
    public static final String KEY_BASE_FILENAME                        = "BASE_FILENAME";
    public static final String DEFAULT_FILENAME_EXTENSION_SEPARATOR     = ".";
    public static final String KEY_RESOURCE_PATH                        = "path";
    public static final String RANGER_FILENAME_EXTENSION_SEPARATOR_PROP = "ranger.plugin.hdfs.filename.extension.separator";

    private final Path addlConfigFile;

    private RangerHdfsPlugin rangerPlugin;

    public RangerHdfsAuthorizer() {
        this(null);
    }

    public RangerHdfsAuthorizer(Path addlConfigFile) {
        LOG.debug("==> RangerHdfsAuthorizer.RangerHdfsAuthorizer()");

        this.addlConfigFile = addlConfigFile;

        LOG.debug("<== RangerHdfsAuthorizer.RangerHdfsAuthorizer()");
    }

    public void start() {
        LOG.debug("==> RangerHdfsAuthorizer.start()");

        RangerHdfsPlugin plugin = new RangerHdfsPlugin(addlConfigFile);

        plugin.init();

        if (plugin.isOptimizeSubAccessAuthEnabled()) {
            LOG.info("{} is enabled", RangerHadoopConstants.RANGER_OPTIMIZE_SUBACCESS_AUTHORIZATION_PROP);
        }

        LOG.info("Legacy way of authorizing sub-access requests will {}be used", plugin.isUseLegacySubAccessAuthorization() ? "" : "not ");

        rangerPlugin = plugin;

        LOG.debug("<== RangerHdfsAuthorizer.start()");
    }

    public void stop() {
        LOG.debug("==> RangerHdfsAuthorizer.stop()");

        RangerHdfsPlugin plugin = rangerPlugin;

        rangerPlugin = null;

        if (plugin != null) {
            plugin.cleanup();
        }

        LOG.debug("<== RangerHdfsAuthorizer.stop()");
    }

    @Override
    public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
        return inode; // return default attributes
    }

    @Override
    public INodeAttributes getAttributes(String[] pathElements, INodeAttributes inode) {
        return inode;
    }

    @Override
    public AccessControlEnforcer getExternalAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
        LOG.debug("==> RangerHdfsAuthorizer.getExternalAccessControlEnforcer()");

        RangerAccessControlEnforcer rangerAce = new RangerAccessControlEnforcer(rangerPlugin, defaultEnforcer);

        LOG.debug("<== RangerHdfsAuthorizer.getExternalAccessControlEnforcer()");

        return rangerAce;
    }

    // for testing
    public Configuration getConfig() {
        return rangerPlugin.getConfig();
    }
}
