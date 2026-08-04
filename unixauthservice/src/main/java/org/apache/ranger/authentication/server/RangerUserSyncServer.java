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

package org.apache.ranger.authentication.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.server.tomcat.EmbeddedServer;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RangerUserSyncServer extends EmbeddedServer {
    private static final Logger LOG           = LoggerFactory.getLogger(RangerUserSyncServer.class);
    private static final String CONFIG_PREFIX = "ranger.usersync.";

    public RangerUserSyncServer(Configuration configuration) {
        super(configuration, CONFIG_PREFIX);
    }

    public static void main(String[] args) {
        LOG.info("==>> RangerUserSyncServer.main()");
        try {
            Configuration config = UserGroupSyncConfig.getInstance().getUserGroupConfig();
            new RangerUserSyncServer(config).start();
        } catch (Throwable e) {
            LOG.error("Failed to initialize embedded server due to: ", e);
        }
        LOG.info("<<== RangerUserSyncServer.main()");
    }

    @Override
    protected String getConnectorServerBanner() {
        return "Apache Ranger Usersync";
    }

    @Override
    protected String getDefaultAccessLogPattern(String servername) {
        return "%h %l %u %t \"%r\" %s %b";
    }
}
