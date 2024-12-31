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

package org.apache.ranger.ha;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.ha.HAConfiguration.ZookeeperProperties;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ActiveInstanceState {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveInstanceState.class);

    private final Configuration  configuration;
    private final CuratorFactory curatorFactory;

    /**
     * Create a new instance of {@link ActiveInstanceState}.
     *
     * @param configuration an instance of {@link Configuration} created from
     * configuration
     * @param curatorFactory an instance of {@link CuratorFactory} to get the
     * {@link InterProcessReadWriteLock}
     */
    public ActiveInstanceState(Configuration configuration, CuratorFactory curatorFactory) {
        this.configuration  = configuration;
        this.curatorFactory = curatorFactory;
    }

    /**
     * Update state of the active server instance.
     * <p>
     * This method writes this instance's Server Address to a shared node in
     * Zookeeper. This information is used by other passive instances to locate the
     * current active server.
     *
     * @param serverId ID of this server instance
     * @throws Exception
     */
    public void update(String serverId) throws Exception {
        try {
            CuratorFramework    client                     = curatorFactory.clientInstance();
            ZookeeperProperties zookeeperProperties        = HAConfiguration.getZookeeperProperties(configuration);
            String              rangerServiceServerAddress = HAConfiguration.getBoundAddressForId(configuration, serverId);

            LOG.info("rangerServiceServerAddress : {}", rangerServiceServerAddress);

            List<ACL> acls      = new ArrayList<>();
            ACL       parsedACL = ZookeeperSecurityProperties.parseAcl(zookeeperProperties.getAcl(), ZooDefs.Ids.OPEN_ACL_UNSAFE.get(0));

            acls.add(parsedACL);

            // adding world read permission
            if (StringUtils.isNotEmpty(zookeeperProperties.getAcl())) {
                ACL worldReadPermissionACL = new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"));

                acls.add(worldReadPermissionACL);
            }

            Stat serverInfo = client.checkExists().forPath(getZnodePath(zookeeperProperties));

            if (serverInfo == null) {
                client.create().withMode(CreateMode.EPHEMERAL).withACL(acls).forPath(getZnodePath(zookeeperProperties));
            } else {
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                if (sessionId != serverInfo.getEphemeralOwner()) {
                    throw new Exception("Not a leader forces it to rejoin the election ");
                }
            }

            client.setData().forPath(getZnodePath(zookeeperProperties), rangerServiceServerAddress.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new Exception("ActiveInstanceState.update resulted in exception. forPath: getZnodePath ", e);
        }
    }

    /**
     * Retrieve state of the active server instance.
     * <p>
     * This method reads the active server location from the shared node in
     * Zookeeper.
     *
     * @return the active server's address and port of form http://host-or-ip:port
     */
    public String getActiveServerAddress() {
        CuratorFramework client        = curatorFactory.clientInstance();
        String           serverAddress = null;

        try {
            ZookeeperProperties zookeeperProperties = HAConfiguration.getZookeeperProperties(configuration);
            byte[]              bytes               = client.getData().forPath(getZnodePath(zookeeperProperties));

            serverAddress = new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOG.error("Error getting active server address", e);
        }

        return serverAddress;
    }

    private String getZnodePath(ZookeeperProperties zookeeperProperties) {
        return zookeeperProperties.getZkRoot() + HAConfiguration.RANGER_SERVICE_ACTIVE_SERVER_INFO;
    }
}
