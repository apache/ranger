/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.storm;

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.authorization.storm.StormRangerPlugin.StormConstants.PluginConfiguration;
import org.apache.ranger.authorization.storm.StormRangerPlugin.StormConstants.ResourceName;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StormRangerPlugin extends RangerBasePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(StormRangerPlugin.class);

    private final Map<String, String> impliedAccessTypes;
    boolean initialized;

    public StormRangerPlugin() {
        super(PluginConfiguration.ServiceType, PluginConfiguration.AuditApplicationType);

        Map<String, String> impliedTypes = new HashMap<>();
        // In future this has to be part of Ranger Storm Service Def.
        impliedTypes.put("getTopologyPageInfo", "getTopologyInfo");
        impliedTypes.put("getComponentPageInfo", "getTopologyInfo");
        impliedTypes.put("setWorkerProfiler", "getTopologyInfo");
        impliedTypes.put("getWorkerProfileActionExpiry", "getTopologyInfo");
        impliedTypes.put("getComponentPendingProfileActions", "getTopologyInfo");
        impliedTypes.put("startProfiling", "getTopologyInfo");
        impliedTypes.put("stopProfiling", "getTopologyInfo");
        impliedTypes.put("dumpProfile", "getTopologyInfo");
        impliedTypes.put("dumpJstack", "getTopologyInfo");
        impliedTypes.put("dumpHeap", "getTopologyInfo");
        impliedTypes.put("setLogConfig", "getTopologyInfo");
        impliedTypes.put("getLogConfig", "getTopologyInfo");
        impliedTypes.put("debug", "getTopologyInfo");

        this.impliedAccessTypes = Collections.unmodifiableMap(impliedTypes);
    }

    // this method isn't expected to be invoked often.  Per knox design this would be invoked ONCE right after the authorizer servlet is loaded
    @Override
    public synchronized void init() {
        if (!initialized) {
            super.init();
            // One time call to register the audit hander with the policy engine.
            super.setResultProcessor(new RangerDefaultAuditHandler(getConfig()));
            // this needed to set things right in the nimbus process
            if (KerberosName.getRules() == null) {
                KerberosName.setRules("DEFAULT");
            }

            initialized = true;
            LOG.info("StormRangerPlugin initialized!");
        }
    }

    public RangerAccessRequest buildAccessRequest(String user, String[] groupArr, String clientIp, String topology, String operation) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(user);
        if (groupArr != null && groupArr.length > 0) {
            Set<String> groups = Sets.newHashSet(groupArr);
            request.setUserGroups(groups);
        }

        request.setAccessType(getAccessType(operation));
        request.setClientIPAddress(clientIp);
        request.setAction(operation);
        // build resource and connect stuff into request
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(ResourceName.Topology, topology);
        request.setResource(resource);

        LOG.debug("Returning request: {}", request);

        return request;
    }

    private String getAccessType(String operation) {
        String ret = impliedAccessTypes.get(operation);
        if (ret == null) {
            ret = operation;
        }

        return ret;
    }

    public static class StormConstants {
        // Plugin parameters
        static class PluginConfiguration {
            static final String ServiceType          = "storm";
            static final String AuditApplicationType = "storm";
        }

        // must match the corresponding string used in service definition file
        static class ResourceName {
            static final String Topology = "topology";
        }
    }
}
