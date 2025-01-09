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

package org.apache.ranger.authorization.storm.authorizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.storm.StormRangerPlugin;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

public class RangerStormAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerStormAuthorizer.class);

    private static final Logger PERF_STORMAUTH_REQUEST_LOG       = RangerPerfTracer.getPerfLogger("stormauth.request");
    private static final String STORM_CLIENT_JASS_CONFIG_SECTION = "StormClient";
    static final Set<String> noAuthzOperations                   = Sets.newHashSet("getNimbusConf", "getClusterInfo");

    private static volatile StormRangerPlugin plugin;

    /**
     * Invoked once immediately after construction
     *
     * @param aStormConfigMap Storm configuration
     */

    @Override
    public void prepare(Map aStormConfigMap) {
        StormRangerPlugin me = plugin;

        if (me == null) {
            synchronized (RangerStormAuthorizer.class) {
                me = plugin;

                if (me == null) {
                    try {
                        MiscUtil.setUGIFromJAASConfig(STORM_CLIENT_JASS_CONFIG_SECTION);
                        LOG.info("LoginUser={}", MiscUtil.getUGILoginUser());
                    } catch (Throwable t) {
                        LOG.error("Error while setting UGI for Storm Plugin...", t);
                    }

                    LOG.info("Creating StormRangerPlugin");

                    plugin = new StormRangerPlugin();
                    plugin.init();
                }
            }
        }
    }

    /**
     * permit() method is invoked for each incoming Thrift request.
     *
     * @param aRequestContext request context includes info about
     * @param aOperationName operation name
     * @param aTopologyConfigMap configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */

    @Override
    public boolean permit(ReqContext aRequestContext, String aOperationName, Map aTopologyConfigMap) {
        boolean accessAllowed  = false;
        boolean isAuditEnabled = false;
        String topologyName    = null;
        RangerPerfTracer perf  = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_STORMAUTH_REQUEST_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_STORMAUTH_REQUEST_LOG, "RangerStormAuthorizer.permit()");
            }

            topologyName = (aTopologyConfigMap == null ? "" : (String) aTopologyConfigMap.get(Config.TOPOLOGY_NAME));

            LOG.debug("[req {}] Access  from: [{}] user: [{}], op:   [{}],topology: [{}]", aRequestContext.requestID(), aRequestContext.remoteAddress(), aRequestContext.principal(), aOperationName, topologyName);

            if (aTopologyConfigMap != null) {
                for (Object keyObj : aTopologyConfigMap.keySet()) {
                    Object valObj = aTopologyConfigMap.get(keyObj);
                    LOG.debug("TOPOLOGY CONFIG MAP [{}] => [{}]", keyObj, valObj);
                }
            } else {
                LOG.debug("TOPOLOGY CONFIG MAP is passed as null.");
            }

            if (noAuthzOperations.contains(aOperationName)) {
                accessAllowed = true;
            } else if (plugin == null) {
                LOG.info("Ranger plugin not initialized yet! Skipping authorization;  allowedFlag => [{}], Audit Enabled:{}", false, false);
            } else {
                String   userName = null;
                String[] groups   = null;

                Principal user = aRequestContext.principal();

                if (user != null) {
                    userName = user.getName();
                    if (userName != null) {
                        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName);
                        userName = ugi.getShortUserName();
                        groups   = ugi.getGroupNames();
                        if (LOG.isDebugEnabled()) { // used due to performance reasons.
                            LOG.debug("User found from principal [{}] => user:[{}], groups:[{}]", user.getName(), userName, StringUtil.toString(groups));
                        }
                    }
                }

                if (userName != null) {
                    String              clientIp      = (aRequestContext.remoteAddress() == null ? null : aRequestContext.remoteAddress().getHostAddress());
                    RangerAccessRequest accessRequest = plugin.buildAccessRequest(userName, groups, clientIp, topologyName, aOperationName);
                    RangerAccessResult  result        = plugin.isAccessAllowed(accessRequest);
                    accessAllowed  = result != null && result.getIsAllowed();
                    isAuditEnabled = result != null && result.getIsAudited();
                    if (LOG.isDebugEnabled()) { // used due to performance reasons.
                        LOG.debug("User found from principal [{}], groups [{}]: verifying using [{}], allowedFlag => [{}], Audit Enabled:{}", userName, StringUtil.toString(groups), plugin.getClass().getName(), accessAllowed, isAuditEnabled);
                    }
                } else {
                    LOG.info("NULL User found from principal [{}]: Skipping authorization;  allowedFlag => [{}], Audit Enabled:{}", user, false, false);
                }
            }
        } catch (Throwable t) {
            LOG.error("RangerStormAuthorizer found this exception", t);
        } finally {
            RangerPerfTracer.log(perf);
            LOG.debug("[req {}] Access  from: [{}] user: [{}], op:   [{}],topology: [{}] => returns [{}], Audit Enabled:{}", aRequestContext.requestID(), aRequestContext.remoteAddress(), aRequestContext.principal(), aOperationName, topologyName, accessAllowed, isAuditEnabled);
        }

        return accessAllowed;
    }
}
