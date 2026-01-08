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

package org.apache.ranger.authorization.seatunnel.authorizer;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.seatunnel.common.access.AccessDeniedException;
import org.apache.seatunnel.common.access.AccessInfo;
import org.apache.seatunnel.common.access.AccessType;
import org.apache.seatunnel.common.access.ResourceType;
import org.apache.seatunnel.common.access.SeatunnelAccessController;

public class RangerSeatunnelAuthorizer implements SeatunnelAccessController {
    private static final String RANGER_SEATUNNEL_SERVICETYPE = "seatunnel";
    private static final String RANGER_SEATUNNEL_APPID = "seatunnel";

    private final RangerBasePlugin rangerPlugin;

    public RangerSeatunnelAuthorizer() {
        rangerPlugin = new RangerBasePlugin(RANGER_SEATUNNEL_SERVICETYPE, RANGER_SEATUNNEL_APPID);
        rangerPlugin.init();
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public boolean hasPermission(String resourceName, ResourceType resourceType, AccessType accessType, AccessInfo accessInfo) {
        if ("default".equals(accessInfo.getWorkspaceName())
                && resourceType != ResourceType.WORKSPACE
                && resourceType != ResourceType.USER) {
            return true; // Allow all operations under default workspace
        }
        RangerSeatunnelResource rangerResource = new RangerSeatunnelResource(resourceName, resourceType, accessInfo.getWorkspaceName());
        RangerSeatunnelAccessRequest request = new RangerSeatunnelAccessRequest(rangerResource, accessType, accessInfo);
        RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
        return result != null && result.getIsAllowed();
    }


    @Override
    public void authorizeAccess(String resourceName, ResourceType resourceType, AccessType accessType, AccessInfo accessInfo) {
        if (!hasPermission(resourceName, resourceType, accessType, accessInfo)) {
            AccessDeniedException.accessDenied(accessInfo.getUsername(), resourceName, resourceType, accessType);
        }
    }
}