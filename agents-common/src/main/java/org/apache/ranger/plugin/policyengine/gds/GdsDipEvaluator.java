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

package org.apache.ranger.plugin.policyengine.gds;

import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DatasetInProjectInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class GdsDipEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDipEvaluator.class);

    private final DatasetInProjectInfo            dip;
    private final GdsProjectEvaluator             projectEvaluator;
    private final RangerValidityScheduleEvaluator scheduleEvaluator;

    public GdsDipEvaluator(DatasetInProjectInfo dip, GdsProjectEvaluator projectEvaluator) {
        this.dip              = dip;
        this.projectEvaluator = projectEvaluator;

        if (dip.getValiditySchedule() != null) {
            scheduleEvaluator = new RangerValidityScheduleEvaluator(dip.getValiditySchedule());
        } else {
            scheduleEvaluator = null;
        }
    }

    public Long getDatasetId() {
        return dip.getDatasetId();
    }

    public Long getProjectId() {
        return dip.getProjectId();
    }

    public GdsProjectEvaluator getProjectEvaluator() {
        return projectEvaluator;
    }

    public boolean isActive() {
        boolean ret = dip.getStatus() == RangerGds.GdsShareStatus.ACTIVE;

        if (ret && scheduleEvaluator != null) {
            ret = scheduleEvaluator.isApplicable(System.currentTimeMillis());
        }

        return ret;
    }

    public boolean isAllowed(RangerAccessRequest request) {
        boolean ret = isActive();

        if (ret) {
            // TODO:
        }

        return ret;
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Set<String> allowedAccessTypes) {
        LOG.debug("==> GdsDipEvaluator.getResourceACLs({}, {})", request, acls);

        isConditional = isConditional || scheduleEvaluator != null;

        projectEvaluator.getResourceACLs(request, acls, isConditional, allowedAccessTypes);

        LOG.debug("<== GdsDipEvaluator.getResourceACLs({}, {})", request, acls);
    }
}
