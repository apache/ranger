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

import java.util.Map;
import java.util.Set;

public class GdsDipEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDipEvaluator.class);

    private final DatasetInProjectInfo            dip;
    private final RangerValidityScheduleEvaluator scheduleEvaluator;

    public GdsDipEvaluator(DatasetInProjectInfo dip) {
        this.dip = dip;

        if (dip.getValiditySchedule() != null) {
            scheduleEvaluator = new RangerValidityScheduleEvaluator(dip.getValiditySchedule());
        } else {
            scheduleEvaluator = null;
        }
    }

    public long getDatasetId() {
        return dip.getDatasetId();
    }

    public long getProjectId() {
        return dip.getProjectId();
    }

    public boolean isAllowed(RangerAccessRequest request) {
        boolean ret = isActive();

        if (ret) {
            // TODO:
        }

        return ret;
    }


    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Set<String> allowedAccessTypes, Map<Long, GdsProjectEvaluator> projects) {
        LOG.debug("==> GdsDipEvaluator.getResourceACLs({}, {})", request, acls);

        if (dip.getStatus() == RangerGds.GdsShareStatus.ACTIVE) {
            GdsProjectEvaluator evaluator = projects.get(dip.getProjectId());

            if (evaluator != null) {
                isConditional = isConditional || scheduleEvaluator != null;

                evaluator.getResourceACLs(request, acls, isConditional, allowedAccessTypes);
            } else {
                LOG.warn("GdsDipEvaluator.getResourceACLs({}): evaluator for projectId={} not found", request, dip.getProjectId());
            }
        }

        LOG.debug("<== GdsDipEvaluator.getResourceACLs({}, {})", request, acls);
    }


    private boolean isActive() {
        boolean ret = dip.getStatus() == RangerGds.GdsShareStatus.ACTIVE;

        if (ret && scheduleEvaluator != null) {
            ret = scheduleEvaluator.isApplicable(System.currentTimeMillis());
        }

        return ret;
    }
}
