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
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInDatasetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class GdsDshidEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDshidEvaluator.class);

    private final        DataShareInDatasetInfo dshid;
    private final RangerValidityScheduleEvaluator scheduleEvaluator;

    public GdsDshidEvaluator(DataShareInDatasetInfo dshid) {
        this.dshid = dshid;

        if (dshid.getValiditySchedule() != null) {
            scheduleEvaluator = new RangerValidityScheduleEvaluator(dshid.getValiditySchedule());
        } else {
            scheduleEvaluator = null;
        }
    }

    public long getDataShareId() {
        return dshid.getDataShareId();
    }

    public long getDatasetId() {
        return dshid.getDatasetId();
    }

    public boolean isAllowed(RangerAccessRequest request) {
        boolean ret = isActive();

        if (ret) {
            // TODO:
        }

        return ret;
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Map<Long, GdsDatasetEvaluator> datasets, Map<Long, GdsProjectEvaluator> projects, Set<String> allowedAccessTypes) {
        LOG.debug("==> GdsDshidEvaluator.getResourceACLs({}, {})", request, acls);

        if (dshid.getStatus() == RangerGds.GdsShareStatus.ACTIVE) {
            GdsDatasetEvaluator datasetEvaluator = datasets.get(dshid.getDatasetId());

            if (datasetEvaluator != null) {
                isConditional = isConditional || scheduleEvaluator != null;

                datasetEvaluator.getResourceACLs(request, acls, isConditional, allowedAccessTypes, projects);
            } else {
                LOG.warn("GdsDshidEvaluator.getResourceACLs({}): datasetEvaluator for datasetId={} not found", request, dshid.getDatasetId());
            }
        }

        LOG.debug("<== GdsDshidEvaluator.getResourceACLs({}, {})", request, acls);
    }


    private boolean isActive() {
        boolean ret = dshid.getStatus() == RangerGds.GdsShareStatus.ACTIVE;

        if (ret && scheduleEvaluator != null) {
            ret = scheduleEvaluator.isApplicable(System.currentTimeMillis());
        }

        return ret;
    }
}
