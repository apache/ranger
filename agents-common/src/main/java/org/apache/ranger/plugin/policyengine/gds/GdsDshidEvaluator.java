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
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInDatasetInfo;

public class GdsDshidEvaluator {
    private final DataShareInDatasetInfo          dshid;
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


    private boolean isActive() {
        boolean ret = dshid.getStatus() == RangerGds.GdsShareStatus.ACTIVE;

        if (ret && scheduleEvaluator != null) {
            ret = scheduleEvaluator.isApplicable(System.currentTimeMillis());
        }

        return ret;
    }
}
