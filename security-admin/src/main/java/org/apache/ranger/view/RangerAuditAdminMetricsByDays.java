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

package org.apache.ranger.view;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.common.AppConstants;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuditAdminMetricsByDays implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public static final Map<Integer, String> TYPE_TO_KEY;

    static {
        Map<Integer, String> map = new LinkedHashMap<>();
        map.put(AppConstants.CLASS_TYPE_RANGER_POLICY, "RangerPolicyMetricsByDays");
        map.put(AppConstants.CLASS_TYPE_XA_SERVICE, "RangerServiceMetricsByDays");
        map.put(AppConstants.CLASS_TYPE_XA_USER, "RangerUserMetricsByDays");
        map.put(AppConstants.CLASS_TYPE_XA_GROUP, "RangerGroupMetricsByDays");
        map.put(AppConstants.CLASS_TYPE_RANGER_ROLE, "RangerRoleMetricsByDays");
        TYPE_TO_KEY = Collections.unmodifiableMap(map);
    }

    private final int objectClassType;
    private final long createCount;
    private final long updateCount;
    private final long deleteCount;
    private final long auditDate;

    public RangerAuditAdminMetricsByDays(int objectClassType, long createCount, long updateCount, long deleteCount, long auditDate) {
        this.objectClassType = objectClassType;
        this.createCount = createCount;
        this.updateCount = updateCount;
        this.deleteCount = deleteCount;
        this.auditDate = auditDate;
    }

    public int getObjectClassType() {
        return objectClassType;
    }

    public long getCreateCount() {
        return createCount;
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public long getDeleteCount() {
        return deleteCount;
    }

    public long getAuditDate() {
        return auditDate;
    }

    @Override
    public String toString() {
        return "RangerAuditAdminMetricsByDays [objectClassType=" + objectClassType + ", createCount=" + createCount
                + ", updateCount=" + updateCount + ", deleteCount=" + deleteCount + ", auditDate=" + auditDate + "]";
    }
}
