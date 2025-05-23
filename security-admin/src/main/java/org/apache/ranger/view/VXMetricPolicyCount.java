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
package org.apache.ranger.view;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VXMetricPolicyCount implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    protected Map<String, VXMetricServiceCount> policyCountList = new HashMap<>();
    protected long                              totalCount;

    /**
     * Default constructor. This will set all the attributes to default value.
     */
    public VXMetricPolicyCount() {
    }

    /**
     * @return the policyCountList
     */
    public Map<String, VXMetricServiceCount> getPolicyCountList() {
        return policyCountList;
    }

    /**
     * @param policyCountList the policyCountList to set
     */
    public void setPolicyCountList(Map<String, VXMetricServiceCount> policyCountList) {
        this.policyCountList = policyCountList;
    }

    /**
     * @return the totalCount
     */
    public long getTotalCount() {
        return totalCount;
    }

    /**
     * @param totalCount the totalCount to set
     */
    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public String toString() {
        return "VXMetricPolicyCount={totalCount=" + totalCount + ", vXMetricServiceCount=[" + policyCountList.toString() + "]}";
    }
}
