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

package org.apache.ranger.authz.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;

import java.util.List;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerMultiAuthzResult {
    private String                  requestId;
    private AccessDecision          decision;
    private List<RangerAuthzResult> accesses;

    public RangerMultiAuthzResult() {
    }

    public RangerMultiAuthzResult(String requestId) {
        this(requestId, null);
    }

    public RangerMultiAuthzResult(String requestId, List<RangerAuthzResult> accesses) {
        this.requestId = requestId;
        this.accesses  = accesses;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public AccessDecision getDecision() {
        return decision;
    }

    public void setDecision(AccessDecision decision) {
        this.decision = decision;
    }

    public List<RangerAuthzResult> getAccesses() {
        return accesses;
    }

    public void setAccesses(List<RangerAuthzResult> accesses) {
        this.accesses = accesses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, decision, accesses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerMultiAuthzResult that = (RangerMultiAuthzResult) o;

        return Objects.equals(requestId, that.requestId) &&
                decision == that.decision &&
                Objects.equals(accesses, that.accesses);
    }

    @Override
    public String toString() {
        return "RangerMultiAuthzResult{" +
                "requestId='" + requestId + '\'' +
                ", decision=" + decision +
                ", accesses=" + accesses +
                '}';
    }
}
