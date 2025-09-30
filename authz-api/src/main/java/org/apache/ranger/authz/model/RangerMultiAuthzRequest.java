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

import java.util.List;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerMultiAuthzRequest {
    private String                 requestId;
    private RangerUserInfo         user;
    private List<RangerAccessInfo> accesses;
    private RangerAccessContext    context;

    public RangerMultiAuthzRequest() {
    }

    public RangerMultiAuthzRequest(RangerUserInfo user, List<RangerAccessInfo> accesses, RangerAccessContext context) {
        this(null, user, accesses, context);
    }

    public RangerMultiAuthzRequest(String requestId, RangerUserInfo user, List<RangerAccessInfo> accesses, RangerAccessContext context) {
        this.requestId = requestId;
        this.user      = user;
        this.accesses  = accesses;
        this.context   = context;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public RangerUserInfo getUser() {
        return user;
    }

    public void setUser(RangerUserInfo user) {
        this.user = user;
    }

    public List<RangerAccessInfo> getAccesses() {
        return accesses;
    }

    public void setAccesses(List<RangerAccessInfo> accesses) {
        this.accesses = accesses;
    }

    public RangerAccessContext getContext() {
        return context;
    }

    public void setContext(RangerAccessContext context) {
        this.context = context;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, user, accesses, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RangerMultiAuthzRequest other = (RangerMultiAuthzRequest) obj;

        return Objects.equals(requestId, other.requestId) &&
               Objects.equals(user, other.user) &&
               Objects.equals(accesses, other.accesses) &&
               Objects.equals(context, other.context);
    }

    @Override
    public String toString() {
        return "RangerAuthzRequest{" +
                "requestId='" + requestId + '\'' +
                ", user=" + user +
                ", accesses=" + accesses +
                ", context=" + context +
                '}';
    }
}
