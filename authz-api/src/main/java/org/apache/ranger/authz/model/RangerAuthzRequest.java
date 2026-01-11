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

import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuthzRequest {
    private String              requestId;
    private RangerUserInfo      user;
    private RangerAccessInfo    access;
    private RangerAccessContext context;

    public RangerAuthzRequest() {
    }

    public RangerAuthzRequest(RangerUserInfo user, RangerAccessInfo access, RangerAccessContext context) {
        this(null, user, access, context);
    }

    public RangerAuthzRequest(String requestId, RangerUserInfo user, RangerAccessInfo access, RangerAccessContext context) {
        this.requestId = requestId;
        this.user      = user;
        this.access    = access;
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

    public RangerAccessInfo getAccess() {
        return access;
    }

    public void setAccess(RangerAccessInfo access) {
        this.access = access;
    }

    public RangerAccessContext getContext() {
        return context;
    }

    public void setContext(RangerAccessContext context) {
        this.context = context;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, user, access, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerAuthzRequest that = (RangerAuthzRequest) o;

        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(user, that.user) &&
                Objects.equals(access, that.access) &&
                Objects.equals(context, that.context);
    }

    @Override
    public String toString() {
        return "RangerAuthzRequest{" +
                "requestId='" + requestId + '\'' +
                ", user=" + user +
                ", access=" + access +
                ", context=" + context +
                '}';
    }
}
