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
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAccessContext {
    public static final String CONTEXT_INFO_CLIENT_TYPE  = "clientType";
    public static final String CONTEXT_INFO_CLUSTER_NAME = "clusterName";
    public static final String CONTEXT_INFO_CLUSTER_TYPE = "clusterType";
    public static final String CONTEXT_INFO_REQUEST_DATA = "requestData";

    private String              serviceType;
    private String              serviceName;
    private long                accessTime;
    private String              clientIpAddress;
    private List<String>        forwardedIpAddresses;
    private Map<String, Object> additionalInfo;

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    public String getClientIpAddress() {
        return clientIpAddress;
    }

    public void setClientIpAddress(String clientIpAddress) {
        this.clientIpAddress = clientIpAddress;
    }

    public List<String> getForwardedIpAddresses() {
        return forwardedIpAddresses;
    }

    public void setForwardedIpAddresses(List<String> forwardedIpAddresses) {
        this.forwardedIpAddresses = forwardedIpAddresses;
    }

    public Map<String, Object> getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(Map<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    @Override
    public String toString() {
        return "RangerAccessContext{" +
                "serviceType=" + serviceType +
                ", serviceName=" + serviceName +
                ", accessTime=" + accessTime +
                ", clientIpAddress='" + clientIpAddress + '\'' +
                ", forwardedIpAddresses=" + forwardedIpAddresses +
                ", additionalInfo=" + additionalInfo +
                '}';
    }
}
