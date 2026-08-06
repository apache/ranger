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

package org.apache.ranger.plugin.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuditMetricsByHours extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String  serviceType;
    private String  serviceName;
    private String  appId;
    private String  clusterName;
    private String  clientIP;
    private Integer hours;
    private Long    numberOfAudits;

    /**
     * @param
     */
    public RangerAuditMetricsByHours() {
        this(null, null, null, null, null, null, null);
    }

    /**
     * @param serviceType
     * @param serviceName
     * @param hours
     * @param numberOfAudits
     */
    public RangerAuditMetricsByHours(String serviceType, String serviceName, String appId, String clusterName, String clientIP, Integer hours, Long numberOfAudits) {
        super();
        setServiceType(serviceType);
        setServiceName(serviceName);
        setAppId(appId);
        setClusterName(clusterName);
        setClientIP(clientIP);
        setHours(hours);
        setNumberOfAudits(numberOfAudits);
    }

    /**
     * @param other
     */
    public void updateFrom(RangerAuditMetricsByHours other) {
        super.updateFrom(other);
        setServiceType(other.getServiceType());
        setServiceName(other.getServiceName());
        setAppId(other.getAppId());
        setClusterName(other.getClusterName());
        setClientIP(other.getClientIP());
        setHours(other.getHours());
        setNumberOfAudits(other.getNumberOfAudits());
    }

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

    public Integer getHours() {
        return hours;
    }

    public void setHours(Integer hours) {
        this.hours = hours;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        if (clientIP == null) {
            this.clientIP = "";
        } else {
            this.clientIP = clientIP;
        }
    }

    public Long getNumberOfAudits() {
        return numberOfAudits;
    }

    public void setNumberOfAudits(Long numberOfAudits) {
        this.numberOfAudits = numberOfAudits;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerAuditMetricsByHours={ ");
        super.toString(sb);
        sb.append("serviceType={").append(serviceType).append("} ");
        sb.append("serviceName={").append(serviceName).append("} ");
        sb.append("appId={").append(appId).append("} ");
        sb.append("clusterName={").append(clusterName).append("} ");
        sb.append("clientIP={").append(clientIP).append("} ");
        sb.append("Hours={").append(hours).append("} ");
        sb.append("numberOfAudits={").append(numberOfAudits).append("} ");
        sb.append("}");
        return sb;
    }
}
