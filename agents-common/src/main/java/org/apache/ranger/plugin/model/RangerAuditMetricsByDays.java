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

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuditMetricsByDays extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String  serviceType;
    private String  serviceName;
    private String  appId;
    private String  clusterName;
    private String  clientIP;
    private Integer days;
    private Long    auditDate;
    private Long    numberOfAudits;

    /**
     * @param
     */
    public RangerAuditMetricsByDays() {
        this(null, null, null, null, null, null, null, null);
    }

    /**
     * @param serviceType
     * @param serviceName
     * @param appId
     * @param clusterName
     * @param clientIP
     * @param days
     * @param auditDate
     * @param numberOfAudits
     */
    public RangerAuditMetricsByDays(String serviceType, String serviceName, String appId, String clusterName, String clientIP, Integer days, Long auditDate, Long numberOfAudits) {
        super();
        setServiceType(serviceType);
        setServiceName(serviceName);
        setAppId(appId);
        setClusterName(clusterName);
        setClientIP(clientIP);
        setDays(days);
        setAuditDate(auditDate);
        setNumberOfAudits(numberOfAudits);
    }

    /**
     * @param other
     */
    public void updateFrom(RangerAuditMetricsByDays other) {
        super.updateFrom(other);
        setServiceType(other.getServiceType());
        setServiceName(other.getServiceName());
        setAppId(other.getAppId());
        setClusterName(other.getClusterName());
        setClientIP(other.getClientIP());
        setAuditDate(other.getAuditDate());
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

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }

    public Long getNumberOfAudits() {
        return numberOfAudits;
    }

    public void setNumberOfAudits(Long numberOfAudits) {
        this.numberOfAudits = numberOfAudits;
    }

    public Long getAuditDate() {
        return auditDate;
    }

    public void setAuditDate(Long auditDate) {
        this.auditDate = auditDate;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerAuditMetricsByDays={ ");
        super.toString(sb);
        sb.append("serviceType={").append(serviceType).append("} ");
        sb.append("serviceName={").append(serviceName).append("} ");
        sb.append("appId={").append(appId).append("} ");
        sb.append("clusterName={").append(clusterName).append("} ");
        sb.append("clientIP={").append(clientIP).append("} ");
        sb.append("days={").append(days).append("} ");
        sb.append("auditDate={").append(auditDate).append("} ");
        sb.append("numberOfAudits={").append(numberOfAudits).append("} ");
        sb.append("}");
        return sb;
    }
}
