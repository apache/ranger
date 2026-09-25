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
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuditMetrics extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String serviceType;
    private String serviceName;
    private String appId;
    private String clusterName;
    private String clientIP;
    private Long   numberOfAudits;

    /**
     * @param
     */
    public RangerAuditMetrics() {
    }

    /**
     * @param serviceType
     * @param serviceName
     */
    public RangerAuditMetrics(String serviceType, String serviceName, String appId, String clusterName, String clientIP, Long numberOfAudits) {
        super();
        setServiceType(serviceType);
        setServiceName(serviceName);
        setAppId(appId);
        setClusterName(clusterName);
        setClientIP(clientIP);
        setNumberOfAudits(numberOfAudits);
    }

    /**
     * @return the serviceType
     */
    public String getServiceType() {
        return serviceType;
    }

    /**
     * @param serviceType to set
     */
    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    /**
     * @return the serviceName
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * @param serviceName to set
     */
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
        this.clientIP = clientIP;
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
        sb.append("RangerAuditMetrics={ ");
        super.toString(sb);
        sb.append("serviceType={").append(serviceType).append("} ");
        sb.append("serviceName={").append(serviceName).append("} ");
        sb.append("appId={").append(appId).append("} ");
        sb.append("clusterName={").append(clusterName).append("} ");
        sb.append("clientIP={").append(clientIP).append("} ");
        sb.append("numberOfAudits={").append(numberOfAudits).append("} ");
        sb.append("}");
        return sb;
    }
}
