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

package org.apache.ranger.audit.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerAuditMetrics extends RangerBaseModelObject implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String                 serviceType;
    private String                 serviceName;
    private String                 appId;
    private String                 clusterName;
    private String                 clientIP;
    private String                 throughPutUnit;
    private int                    numberOfAudits;
    private RangerAuditMetricsText metricsText;

    /**
     * @param
     */
    public RangerAuditMetrics() {
    }

    /**
     * @param serviceType
     * @param serviceName
     * @param metricsText
     */
    public RangerAuditMetrics(String serviceType, String serviceName, String appId, String clusterName, String clientIP, String throughPutUnit, int numberOfAudits, RangerAuditMetricsText metricsText) {
        super();
        setServiceType(serviceType);
        setServiceName(serviceName);
        setAppId(appId);
        setClusterName(clusterName);
        setclientIP(clientIP);
        setMetricsText(metricsText);
        setThroughPutUnit(throughPutUnit);
        setNumberOfAudits(numberOfAudits);
    }

    /**
     * @param other
     */
    public void updateFrom(RangerAuditMetrics other) {
        super.updateFrom(other);
        setServiceType(other.getServiceType());
        setServiceName(other.getServiceName());
        setAppId(other.getAppId());
        setClusterName(other.getClusterName());
        setclientIP(other.getclientIP());
        setMetricsText(other.getMetricsText());
        setThroughPutUnit(other.getThroughPutUnit());
        setNumberOfAudits(other.getNumberOfAudits());
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

    public String getclientIP() {
        return clientIP;
    }

    public void setclientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getThroughPutUnit() {
        return throughPutUnit;
    }

    public void setThroughPutUnit(String throughPutUnit) {
        this.throughPutUnit = throughPutUnit;
    }

    public int getNumberOfAudits() {
        return numberOfAudits;
    }

    public void setNumberOfAudits(int numberOfAudits) {
        this.numberOfAudits = numberOfAudits;
    }

    /**
     * @return the metricsText
     */

    public RangerAuditMetricsText getMetricsText() {
        return metricsText;
    }

    /**
     * @param metricsText to set
     */
    public void setMetricsText(RangerAuditMetricsText metricsText) {
        this.metricsText = metricsText;
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
        sb.append("throughPutUnit={").append(throughPutUnit).append("} ");
        sb.append("numberOfAudits={").append(numberOfAudits).append("} ");
        sb.append("metricsText={").append(metricsText).append("} ");
        sb.append("numberOfAudits={").append(numberOfAudits).append("} ");
        sb.append("metricsText={").append(metricsText).append("} ");
        sb.append("}");
        return sb;
    }
}
