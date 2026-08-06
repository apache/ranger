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

package org.apache.ranger.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Version;

import java.util.Objects;

@Entity
@Table(name = "x_audit_metrics")
public class XXAuditMetrics extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_AUDIT_METRICS_SEQ", sequenceName = "X_AUDIT_METRICS_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_AUDIT_METRICS_SEQ")
    @Column(name = "id")
    protected Long id;
    @Version
    @Column(name = "version")
    protected Long version;
    @Column(name = "service_type")
    protected Long serviceType;
    @Column(name = "service_name")
    protected String serviceName;
    @Column(name = "app_id")
    protected String appId;
    @Column(name = "cluster_name")
    protected String clusterName;
    @Column(name = "client_ip")
    protected String clientIP;
    @Column(name = "metrics_text")
    protected String metricsText;
    @Column(name = "throughput_unit")
    protected String throughPutUnit;
    @Column(name = "number_of_audits")
    protected int numberOfAudits;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), serviceType, serviceName, throughPutUnit, numberOfAudits, metricsText);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        XXAuditMetrics other = (XXAuditMetrics) obj;

        return Objects.equals(serviceType, other.serviceType) &&
                Objects.equals(serviceName, other.serviceName) &&
                Objects.equals(appId, other.appId) &&
                Objects.equals(clusterName, other.clusterName) &&
                Objects.equals(clientIP, other.clientIP) &&
                Objects.equals(throughPutUnit, other.throughPutUnit) &&
                Objects.equals(numberOfAudits, other.numberOfAudits) &&
                Objects.equals(metricsText, other.metricsText);
    }

    @Override
    public String toString() {
        String sb = "XXAuditMetrics={" +
                super.toString() +
                "{[" +
                "serviceType=" + serviceType + ", " +
                "serviceName=" + serviceType + ", " +
                "appId=" + appId + ", " +
                "clusterName=" + clusterName + ", " +
                "clientIP=" + clientIP + ", " +
                "throughput unit=" + throughPutUnit + ", " +
                "numberOfAudits=" + numberOfAudits + ", " +
                "metricsText=" + metricsText + ", " +
                "]}";
        return sb;
    }

    public Long getServiceType() {
        return serviceType;
    }

    public void setServiceType(Long serviceType) {
        this.serviceType = serviceType;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getAppID() {
        return appId;
    }

    public void setAppID(String appId) {
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

    public String getMetricsText() {
        return metricsText;
    }

    public void setMetricsText(String metricsText) {
        this.metricsText = metricsText;
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
}
