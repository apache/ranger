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

package org.apache.ranger.entity.view;

import org.apache.ranger.common.DateUtil;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import java.util.Date;

@Entity
@Table(name = "vx_plugin_info")
public class VXXPluginInfo implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "id")
    protected Long id;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CREATE_TIME")
    protected Date createTime;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "UPDATE_TIME")
    protected Date updateTime;

    @Column(name = "service_name")
    protected String serviceName;

    @Column(name = "service_type")
    protected String serviceType;

    @Column(name = "app_type")
    protected String appType;

    @Column(name = "host_name")
    protected String hostName;

    @Column(name = "ip_address")
    protected String ipAddress;

    @Column(name = "info")
    protected String info;

    @Column(name = "is_tag_service_enable")
    protected Boolean isTagServiceEnabled;

    @Column(name = "policy_download_time")
    protected Long policyDownloadTime;

    @Column(name = "policy_activation_time")
    protected Long policyActivationTime;

    @Column(name = "tag_download_time")
    protected Long tagDownloadTime;

    @Column(name = "tag_activation_time")
    protected Long tagActivationTime;

    @Column(name = "gds_download_time")
    protected Long gdsDownloadTime;

    @Column(name = "gds_activation_time")
    protected Long gdsActivationTime;

    @Column(name = "role_download_time")
    protected Long roleDownloadTime;

    @Column(name = "role_activation_time")
    protected Long roleActivationTime;

    @Column(name = "userstore_download_time")
    protected Long userstoreDownloadTime;

    @Column(name = "userstore_activation_time")
    protected Long userstoreActivationTime;

    @Column(name = "cluster_name")
    protected String clusterName;

    @Column(name = "latest_policy_version")
    protected Long latestPolicyVersion;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "last_policy_update_time", nullable = false)
    protected Date lastPolicyUpdateTime = DateUtil.getUTCDate();

    @Column(name = "latest_tag_version")
    protected Long latestTagVersion;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "last_tag_update_time", nullable = false)
    protected Date lastTagUpdateTime = DateUtil.getUTCDate();

    @Column(name = "latest_gds_version")
    protected Long latestGdsVersion;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "last_gds_update_time", nullable = false)
    protected Date lastGdsUpdateTime = DateUtil.getUTCDate();

    @Column(name = "latest_role_version")
    protected Long latestRoleVersion;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "last_role_update_time", nullable = false)
    protected Date lastRoleUpdateTime = DateUtil.getUTCDate();

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public Boolean getIsTagServiceEnabled() {
        return isTagServiceEnabled;
    }

    public void setIsTagServiceEnabled(Boolean isTagServiceEnabled) {
        this.isTagServiceEnabled = isTagServiceEnabled;
    }

    public Long getPolicyDownloadTime() {
        return policyDownloadTime;
    }

    public void setPolicyDownloadTime(Long policyDownloadTime) {
        this.policyDownloadTime = policyDownloadTime;
    }

    public Long getPolicyActivationTime() {
        return policyActivationTime;
    }

    public void setPolicyActivationTime(Long policyActivationTime) {
        this.policyActivationTime = policyActivationTime;
    }

    public Long getTagDownloadTime() {
        return tagDownloadTime;
    }

    public void setTagDownloadTime(Long tagDownloadTime) {
        this.tagDownloadTime = tagDownloadTime;
    }

    public Long getTagActivationTime() {
        return tagActivationTime;
    }

    public void setTagActivationTime(Long tagActivationTime) {
        this.tagActivationTime = tagActivationTime;
    }

    public Long getGdsDownloadTime() {
        return gdsDownloadTime;
    }

    public void setGdsDownloadTime(Long gdsDownloadTime) {
        this.gdsDownloadTime = gdsDownloadTime;
    }

    public Long getGdsActivationTime() {
        return gdsActivationTime;
    }

    public void setGdsActivationTime(Long gdsActivationTime) {
        this.gdsActivationTime = gdsActivationTime;
    }

    public Long getRoleDownloadTime() {
        return roleDownloadTime;
    }

    public void setRoleDownloadTime(Long roleDownloadTime) {
        this.roleDownloadTime = roleDownloadTime;
    }

    public Long getRoleActivationTime() {
        return roleActivationTime;
    }

    public void setRoleActivationTime(Long roleActivationTime) {
        this.roleActivationTime = roleActivationTime;
    }

    public Long getUserstoreDownloadTime() {
        return userstoreDownloadTime;
    }

    public void setUserstoreDownloadTime(Long userstoreDownloadTime) {
        this.userstoreDownloadTime = userstoreDownloadTime;
    }

    public Long getUserstoreActivationTime() {
        return userstoreActivationTime;
    }

    public void setUserstoreActivationTime(Long userstoreActivationTime) {
        this.userstoreActivationTime = userstoreActivationTime;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Long getLatestPolicyVersion() {
        return latestPolicyVersion;
    }

    public void setLatestPolicyVersion(Long latestPolicyVersion) {
        this.latestPolicyVersion = latestPolicyVersion;
    }

    public Date getLastPolicyUpdateTime() {
        return lastPolicyUpdateTime;
    }

    public void setLastPolicyUpdateTime(Date lastPolicyUpdateTime) {
        this.lastPolicyUpdateTime = lastPolicyUpdateTime;
    }

    public Long getLatestTagVersion() {
        return latestTagVersion;
    }

    public void setLatestTagVersion(Long latestTagVersion) {
        this.latestTagVersion = latestTagVersion;
    }

    public Date getLastTagUpdateTime() {
        return lastTagUpdateTime;
    }

    public void setLastTagUpdateTime(Date lastTagUpdateTime) {
        this.lastTagUpdateTime = lastTagUpdateTime;
    }

    public Long getLatestGdsVersion() {
        return latestGdsVersion;
    }

    public void setLatestGdsVersion(Long latestGdsVersion) {
        this.latestGdsVersion = latestGdsVersion;
    }

    public Date getLastGdsUpdateTime() {
        return lastGdsUpdateTime;
    }

    public void setLastGdsUpdateTime(Date lastGdsUpdateTime) {
        this.lastGdsUpdateTime = lastGdsUpdateTime;
    }

    public Long getLatestRoleVersion() {
        return latestRoleVersion;
    }

    public void setLatestRoleVersion(Long latestRoleVersion) {
        this.latestRoleVersion = latestRoleVersion;
    }

    public Date getLastRoleUpdateTime() {
        return lastRoleUpdateTime;
    }

    public void setLastRoleUpdateTime(Date lastRoleUpdateTime) {
        this.lastRoleUpdateTime = lastRoleUpdateTime;
    }

    @Override
    public String toString() {
        String str = "VXXPluginInfo={";
        str += "id={" + id + "} ";
        str += "createTime={" + createTime + "} ";
        str += "updateTime={" + updateTime + "} ";
        str += "serviceName={" + serviceName + "} ";
        str += "hostName={" + hostName + "} ";
        str += "appType={" + appType + "} ";
        str += "ipAddress={" + ipAddress + "} ";
        str += "serviceType={" + serviceType + "} ";
        str += "isTagServiceEnabled={" + isTagServiceEnabled + "} ";
        str += "clusterName={" + clusterName + "} ";
        str += "}";
        return str;
    }

    public static boolean equals(Object object1, Object object2) {
        boolean ret = false;

        if (object1 == object2) {
            ret = true;
        } else if ((object1 != null) && (object2 != null)) {
            ret = object1.equals(object2);
        }

        return ret;
    }
}
