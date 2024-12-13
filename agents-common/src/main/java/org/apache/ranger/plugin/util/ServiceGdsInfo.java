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

package org.apache.ranger.plugin.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerValiditySchedule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceGdsInfo implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String                       serviceName;
    private List<DataShareInfo>          dataShares;
    private List<SharedResourceInfo>     resources;
    private List<DatasetInfo>            datasets;
    private List<ProjectInfo>            projects;
    private List<DataShareInDatasetInfo> dshids;
    private List<DatasetInProjectInfo>   dips;
    private Boolean                      isDelta = Boolean.FALSE;
    private List<ObjectChangeLog>        deltaLogs;
    private RangerServiceDef             gdsServiceDef;
    private Long                         gdsLastUpdateTime;
    private Long                         gdsVersion;

    public ServiceGdsInfo() {
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public List<DataShareInfo> getDataShares() {
        return dataShares;
    }

    public void setDataShares(List<DataShareInfo> dataShares) {
        this.dataShares = dataShares;
    }

    public List<SharedResourceInfo> getResources() {
        return resources;
    }

    public void setResources(List<SharedResourceInfo> resources) {
        this.resources = resources;
    }

    public List<DatasetInfo> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<DatasetInfo> datasets) {
        this.datasets = datasets;
    }

    public List<ProjectInfo> getProjects() {
        return projects;
    }

    public void setProjects(List<ProjectInfo> projects) {
        this.projects = projects;
    }

    public List<DataShareInDatasetInfo> getDshids() {
        return dshids;
    }

    public void setDshids(List<DataShareInDatasetInfo> dshids) {
        this.dshids = dshids;
    }

    public List<DatasetInProjectInfo> getDips() {
        return dips;
    }

    public void setDips(List<DatasetInProjectInfo> dips) {
        this.dips = dips;
    }

    public Boolean getIsDelta() {
        return isDelta;
    }

    public void setIsDelta(Boolean delta) {
        isDelta = delta == null ? Boolean.FALSE : delta;
    }

    public List<ObjectChangeLog> getDeltaLogs() {
        return deltaLogs;
    }

    public void setDeltaLogs(List<ObjectChangeLog> deltaLogs) {
        this.deltaLogs = deltaLogs;
    }

    public RangerServiceDef getGdsServiceDef() {
        return gdsServiceDef;
    }

    public void setGdsServiceDef(RangerServiceDef gdsServiceDef) {
        this.gdsServiceDef = gdsServiceDef;
    }

    public Long getGdsLastUpdateTime() {
        return gdsLastUpdateTime;
    }

    public void setGdsLastUpdateTime(Long gdsLastUpdateTime) {
        this.gdsLastUpdateTime = gdsLastUpdateTime;
    }

    public Long getGdsVersion() {
        return gdsVersion;
    }

    public void setGdsVersion(Long gdsVersion) {
        this.gdsVersion = gdsVersion;
    }

    public void dedupStrings() {
        // TODO: implement this
    }

    public void addDataShare(DataShareInfo dataShare) {
        if (dataShares == null) {
            dataShares = new ArrayList<>();
        }

        dataShares.add(dataShare);
    }

    public void addResource(SharedResourceInfo resource) {
        if (resources == null) {
            resources = new ArrayList<>();
        }

        resources.add(resource);
    }

    public void addDataset(DatasetInfo dataset) {
        if (datasets == null) {
            datasets = new ArrayList<>();
        }

        datasets.add(dataset);
    }

    public void addProject(ProjectInfo project) {
        if (projects == null) {
            projects = new ArrayList<>();
        }

        projects.add(project);
    }

    public void addDataShareInDataset(DataShareInDatasetInfo dshid) {
        if (dshids == null) {
            dshids = new ArrayList<>();
        }

        dshids.add(dshid);
    }

    public void addDatasetInProjectInfo(DatasetInProjectInfo dip) {
        if (dips == null) {
            dips = new ArrayList<>();
        }

        dips.add(dip);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("ServiceGdsInfo={");

        sb.append("serviceName={").append(serviceName).append("}");
        sb.append(", dataShares=[");
        if (dataShares != null) {
            for (DataShareInfo dataShare : dataShares) {
                dataShare.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append(", datasets=[");
        if (datasets != null) {
            for (DatasetInfo dataset : datasets) {
                dataset.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append(", projects=[");
        if (projects != null) {
            for (ProjectInfo project : projects) {
                project.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append(", dshids=[");
        if (dshids != null) {
            for (DataShareInDatasetInfo dshid : dshids) {
                dshid.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append(", dshInDs=[");
        if (dips != null) {
            for (DatasetInProjectInfo dip : dips) {
                dip.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append(", isDelta={").append(isDelta).append("}");

        sb.append(", deltaLogs=[");
        if (deltaLogs != null) {
            for (ObjectChangeLog changeLog : deltaLogs) {
                changeLog.toString(sb).append(", ");
            }
        }
        sb.append("]");

        sb.append("serviceDef={");
        if (gdsServiceDef != null) {
            gdsServiceDef.toString(sb);
        }
        sb.append("}");

        sb.append(", gdsLastUpdateTime={").append(gdsLastUpdateTime).append("}");
        sb.append(", gdsVersion={").append(gdsVersion).append("}");

        sb.append("}");

        return sb;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DataShareInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                    id;
        private String                  name;
        private String                  zoneName;
        private String                  conditionExpr;
        private Set<String>             defaultAccessTypes;
        private List<RangerGdsMaskInfo> defaultTagMasks;

        public DataShareInfo() {
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getZoneName() {
            return zoneName;
        }

        public void setZoneName(String zoneName) {
            this.zoneName = zoneName;
        }

        public String getConditionExpr() {
            return conditionExpr;
        }

        public void setConditionExpr(String conditionExpr) {
            this.conditionExpr = conditionExpr;
        }

        public Set<String> getDefaultAccessTypes() {
            return defaultAccessTypes;
        }

        public void setDefaultAccessTypes(Set<String> defaultAccessTypes) {
            this.defaultAccessTypes = defaultAccessTypes;
        }

        public List<RangerGdsMaskInfo> getDefaultTagMasks() {
            return defaultTagMasks;
        }

        public void setDefaultTagMasks(List<RangerGdsMaskInfo> defaultTagMasks) {
            this.defaultTagMasks = defaultTagMasks;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DataShareInfo={")
                    .append("id=").append(id)
                    .append(", name=").append(name)
                    .append(", zoneName=").append(zoneName);

            sb.append(", conditionExpr=").append(conditionExpr);

            sb.append(", defaultAccessTypes=[");
            if (defaultAccessTypes != null) {
                for (String defaultAccessType : defaultAccessTypes) {
                    sb.append(defaultAccessType).append(", ");
                }
            }
            sb.append("]");

            sb.append(", defaultTagMasks=[");
            if (defaultTagMasks != null) {
                for (RangerGdsMaskInfo defaultTagMask : defaultTagMasks) {
                    defaultTagMask.toString(sb).append(", ");
                }
            }
            sb.append("]");

            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SharedResourceInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                              id;
        private String                            name;
        private Long                              dataShareId;
        private Map<String, RangerPolicyResource> resource;
        private RangerPolicyResource              subResource;
        private String                            subResourceType;
        private String                            conditionExpr;
        private Set<String>                       accessTypes;
        private RangerPolicyItemRowFilterInfo     rowFilter;
        private List<RangerGdsMaskInfo>           subResourceMasks;
        private Set<String>                       profiles;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getDataShareId() {
            return dataShareId;
        }

        public void setDataShareId(Long dataShareId) {
            this.dataShareId = dataShareId;
        }

        public Map<String, RangerPolicyResource> getResource() {
            return resource;
        }

        public void setResource(Map<String, RangerPolicyResource> resource) {
            this.resource = resource;
        }

        public RangerPolicyResource getSubResource() {
            return subResource;
        }

        public void setSubResource(RangerPolicyResource subResource) {
            this.subResource = subResource;
        }

        public String getSubResourceType() {
            return subResourceType;
        }

        public void setSubResourceType(String subResourceType) {
            this.subResourceType = subResourceType;
        }

        public String getConditionExpr() {
            return conditionExpr;
        }

        public void setConditionExpr(String conditionExpr) {
            this.conditionExpr = conditionExpr;
        }

        public Set<String> getAccessTypes() {
            return accessTypes;
        }

        public void setAccessTypes(Set<String> accessTypes) {
            this.accessTypes = accessTypes;
        }

        public RangerPolicyItemRowFilterInfo getRowFilter() {
            return rowFilter;
        }

        public void setRowFilter(RangerPolicyItemRowFilterInfo rowFilter) {
            this.rowFilter = rowFilter;
        }

        public List<RangerGdsMaskInfo> getSubResourceMasks() {
            return subResourceMasks;
        }

        public void setSubResourceMasks(List<RangerGdsMaskInfo> subResourceMasks) {
            this.subResourceMasks = subResourceMasks;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("SharedResourceInfo={")
                    .append("id=").append(id)
                    .append(", name=").append(name)
                    .append(", dataShareId=").append(dataShareId)
                    .append(", resource=").append(resource)
                    .append(", subResource=").append(subResource)
                    .append(", subResourceType=").append(subResourceType)
                    .append(", conditionExpr=").append(conditionExpr);

            sb.append(", accessTypes=[");
            if (accessTypes != null) {
                for (String accessType : accessTypes) {
                    sb.append(accessType).append(", ");
                }
            }
            sb.append("]");

            sb.append(", rowFilter=");
            if (rowFilter != null) {
                rowFilter.toString(sb);
            }

            sb.append(", subResourceMasks=[");
            if (subResourceMasks != null) {
                for (RangerGdsMaskInfo maskInfo : subResourceMasks) {
                    maskInfo.toString(sb).append(' ');
                }
            }
            sb.append("]");

            sb.append(", profiles=[");
            if (profiles != null) {
                for (String profile : profiles) {
                    sb.append(profile).append(", ");
                }
            }
            sb.append("]");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatasetInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   id;
        private String                 name;
        private RangerValiditySchedule validitySchedule;
        private List<RangerPolicy>     policies;

        public DatasetInfo() {
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public List<RangerPolicy> getPolicies() {
            return policies;
        }

        public void setPolicies(List<RangerPolicy> policies) {
            this.policies = policies;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DatasetInfo={")
                    .append("id=").append(id)
                    .append(", name=").append(name)
                    .append(", validitySchedule=").append(validitySchedule);

            sb.append(", policies=[");
            if (policies != null) {
                for (RangerPolicy policy : policies) {
                    policy.toString(sb).append(", ");
                }
            }
            sb.append("]");

            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProjectInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   id;
        private String                 name;
        private RangerValiditySchedule validitySchedule;
        private List<RangerPolicy>     policies;

        public ProjectInfo() {
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public List<RangerPolicy> getPolicies() {
            return policies;
        }

        public void setPolicies(List<RangerPolicy> policies) {
            this.policies = policies;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("ProjectInfo={")
                    .append("id=").append(id)
                    .append(", name=").append(name)
                    .append(", validitySchedule=").append(validitySchedule);

            sb.append(", policies=[");
            if (policies != null) {
                for (RangerPolicy policy : policies) {
                    policy.toString(sb).append(", ");
                }
            }
            sb.append("]");

            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DataShareInDatasetInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   dataShareId;
        private Long                   datasetId;
        private GdsShareStatus         status;
        private RangerValiditySchedule validitySchedule;
        private Set<String>            profiles;

        public DataShareInDatasetInfo() {
        }

        public Long getDataShareId() {
            return dataShareId;
        }

        public void setDataShareId(Long dataShareId) {
            this.dataShareId = dataShareId;
        }

        public Long getDatasetId() {
            return datasetId;
        }

        public void setDatasetId(Long datasetId) {
            this.datasetId = datasetId;
        }

        public GdsShareStatus getStatus() {
            return status;
        }

        public void setStatus(GdsShareStatus status) {
            this.status = status;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DataShareInDatasetInfo={")
                    .append("dataShareId=").append(dataShareId)
                    .append(", datasetId=").append(datasetId)
                    .append(", status=").append(status)
                    .append(", validitySchedule=").append(validitySchedule);

            sb.append(", profiles=[");
            if (profiles != null) {
                for (String profile : profiles) {
                    sb.append(profile).append(", ");
                }
            }
            sb.append("]");

            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatasetInProjectInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private Long                   datasetId;
        private Long                   projectId;
        private GdsShareStatus         status;
        private RangerValiditySchedule validitySchedule;
        private Set<String>            profiles;

        public DatasetInProjectInfo() {
        }

        public Long getDatasetId() {
            return datasetId;
        }

        public void setDatasetId(Long datasetId) {
            this.datasetId = datasetId;
        }

        public Long getProjectId() {
            return projectId;
        }

        public void setProjectId(Long projectId) {
            this.projectId = projectId;
        }

        public GdsShareStatus getStatus() {
            return status;
        }

        public void setStatus(GdsShareStatus status) {
            this.status = status;
        }

        public RangerValiditySchedule getValiditySchedule() {
            return validitySchedule;
        }

        public void setValiditySchedule(RangerValiditySchedule validitySchedule) {
            this.validitySchedule = validitySchedule;
        }

        public Set<String> getProfiles() {
            return profiles;
        }

        public void setProfiles(Set<String> profiles) {
            this.profiles = profiles;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("DatasetInProjectInfo={")
                    .append("datasetId=").append(datasetId)
                    .append(", projectId=").append(projectId)
                    .append(", status=").append(status)
                    .append(", validitySchedule=").append(validitySchedule);

            sb.append(", profiles=[");
            if (profiles != null) {
                for (String profile : profiles) {
                    sb.append(profile).append(", ");
                }
            }
            sb.append("]");

            sb.append("}");

            return sb;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ObjectChangeLog {
        public static final Integer CHANGE_TYPE_CREATE = 1;
        public static final Integer CHANGE_TYPE_UPDATE = 2;
        public static final Integer CHANGE_TYPE_DELETE = 3;

        private Integer objectType;
        private Integer objectId;
        private Integer changeType;

        public ObjectChangeLog() {
        }

        public ObjectChangeLog(Integer objectType, Integer objectId, Integer changeType) {
            this.objectType = objectType;
            this.objectId   = objectId;
            this.changeType = changeType;
        }

        public Integer getObjectType() {
            return objectType;
        }

        public void setObjectType(Integer objectType) {
            this.objectType = objectType;
        }

        public Integer getObjectId() {
            return objectId;
        }

        public void setObjectId(Integer objectId) {
            this.objectId = objectId;
        }

        public Integer getChangeType() {
            return changeType;
        }

        public void setChangeType(Integer changeType) {
            this.changeType = changeType;
        }

        @Override
        public String toString() {
            return toString(new StringBuilder()).toString();
        }

        public StringBuilder toString(StringBuilder sb) {
            sb.append("ObjectChangeLog={")
                    .append("objectType=").append(objectType)
                    .append(", objectId=").append(objectId)
                    .append(", changeType=").append(changeType)
                    .append("}");

            return sb;
        }
    }
}
