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
package org.apache.ranger.plugin.policyengine.gds;


import java.util.*;

public class GdsAccessResult {
    private Set<Long>    datasets;
    private Set<Long>    projects;
    private List<String> datasetNames;
    private List<String> projectNames;
    private boolean      isAllowed;
    private boolean      isAudited;
    private long         policyId = -1;
    private Long         policyVersion;


    public GdsAccessResult() {
    }

    public void addDataset(Long datasetId) {
        if (datasets == null) {
            datasets = new HashSet<>();
        }

        datasets.add(datasetId);
    }

    public boolean hasDataset(Long datasetId) {
        return datasets != null && datasets.contains(datasetId);
    }

    public Set<Long> getDatasets() {
        return datasets;
    }

    public void addDatasetName(String name) {
        if (datasetNames == null) {
            datasetNames = datasets == null ? new ArrayList<>() : new ArrayList<>(datasets.size());
        }

        datasetNames.add(name);
    }

    public List<String> getDatasetNames() {
        return datasetNames;
    }

    public void addProject(Long projectId) {
        if (projects == null) {
            projects = new HashSet<>();
        }

        projects.add(projectId);
    }

    public boolean hasProject(Long projectId) {
        return projects != null && projects.contains(projectId);
    }

    public Set<Long> getProjects() {
        return projects;
    }

    public void addProjectName(String name) {
        if (projectNames == null) {
            projectNames = projects == null ? new ArrayList<>() : new ArrayList<>(projects.size());
        }

        projectNames.add(name);
    }

    public List<String> getProjectNames() {
        return projectNames;
    }

    public boolean getIsAllowed() {
        return isAllowed;
    }

    public void setIsAllowed(boolean allowed) {
        isAllowed = allowed;
    }

    public boolean getIsAudited() {
        return isAudited;
    }

    public void setIsAudited(boolean audited) {
        isAudited = audited;
    }

    public long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(long policyId) {
        this.policyId = policyId;
    }

    public Long getPolicyVersion() {
        return policyVersion;
    }

    public void setPolicyVersion(Long policyVersion) {
        this.policyVersion = policyVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasets, projects, datasetNames, projectNames, isAllowed, isAudited, policyId, policyVersion);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if ((obj == null) || !obj.getClass().equals(getClass())) {
            return false;
        } else {
            GdsAccessResult other = (GdsAccessResult) obj;

            return Objects.equals(datasets, other.datasets) &&
                   Objects.equals(projects, other.projects) &&
                   Objects.equals(datasetNames, other.datasetNames) &&
                   Objects.equals(projectNames, other.projectNames) &&
                   Objects.equals(isAllowed, other.isAllowed) &&
                   Objects.equals(isAudited, other.isAudited) &&
                   Objects.equals(policyId, other.policyId) &&
                   Objects.equals(policyVersion, other.policyVersion);
        }
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerGdsAccessResult={");
        sb.append("datasets={").append(datasets).append("}");
        sb.append(", projects={").append(projects).append("}");
        sb.append(", datasetNames={").append(datasetNames).append("}");
        sb.append(", projectNames={").append(projectNames).append("}");
        sb.append(", isAllowed={").append(isAllowed).append("}");
        sb.append(", isAudited={").append(isAudited).append("}");
        sb.append(", policyId={").append(policyId).append("}");
        sb.append(", policyVersion={").append(policyVersion).append("}");
        sb.append("}");

        return sb;
    }
}
