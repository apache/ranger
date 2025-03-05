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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class GdsAccessResult {
    private boolean      isAllowed;
    private boolean      isAudited;
    private long         policyId = -1;
    private Long         policyVersion;
    private String       maskType;
    private String       maskedValue;
    private String       maskCondition;
    private List<String> rowFilters;
    private Set<String>  datasets;
    private Set<String>  projects;
    private Set<String>  allowedByDatasets;
    private Set<String>  allowedByProjects;

    public GdsAccessResult() {
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

    public String getMaskType() {
        return maskType;
    }

    public void setMaskType(String maskType) {
        this.maskType = maskType;
    }

    public String getMaskedValue() {
        return maskedValue;
    }

    public void setMaskedValue(String maskedValue) {
        this.maskedValue = maskedValue;
    }

    public String getMaskCondition() {
        return maskCondition;
    }

    public void setMaskCondition(String maskCondition) {
        this.maskCondition = maskCondition;
    }

    public List<String> getRowFilters() {
        return rowFilters;
    }

    public void setRowFilters(List<String> rowFilters) {
        this.rowFilters = rowFilters;
    }

    public Set<String> getDatasets() {
        return datasets;
    }

    public Set<String> getProjects() {
        return projects;
    }

    public Set<String> getAllowedByDatasets() {
        return allowedByDatasets;
    }

    public Set<String> getAllowedByProjects() {
        return allowedByProjects;
    }

    public void addDataset(String name) {
        if (datasets == null) {
            datasets = new HashSet<>();
        }

        datasets.add(name);
    }

    public void addProject(String name) {
        if (projects == null) {
            projects = new HashSet<>();
        }

        projects.add(name);
    }

    public void addAllowedByDataset(String name) {
        if (allowedByDatasets == null) {
            allowedByDatasets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        }

        allowedByDatasets.add(name);
    }

    public void addAllowedByProject(String name) {
        if (allowedByProjects == null) {
            allowedByProjects = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        }

        allowedByProjects.add(name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAllowed, isAudited, policyId, policyVersion, maskType, maskedValue, maskCondition, rowFilters, datasets, projects, allowedByDatasets, allowedByProjects);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if ((obj == null) || !obj.getClass().equals(getClass())) {
            return false;
        } else {
            GdsAccessResult other = (GdsAccessResult) obj;

            return Objects.equals(isAllowed, other.isAllowed) &&
                    Objects.equals(isAudited, other.isAudited) &&
                    Objects.equals(policyId, other.policyId) &&
                    Objects.equals(policyVersion, other.policyVersion) &&
                    Objects.equals(maskType, other.maskType) &&
                    Objects.equals(maskedValue, other.maskedValue) &&
                    Objects.equals(maskCondition, other.maskCondition) &&
                    Objects.equals(rowFilters, other.rowFilters) &&
                    Objects.equals(datasets, other.datasets) &&
                    Objects.equals(projects, other.projects) &&
                    Objects.equals(allowedByDatasets, other.allowedByDatasets) &&
                    Objects.equals(allowedByProjects, other.allowedByProjects);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("RangerGdsAccessResult={");
        sb.append("isAllowed={").append(isAllowed).append("}");
        sb.append(", isAudited={").append(isAudited).append("}");
        sb.append(", policyId={").append(policyId).append("}");
        sb.append(", policyVersion={").append(policyVersion).append("}");
        sb.append(", maskType={").append(maskType).append("}");
        sb.append(", maskedValue={").append(maskedValue).append("}");
        sb.append(", maskCondition={").append(maskCondition).append("}");
        sb.append(", rowFilters={").append(rowFilters).append("}");
        sb.append(", datasets={").append(datasets).append("}");
        sb.append(", projects={").append(projects).append("}");
        sb.append(", allowedByDatasets={").append(allowedByDatasets).append("}");
        sb.append(", allowedByProjects={").append(allowedByProjects).append("}");
        sb.append("}");

        return sb;
    }
}
