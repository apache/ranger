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

package org.apache.ranger.plugin.store;

import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.Collection;
import java.util.List;

/**
 * Interface to backing store for Data share model objects
 */

public interface GdsStore {
    RangerDataset createDataset(RangerDataset dataset) throws Exception;

    RangerDataset updateDataset(RangerDataset dataset) throws Exception;

    void deleteDataset(Long datasetId, boolean forceDelete) throws Exception;

    RangerDataset getDataset(Long datasetId) throws Exception;

    RangerDataset getDatasetByName(String name) throws Exception;

    PList<String> getDatasetNames(SearchFilter filter) throws Exception;

    PList<RangerDataset> searchDatasets(SearchFilter filter) throws Exception;

    RangerPolicy addDatasetPolicy(Long datasetId, RangerPolicy policy) throws Exception;

    RangerPolicy updateDatasetPolicy(Long datasetId, RangerPolicy policy) throws Exception;

    void deleteDatasetPolicy(Long datasetId, Long policyId) throws Exception;

    void deleteDatasetPolicies(Long datasetId) throws Exception;

    RangerPolicy getDatasetPolicy(Long datasetId, Long policyId) throws Exception;

    List<RangerPolicy> getDatasetPolicies(Long datasetId) throws Exception;

    RangerProject createProject(RangerProject dataset) throws Exception;

    RangerProject updateProject(RangerProject dataset) throws Exception;

    void deleteProject(Long datasetId, boolean forceDelete) throws Exception;

    RangerProject getProject(Long projectId) throws Exception;

    RangerProject getProjectByName(String name) throws Exception;

    PList<String> getProjectNames(SearchFilter filter) throws Exception;

    PList<RangerProject> searchProjects(SearchFilter filter) throws Exception;

    RangerPolicy addProjectPolicy(Long projectId, RangerPolicy policy) throws Exception;

    RangerPolicy updateProjectPolicy(Long projectId, RangerPolicy policy) throws Exception;

    void deleteProjectPolicy(Long projectId, Long policyId) throws Exception;

    void deleteProjectPolicies(Long projectId) throws Exception;

    RangerPolicy getProjectPolicy(Long projectId, Long policyId) throws Exception;

    List<RangerPolicy> getProjectPolicies(Long projectId) throws Exception;

    RangerDataShare createDataShare(RangerDataShare dataShare) throws Exception;

    RangerDataShare updateDataShare(RangerDataShare dataShare) throws Exception;

    void deleteDataShare(Long dataShareId, boolean forceDelete) throws Exception;

    RangerDataShare getDataShare(Long dataShareId) throws Exception;

    PList<RangerDataShare> searchDataShares(SearchFilter filter) throws Exception;

    List<RangerSharedResource> addSharedResources(List<RangerSharedResource> resources) throws Exception;

    RangerSharedResource updateSharedResource(RangerSharedResource resource) throws Exception;

    void removeSharedResources(List<Long> sharedResourceIds) throws Exception;

    RangerSharedResource getSharedResource(Long sharedResourceId) throws Exception;

    PList<RangerSharedResource> searchSharedResources(SearchFilter filter) throws Exception;

    RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception;

    RangerDataShareInDataset updateDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception;

    void removeDataShareInDataset(Long dataShareInDatasetId) throws Exception;

    RangerDataShareInDataset getDataShareInDataset(Long dataShareInDatasetId) throws Exception;

    PList<RangerDataShareInDataset> searchDataShareInDatasets(SearchFilter filter) throws Exception;

    RangerDatasetInProject addDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception;

    RangerDatasetInProject updateDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception;

    void removeDatasetInProject(Long datasetInProjectId) throws Exception;

    RangerDatasetInProject getDatasetInProject(Long datasetInProjectId) throws Exception;

    PList<RangerDatasetInProject> searchDatasetInProjects(SearchFilter filter) throws Exception;

    void deleteAllGdsObjectsForService(Long serviceId) throws Exception;

    void deleteAllGdsObjectsForSecurityZone(Long zoneId) throws Exception;

    void onSecurityZoneUpdate(Long zoneId, Collection<String> updatedServices, Collection<String> removedServices) throws Exception;
}
