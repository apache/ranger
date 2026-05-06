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
import org.apache.ranger.plugin.util.SearchFilter;

import java.util.List;

public abstract class AbstractGdsStore implements GdsStore {
    @Override
    public RangerDataset createDataset(RangerDataset dataset) throws Exception {
        return null;
    }

    @Override
    public RangerDataset updateDataset(RangerDataset dataset) throws Exception {
        return null;
    }

    @Override
    public void deleteDataset(Long datasetId, boolean forceDelete) throws Exception {
    }

    @Override
    public RangerDataset getDataset(Long datasetId) throws Exception {
        return null;
    }

    @Override
    public RangerDataset getDatasetByName(String name) throws Exception {
        return null;
    }

    @Override
    public PList<String> getDatasetNames(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public PList<RangerDataset> searchDatasets(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public RangerProject createProject(RangerProject dataset) throws Exception {
        return null;
    }

    @Override
    public RangerProject updateProject(RangerProject dataset) throws Exception {
        return null;
    }

    @Override
    public void deleteProject(Long datasetId, boolean forceDelete) throws Exception {
    }

    @Override
    public RangerProject getProject(Long projectId) throws Exception {
        return null;
    }

    @Override
    public RangerProject getProjectByName(String name) throws Exception {
        return null;
    }

    @Override
    public PList<String> getProjectNames(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public PList<RangerProject> searchProjects(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public RangerDataShare createDataShare(RangerDataShare dataShare) throws Exception {
        return null;
    }

    @Override
    public RangerDataShare updateDataShare(RangerDataShare dataShare) throws Exception {
        return null;
    }

    @Override
    public void deleteDataShare(Long dataShareId, boolean forceDelete) throws Exception {
    }

    @Override
    public RangerDataShare getDataShare(Long dataShareId) throws Exception {
        return null;
    }

    @Override
    public PList<RangerDataShare> searchDataShares(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public List<RangerSharedResource> addSharedResources(List<RangerSharedResource> resources) throws Exception {
        return null;
    }

    @Override
    public RangerSharedResource updateSharedResource(RangerSharedResource resource) throws Exception {
        return null;
    }

    @Override
    public void removeSharedResources(List<Long> sharedResourceIds) throws Exception {
    }

    @Override
    public RangerSharedResource getSharedResource(Long sharedResourceId) throws Exception {
        return null;
    }

    @Override
    public PList<RangerSharedResource> searchSharedResources(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception {
        return null;
    }

    @Override
    public RangerDataShareInDataset updateDataShareInDataset(RangerDataShareInDataset dataShareInDataset) throws Exception {
        return null;
    }

    @Override
    public void removeDataShareInDataset(Long dataShareInDatasetId) throws Exception {
    }

    @Override
    public RangerDataShareInDataset getDataShareInDataset(Long dataShareInDatasetId) throws Exception {
        return null;
    }

    @Override
    public PList<RangerDataShareInDataset> searchDataShareInDatasets(SearchFilter filter) throws Exception {
        return null;
    }

    @Override
    public RangerDatasetInProject addDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception {
        return null;
    }

    @Override
    public RangerDatasetInProject updateDatasetInProject(RangerDatasetInProject datasetInProject) throws Exception {
        return null;
    }

    @Override
    public void removeDatasetInProject(Long datasetInProjectId) throws Exception {
    }

    @Override
    public RangerDatasetInProject getDatasetInProject(Long datasetInProjectId) throws Exception {
        return null;
    }

    @Override
    public PList<RangerDatasetInProject> searchDatasetInProjects(SearchFilter filter) throws Exception {
        return null;
    }
}
