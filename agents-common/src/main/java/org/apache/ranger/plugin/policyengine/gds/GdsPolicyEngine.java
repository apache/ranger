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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DatasetInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DatasetInProjectInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInDatasetInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.ProjectInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.SharedResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class GdsPolicyEngine {
    private static final Logger LOG = LoggerFactory.getLogger(GdsPolicyEngine.class);

    public static final String GDS_SERVICE_NAME         = "_gds";
    public static final String RESOURCE_NAME_DATASET_ID = "dataset-id";
    public static final String RESOURCE_NAME_PROJECT_ID = "project-id";

    private final ServiceGdsInfo                           gdsInfo;
    private final Set<String>                              allAccessTypes;
    private final Map<String, List<GdsDataShareEvaluator>> zoneDataShares = new HashMap<>();
    private final Map<Long, GdsDatasetEvaluator>           datasets       = new HashMap<>();
    private final Map<Long, GdsProjectEvaluator>           projects       = new HashMap<>();

    public GdsPolicyEngine(ServiceGdsInfo gdsInfo, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        LOG.debug("==> RangerGdsPolicyEngine()");

        this.gdsInfo        = gdsInfo;
        this.allAccessTypes = Collections.unmodifiableSet(getAllAccessTypes(serviceDefHelper));

        init(serviceDefHelper, pluginContext);

        LOG.debug("<== RangerGdsPolicyEngine()");
    }

    public ServiceGdsInfo getGdsInfo() {
        return gdsInfo;
    }

    public GdsAccessResult evaluate(RangerAccessRequest request) {
        LOG.debug("==> RangerGdsPolicyEngine.evaluate({})", request);

        GdsAccessResult ret         = null;
        boolean         isAnyAccess = request.isAccessTypeAny();

        try {
            if (isAnyAccess) {
                RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), allAccessTypes);
                RangerAccessRequestUtil.setIsAnyAccessInContext(request.getContext(), Boolean.TRUE);
            }

            List<GdsDataShareEvaluator> dataShares = getDataShareEvaluators(request);

            if (!dataShares.isEmpty()) {
                ret = new GdsAccessResult();

                if (dataShares.size() > 1) {
                    dataShares.sort(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);
                }

                Set<Long> datasetIds = new HashSet<>();

                for (GdsDataShareEvaluator dshEvaluator : dataShares) {
                    dshEvaluator.evaluate(request, ret, datasetIds);
                }

                if (!datasetIds.isEmpty()) {
                    Set<Long> projectIds = new HashSet<>();

                    evaluateDatasetPolicies(datasetIds, request, ret, projectIds);

                    if (!projectIds.isEmpty()) {
                        evaluateProjectPolicies(projectIds, request, ret);
                    }
                }
            }
        } finally {
            if (isAnyAccess) {
                RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), null);
                RangerAccessRequestUtil.setIsAnyAccessInContext(request.getContext(), Boolean.FALSE);

            }
        }

        LOG.debug("<== RangerGdsPolicyEngine.evaluate({}): {}", request, ret);

        return ret;
    }

    public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
        RangerResourceACLs ret = new RangerResourceACLs();

        List<GdsDataShareEvaluator> dataShares = getDataShareEvaluators(request);

        if (!dataShares.isEmpty()) {
            if (dataShares.size() > 1) {
                dataShares.sort(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);
            }

            for (GdsDataShareEvaluator dshEvaluator : dataShares) {
                dshEvaluator.getResourceACLs(request, ret);
            }
        }

        ret.finalizeAcls();

        return ret;
    }

    public Set<Long> getDatasetsSharedWith(Set<String> users, Set<String> groups, Set<String> roles) {
        Set<Long> ret = new HashSet<>();

        for (GdsDatasetEvaluator dataset : datasets.values()) {
            if (dataset.hasReference(users, groups, roles)) {
                ret.add(dataset.getId());
            }
        }

        return ret;
    }

    public Set<Long> getProjectsSharedWith(Set<String> users, Set<String> groups, Set<String> roles) {
        Set<Long> ret = new HashSet<>();

        for (GdsProjectEvaluator project : projects.values()) {
            if (project.hasReference(users, groups, roles)) {
                ret.add(project.getId());
            }
        }

        return ret;
    }

    public long getDatasetId(String datasetName) {
        GdsDatasetEvaluator evaluator = getDatasetEvaluator(datasetName);

        return evaluator == null ? -1 : evaluator.getId();
    }

    public long getProjectId(String projectName) {
        GdsProjectEvaluator evaluator = getProjectEvaluator(projectName);

        return evaluator == null ? -1 : evaluator.getId();
    }

    public String getDatasetName(Long id) {
        GdsDatasetEvaluator evaluator = datasets.get(id);

        return evaluator == null ? null : evaluator.getName();
    }

    public String getProjectName(Long id) {
        GdsProjectEvaluator evaluator = projects.get(id);

        return evaluator == null ? null : evaluator.getName();
    }

    public Iterator<GdsSharedResourceEvaluator> getDatasetResources(long datasetId) {
        Set<GdsDataShareEvaluator> dshEvaluators = new HashSet<>();

        collectDataSharesForDataset(datasetId, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getProjectResources(long projectId) {
        Set<GdsDataShareEvaluator> dshEvaluators = new HashSet<>();

        collectDataSharesForProject(projectId, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getDataShareResources(long dataShareId) {
        GdsDataShareEvaluator      dshEvaluator  = getDataShareEvaluator(dataShareId);
        Set<GdsDataShareEvaluator> dshEvaluators = dshEvaluator == null ? Collections.emptySet() : Collections.singleton(dshEvaluator);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getResources(List<Long> datasetIds, List<Long> dataShareIds) {
        Set<GdsDataShareEvaluator> dshEvaluators = new HashSet<>();

        collectDataShares(null, datasetIds, dataShareIds, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getResources(List<Long> projectIds, List<Long> datasetIds, List<Long> dataShareIds) {
        Set<GdsDataShareEvaluator> dshEvaluators = new HashSet<>();

        collectDataShares(projectIds, datasetIds, dataShareIds, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }


    private void init(RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        LOG.debug("==> RangerGdsPolicyEngine.init()");

        preprocessGdsServiceDef(gdsInfo.getGdsServiceDef(), serviceDefHelper);

        RangerServiceDef                    gdsServiceDef = gdsInfo.getGdsServiceDef();
        RangerPolicyEngineOptions           options       = new RangerPolicyEngineOptions(pluginContext.getConfig().getPolicyEngineOptions(), new RangerServiceDefHelper(gdsServiceDef, false));
        Map<Long, List<SharedResourceInfo>> dshResources  = new HashMap<>();
        Map<Long, GdsDataShareEvaluator>    dshEvaluators = new HashMap<>();

        if (gdsInfo.getProjects() != null) {
            for (ProjectInfo projectInfo : gdsInfo.getProjects()) {
                projects.put(projectInfo.getId(), new GdsProjectEvaluator(projectInfo, gdsServiceDef, options));
            }
        }

        if (gdsInfo.getDatasets() != null) {
            for (DatasetInfo datasetInfo : gdsInfo.getDatasets()) {
                datasets.put(datasetInfo.getId(), new GdsDatasetEvaluator(datasetInfo, gdsServiceDef, options));
            }
        }

        // dshResources must be populated before processing dataShares; hence resources should be processed before dataShares
        if (gdsInfo.getResources() != null) {
            for (SharedResourceInfo resource : gdsInfo.getResources()) {
                List<SharedResourceInfo> resources = dshResources.computeIfAbsent(resource.getDataShareId(), k -> new ArrayList<>());

                resources.add(resource);
            }
        }

        if (gdsInfo.getDataShares() != null) {
            for (DataShareInfo dsh : gdsInfo.getDataShares()) {
                GdsDataShareEvaluator       dshEvaluator   = new GdsDataShareEvaluator(dsh, dshResources.get(dsh.getId()), serviceDefHelper, pluginContext);
                List<GdsDataShareEvaluator> zoneEvaluators = zoneDataShares.computeIfAbsent(dshEvaluator.getZoneName(), k -> new ArrayList<>());

                zoneEvaluators.add(dshEvaluator);
                dshEvaluators.put(dsh.getId(), dshEvaluator);
            }
        }

        if (gdsInfo.getDshids() != null) {
            for (DataShareInDatasetInfo dshid : gdsInfo.getDshids()) {
                if (dshid.getStatus() != RangerGds.GdsShareStatus.ACTIVE) {
                    LOG.error("RangerGdsPolicyEngine(): dshid is not active {}. Ignored", dshid);

                    continue;
                }

                GdsDataShareEvaluator dshEvaluator = dshEvaluators.get(dshid.getDataShareId());

                if (dshEvaluator == null) {
                    LOG.error("RangerGdsPolicyEngine(): invalid dataShareId in dshid: {}. Ignored", dshid);

                    continue;
                }

                GdsDatasetEvaluator datasetEvaluator = datasets.get(dshid.getDatasetId());

                if (datasetEvaluator == null) {
                    LOG.error("RangerGdsPolicyEngine(): invalid datasetId in dshid: {}. Ignored", dshid);

                    continue;
                }

                GdsDshidEvaluator dshidEvaluator = new GdsDshidEvaluator(dshid, datasetEvaluator);

                dshEvaluator.addDshidEvaluator(dshidEvaluator);
            }
        }

        if (gdsInfo.getDips() != null) {
            for (DatasetInProjectInfo dip : gdsInfo.getDips()) {
                if (dip.getStatus() != RangerGds.GdsShareStatus.ACTIVE) {
                    LOG.error("RangerGdsPolicyEngine(): dip is not active {}. Ignored", dip);

                    continue;
                }

                GdsDatasetEvaluator datasetEvaluator = datasets.get(dip.getDatasetId());

                if (datasetEvaluator == null) {
                    LOG.error("RangerGdsPolicyEngine(): invalid datasetId in dip: {}. Ignored", dip);

                    continue;
                }

                GdsProjectEvaluator projectEvaluator = projects.get(dip.getProjectId());

                if (projectEvaluator == null) {
                    LOG.error("RangerGdsPolicyEngine(): invalid projectId in dip: {}. Ignored", dip);

                    continue;
                }

                GdsDipEvaluator dipEvaluator = new GdsDipEvaluator(dip, projectEvaluator);

                datasetEvaluator.addDipEvaluator(dipEvaluator);
            }
        }

        LOG.debug("<== RangerGdsPolicyEngine.init()");
    }

    private void preprocessGdsServiceDef(RangerServiceDef gdsServiceDef, RangerServiceDefHelper serviceDefHelper) {
        // populate accessTypes in GDS servicedef with implied accessTypes from the service
        for (RangerAccessTypeDef gdsAccessTypeDef : gdsServiceDef.getAccessTypes()) {
            Collection<String> impliedGrants = serviceDefHelper.getImpliedAccessGrants().get(gdsAccessTypeDef.getName());

            if (impliedGrants != null) {
                gdsAccessTypeDef.getImpliedGrants().addAll(impliedGrants);
            }
        }

        gdsServiceDef.getAccessTypes().addAll(serviceDefHelper.getServiceDef().getAccessTypes());
    }

    private List<GdsDataShareEvaluator> getDataShareEvaluators(RangerAccessRequest request) {
        LOG.debug("==> RangerGdsPolicyEngine.getDataShareEvaluators({})", request);

        List<GdsDataShareEvaluator> ret = null;

        if (!zoneDataShares.isEmpty()) {
            Set<String> zoneNames = RangerAccessRequestUtil.getResourceZoneNamesFromContext(request.getContext());

            if (zoneNames == null || zoneNames.isEmpty()) {
                zoneNames = Collections.singleton(StringUtils.EMPTY); // unzoned
            } else if (zoneNames.size() > 1 && !request.isAccessTypeAny()) {
                LOG.warn("RangerGdsPolicyEngine.getDataShareEvaluators(): resource matches multiple zones and accessType is not ANY - ignored. resource={}, zones={}", request.getResource(), zoneNames);

                zoneNames = Collections.emptySet();
            }

            for (String zoneName : zoneNames) {
                List<GdsDataShareEvaluator> zonEvaluators = zoneDataShares.get(zoneName);

                if (zonEvaluators != null && !zonEvaluators.isEmpty()) {
                    if (ret == null) {
                        ret = new ArrayList<>();
                    }

                    ret.addAll(zonEvaluators);
                }
            }
        }

        if (ret == null) {
            ret = Collections.emptyList();
        }

        LOG.debug("<== RangerGdsPolicyEngine.getDataShareEvaluators({}): {}", request, ret);

        return ret;
    }

    private void evaluateDatasetPolicies(Set<Long> datasetIds, RangerAccessRequest request, GdsAccessResult result, Set<Long> projectIds) {
        List<GdsDatasetEvaluator> evaluators = new ArrayList<>(datasetIds.size());

        for (Long datasetId : datasetIds) {
            GdsDatasetEvaluator evaluator = datasets.get(datasetId);

            if (evaluator == null) {
                LOG.error("evaluateDatasetPolicies(): invalid datasetId in result: {}. Ignored", datasetId);

                continue;
            }

            evaluators.add(evaluator);
        }

        if (evaluators.size() > 1) {
            evaluators.sort(GdsDatasetEvaluator.EVAL_ORDER_COMPARATOR);
        }

        if (!evaluators.isEmpty()) {
            for (GdsDatasetEvaluator evaluator : evaluators) {
                evaluator.evaluate(request, result, projectIds);
            }
        }
    }

    private void evaluateProjectPolicies(Set<Long> projectIds, RangerAccessRequest request, GdsAccessResult result) {
        List<GdsProjectEvaluator> evaluators = new ArrayList<>(projectIds.size());

        for (Long projectId : projectIds) {
            GdsProjectEvaluator evaluator = projects.get(projectId);

            if (evaluator == null) {
                LOG.error("evaluateProjectPolicies(): invalid projectId in result: {}. Ignored", projectId);

                continue;
            }

            evaluators.add(evaluator);
        }

        if (evaluators.size() > 1) {
            evaluators.sort(GdsProjectEvaluator.EVAL_ORDER_COMPARATOR);
        }

        for (GdsProjectEvaluator evaluator : evaluators) {
            evaluator.evaluate(request, result);
        }
    }

    private GdsDatasetEvaluator getDatasetEvaluator(String dsName) {
        GdsDatasetEvaluator ret = null;

        for (GdsDatasetEvaluator evaluator : datasets.values()) {
            if (StringUtils.equals(evaluator.getName(), dsName)) {
                ret = evaluator;

                break;
            }
        }

        return ret;
    }

    private GdsProjectEvaluator getProjectEvaluator(String projectName) {
        GdsProjectEvaluator ret = null;

        for (GdsProjectEvaluator evaluator : projects.values()) {
            if (StringUtils.equals(evaluator.getName(), projectName)) {
                ret = evaluator;

                break;
            }
        }

        return ret;
    }

    private GdsDataShareEvaluator getDataShareEvaluator(long dataShareId) {
        GdsDataShareEvaluator ret = null;

        for (List<GdsDataShareEvaluator> dshEvaluators : zoneDataShares.values()) {
            for (GdsDataShareEvaluator dshEvaluator : dshEvaluators) {
                if (dshEvaluator.getId().equals(dataShareId)) {
                    ret = dshEvaluator;

                    break;
                }
            }
        }

        return ret;
    }

    private void collectDataSharesForDataset(long datasetId, Set<GdsDataShareEvaluator> evaluators) {
        for (List<GdsDataShareEvaluator> dshEvaluators : zoneDataShares.values()) {
            for (GdsDataShareEvaluator dshEvaluator : dshEvaluators) {
                if (dshEvaluator.isInDataset(datasetId)) {
                    evaluators.add(dshEvaluator);
                }
            }
        }
    }

    private void collectDataSharesForProject(long projectId, Set<GdsDataShareEvaluator> evaluators) {
        for (List<GdsDataShareEvaluator> dshEvaluators : zoneDataShares.values()) {
            for (GdsDataShareEvaluator dshEvaluator : dshEvaluators) {
                if (dshEvaluator.isInProject(projectId)) {
                    evaluators.add(dshEvaluator);
                }
            }
        }
    }

    private void collectDataShares(List<Long> projectIds, List<Long> datasetIds, List<Long> dataShareIds, Set<GdsDataShareEvaluator> evaluators) {
        if (projectIds != null) {
            for (Long projectId : projectIds) {
                collectDataSharesForProject(projectId, evaluators);
            }
        }

        if (datasetIds != null) {
            for (Long datasetId : datasetIds) {
                collectDataSharesForDataset(datasetId, evaluators);
            }
        }

        if (dataShareIds != null) {
            for (Long dataShareId : dataShareIds) {
                GdsDataShareEvaluator evaluator = getDataShareEvaluator(dataShareId);

                if (evaluator != null) {
                    evaluators.add(evaluator);
                }
            }
        }
    }

    private Set<String> getAllAccessTypes(RangerServiceDefHelper serviceDefHelper) {
        Set<String> ret = new HashSet<>();

        for (RangerAccessTypeDef accessTypeDef : serviceDefHelper.getServiceDef().getAccessTypes()) {
            ret.add(accessTypeDef.getName());
        }

        return ret;
    }

    static class SharedResourceIter implements Iterator<GdsSharedResourceEvaluator> {
        private final Iterator<GdsDataShareEvaluator>      dataShareIter;
        private       Iterator<GdsSharedResourceEvaluator> sharedResourceIter = Collections.emptyIterator();
        private       GdsSharedResourceEvaluator           nextResource       = null;

        SharedResourceIter(Set<GdsDataShareEvaluator> evaluators) {
            if (evaluators == null || evaluators.isEmpty()) {
                dataShareIter = Collections.emptyIterator();
            } else if (evaluators.size() == 1) {
                dataShareIter = evaluators.iterator();
            } else {
                List<GdsDataShareEvaluator> list = new ArrayList<>(evaluators);

                list.sort(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);

                dataShareIter = list.iterator();
            }

            setNext();
        }

        @Override
        public boolean hasNext() {
            return nextResource != null;
        }

        @Override
        public GdsSharedResourceEvaluator next() {
            GdsSharedResourceEvaluator ret = nextResource;

            if (ret != null) {
                setNext();
            }

            return ret;
        }

        private void setNext() {
            if (!sharedResourceIter.hasNext()) {
                while (dataShareIter.hasNext()) {
                    GdsDataShareEvaluator dataShareEvaluator = dataShareIter.next();

                    sharedResourceIter = dataShareEvaluator.getSharedResourceEvaluators().iterator();

                    if (sharedResourceIter.hasNext()) {
                        break;
                    }
                }
            }

            nextResource = sharedResourceIter.hasNext() ? sharedResourceIter.next() : null;
        }
    }
}

/*
     dataShare-1 ----------------------- dataset-1 ---
           resource-11                /                \
           resource-12               /                  \
                                    /                    \
     dataShare-2 -------------------|                    | ---- project-1
           resource-21               \                  /
           resource-22                \                /
                                       -- dataset-2---
                                      /
     dataShare-3 ---------------------
           resource-31

     dataShare-4 ------------------------- dataset-3 --------- project-2
           resource-41

     dataShare-5 ------------------------- dataset-4
           resource-51
 */