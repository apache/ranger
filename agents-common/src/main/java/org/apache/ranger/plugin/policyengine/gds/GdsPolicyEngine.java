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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerResourceEvaluatorsRetriever;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;


public class GdsPolicyEngine {
    private static final Logger LOG = LoggerFactory.getLogger(GdsPolicyEngine.class);

    public static final String GDS_SERVICE_NAME         = "_gds";
    public static final String RESOURCE_NAME_DATASET_ID = "dataset-id";
    public static final String RESOURCE_NAME_PROJECT_ID = "project-id";

    private final ServiceGdsInfo                   gdsInfo;
    private final Set<String>                      allAccessTypes;
    private final Map<Long, GdsProjectEvaluator>   projects      = new HashMap<>();
    private final Map<Long, GdsDatasetEvaluator>   datasets      = new HashMap<>();
    private final Map<Long, GdsDataShareEvaluator> dataShares    = new HashMap<>();
    private final Map<String, GdsZoneResources>    zoneResources = new HashMap<>();

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

        final GdsAccessResult ret;
        final boolean         isAnyAccess = request.isAccessTypeAny();

        try {
            if (isAnyAccess) {
                RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), allAccessTypes);
                RangerAccessRequestUtil.setIsAnyAccessInContext(request.getContext(), Boolean.TRUE);
            }

            Collection<GdsDataShareEvaluator> dataShares = getDataShareEvaluators(request);

            if (!dataShares.isEmpty()) {
                ret = new GdsAccessResult();

                Set<Long> datasetIds = new HashSet<>();

                dataShares.forEach(e -> e.collectDatasets(request, ret, datasetIds));

                if (!datasetIds.isEmpty()) {
                    Set<Long> projectIds = new HashSet<>();

                    evaluateDatasetPolicies(datasetIds, request, ret, projectIds);

                    if (!projectIds.isEmpty()) {
                        evaluateProjectPolicies(projectIds, request, ret);
                    }
                }
            } else {
                ret = null;
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

        getDataShareEvaluators(request).forEach(e -> e.getResourceACLs(request, ret));

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
        Set<GdsDataShareEvaluator> dshEvaluators = new TreeSet<>(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);

        collectDataSharesForDataset(datasetId, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getProjectResources(long projectId) {
        Set<GdsDataShareEvaluator> dshEvaluators = new TreeSet<>(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);

        collectDataSharesForProject(projectId, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getDataShareResources(long dataShareId) {
        GdsDataShareEvaluator      dshEvaluator  = dataShares.get(dataShareId);
        Set<GdsDataShareEvaluator> dshEvaluators = dshEvaluator == null ? Collections.emptySet() : Collections.singleton(dshEvaluator);

        return new SharedResourceIter(dshEvaluators);
    }

    public Iterator<GdsSharedResourceEvaluator> getResources(List<Long> projectIds, List<Long> datasetIds, List<Long> dataShareIds) {
        Set<GdsDataShareEvaluator> dshEvaluators = new TreeSet<>(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);

        collectDataShares(projectIds, datasetIds, dataShareIds, dshEvaluators);

        return new SharedResourceIter(dshEvaluators);
    }


    private void init(RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        LOG.debug("==> RangerGdsPolicyEngine.init()");

        preprocess(serviceDefHelper);

        RangerServiceDef          gdsServiceDef = gdsInfo.getGdsServiceDef();
        RangerPolicyEngineOptions options       = new RangerPolicyEngineOptions(pluginContext.getConfig().getPolicyEngineOptions(), new RangerServiceDefHelper(gdsServiceDef, false));

        gdsInfo.getProjects().forEach(project -> projects.put(project.getId(), new GdsProjectEvaluator(project, gdsServiceDef, options)));

        gdsInfo.getDatasets().forEach(dataset -> datasets.put(dataset.getId(), new GdsDatasetEvaluator(dataset, gdsServiceDef, options)));

        gdsInfo.getDataShares().forEach(dataShare -> dataShares.put(dataShare.getId(), new GdsDataShareEvaluator(dataShare, serviceDefHelper)));

        gdsInfo.getDshids().forEach(dshid -> {
            if (dshid.getStatus() == RangerGds.GdsShareStatus.ACTIVE) {
                GdsDataShareEvaluator dshEvaluator = dataShares.get(dshid.getDataShareId());

                if (dshEvaluator != null) {
                    GdsDatasetEvaluator datasetEvaluator = datasets.get(dshid.getDatasetId());

                    if (datasetEvaluator != null) {
                        GdsDshidEvaluator dshidEvaluator = new GdsDshidEvaluator(dshid, datasetEvaluator);

                        dshEvaluator.addDshidEvaluator(dshidEvaluator);
                    } else {
                        LOG.error("RangerGdsPolicyEngine(): invalid datasetId in dshid: {}. Ignored", dshid);
                    }
                } else {
                    LOG.error("RangerGdsPolicyEngine(): invalid dataShareId in dshid: {}. Ignored", dshid);
                }
            } else {
                LOG.error("RangerGdsPolicyEngine(): dshid is not active {}. Ignored", dshid);
            }
        });

        gdsInfo.getDips().forEach(dip -> {
            if (dip.getStatus() == RangerGds.GdsShareStatus.ACTIVE) {
                GdsDatasetEvaluator datasetEvaluator = datasets.get(dip.getDatasetId());

                if (datasetEvaluator != null) {
                    GdsProjectEvaluator projectEvaluator = projects.get(dip.getProjectId());

                    if (projectEvaluator != null) {
                        GdsDipEvaluator dipEvaluator = new GdsDipEvaluator(dip, projectEvaluator);

                        datasetEvaluator.addDipEvaluator(dipEvaluator);
                    } else {
                        LOG.error("RangerGdsPolicyEngine(): invalid projectId in dip: {}. Ignored", dip);
                    }
                } else {
                    LOG.error("RangerGdsPolicyEngine(): invalid datasetId in dip: {}. Ignored", dip);
                }
            } else {
                LOG.error("RangerGdsPolicyEngine(): dip is not active {}. Ignored", dip);
            }
        });

        // purge dataShares that are not part of any dataset
        dataShares.values().removeIf(evaluator -> CollectionUtils.isEmpty(evaluator.getDshidEvaluators()));

        Map<String, List<GdsSharedResourceEvaluator>> zoneResEvaluators = new HashMap<>();

        gdsInfo.getResources().forEach(resource -> {
            GdsDataShareEvaluator dshEvaluator = dataShares.get(resource.getDataShareId());

            if (dshEvaluator != null) {
                GdsSharedResourceEvaluator evaluator = new GdsSharedResourceEvaluator(resource, dshEvaluator.getDefaultAccessTypes(), serviceDefHelper, pluginContext);

                dshEvaluator.addResourceEvaluator(evaluator);

                zoneResEvaluators.computeIfAbsent(dshEvaluator.getZoneName(), k -> new ArrayList<>()).add(evaluator);
            }
        });

        zoneResEvaluators.forEach((zoneName, evaluators) -> zoneResources.put(zoneName, new GdsZoneResources(zoneName, evaluators, serviceDefHelper, pluginContext)));

        LOG.debug("<== RangerGdsPolicyEngine.init()");
    }

    private void preprocess(RangerServiceDefHelper serviceDefHelper) {
        if (gdsInfo.getProjects() == null) {
            gdsInfo.setProjects(Collections.emptyList());
        }

        if (gdsInfo.getDatasets() == null) {
            gdsInfo.setDatasets(Collections.emptyList());
        }

        if (gdsInfo.getDataShares() == null) {
            gdsInfo.setDataShares(Collections.emptyList());
        } else {
            gdsInfo.getDataShares().stream().filter(dsh -> dsh.getZoneName() == null).forEach(dsh -> dsh.setZoneName(StringUtils.EMPTY));
        }

        if (gdsInfo.getResources() == null) {
            gdsInfo.setResources(Collections.emptyList());
        }

        if (gdsInfo.getDshids() == null) {
            gdsInfo.setDshids(Collections.emptyList());
        }

        if (gdsInfo.getDips() == null) {
            gdsInfo.setDips(Collections.emptyList());
        }

        RangerServiceDef gdsServiceDef = gdsInfo.getGdsServiceDef();

        // populate accessTypes in GDS servicedef with implied accessTypes from the service
        for (RangerAccessTypeDef gdsAccessTypeDef : gdsServiceDef.getAccessTypes()) {
            Collection<String> impliedGrants = serviceDefHelper.getImpliedAccessGrants().get(gdsAccessTypeDef.getName());

            if (impliedGrants != null) {
                gdsAccessTypeDef.getImpliedGrants().addAll(impliedGrants);
            }
        }

        gdsServiceDef.getAccessTypes().addAll(serviceDefHelper.getServiceDef().getAccessTypes());
    }

    private Collection<GdsDataShareEvaluator> getDataShareEvaluators(RangerAccessRequest request) {
        LOG.debug("==> RangerGdsPolicyEngine.getDataShareEvaluators({})", request);

        final Collection<GdsDataShareEvaluator> ret;

        if (!dataShares.isEmpty()) {
            Set<String> zoneNames = RangerAccessRequestUtil.getResourceZoneNamesFromContext(request.getContext());

            if (zoneNames == null || zoneNames.isEmpty()) {
                zoneNames = Collections.singleton(StringUtils.EMPTY); // unzoned
            } else if (zoneNames.size() > 1 && !request.isAccessTypeAny()) {
                LOG.warn("RangerGdsPolicyEngine.getDataShareEvaluators(): resource matches multiple zones and accessType is not ANY - ignored. resource={}, zones={}", request.getResource(), zoneNames);

                zoneNames = Collections.emptySet();
            }

            ret = new TreeSet<>(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR);

            zoneNames.stream().map(zoneResources::get).filter(Objects::nonNull).forEach(zr -> zr.collectMatchingDataShares(request, ret));
        } else {
            ret = Collections.emptyList();
        }

        LOG.debug("<== RangerGdsPolicyEngine.getDataShareEvaluators({}): {}", request, ret);

        return ret;
    }

    private void evaluateDatasetPolicies(Set<Long> datasetIds, RangerAccessRequest request, GdsAccessResult result, Set<Long> projectIds) {
        datasetIds.stream().map(datasets::get).filter(Objects::nonNull).sorted(GdsDatasetEvaluator.EVAL_ORDER_COMPARATOR).forEach(e -> e.evaluate(request, result, projectIds));
    }

    private void evaluateProjectPolicies(Set<Long> projectIds, RangerAccessRequest request, GdsAccessResult result) {
        projectIds.stream().map(projects::get).filter(Objects::nonNull).sorted(GdsProjectEvaluator.EVAL_ORDER_COMPARATOR).forEach(e -> e.evaluate(request, result));
    }

    private GdsDatasetEvaluator getDatasetEvaluator(String dsName) {
        return datasets.values().stream().filter(e -> StringUtils.equals(e.getName(), dsName)).findFirst().orElse(null);
    }

    private GdsProjectEvaluator getProjectEvaluator(String projectName) {
        return projects.values().stream().filter(e -> StringUtils.equals(e.getName(), projectName)).findFirst().orElse(null);
    }

    private void collectDataSharesForDataset(Long datasetId, Set<GdsDataShareEvaluator> evaluators) {
        dataShares.values().stream().filter(e -> e.isInDataset(datasetId)).forEach(evaluators::add);
    }

    private void collectDataSharesForProject(Long projectId, Set<GdsDataShareEvaluator> evaluators) {
        dataShares.values().stream().filter(e -> e.isInProject(projectId)).forEach(evaluators::add);
    }

    private void collectDataShares(List<Long> projectIds, List<Long> datasetIds, List<Long> dataShareIds, Set<GdsDataShareEvaluator> evaluators) {
        if (projectIds != null) {
            projectIds.forEach(projectId -> collectDataSharesForProject(projectId, evaluators));
        }

        if (datasetIds != null) {
            datasetIds.forEach(datasetId -> collectDataSharesForDataset(datasetId, evaluators));
        }

        if (dataShareIds != null) {
            dataShareIds.stream().map(dataShares::get).filter(Objects::nonNull).forEach(evaluators::add);
        }
    }

    private Set<String> getAllAccessTypes(RangerServiceDefHelper serviceDefHelper) {
        return serviceDefHelper.getServiceDef().getAccessTypes().stream().map(RangerAccessTypeDef::getName).collect(Collectors.toSet());
    }

    private class GdsZoneResources {
        private final String                                                      zoneName;
        private final Map<String, RangerResourceTrie<GdsSharedResourceEvaluator>> resourceTries;

        public GdsZoneResources(String zoneName, List<GdsSharedResourceEvaluator> evaluators, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
            this.zoneName      = zoneName;
            this.resourceTries = createResourceTries(evaluators, serviceDefHelper, pluginContext);
        }

        public String getZoneName() { return zoneName; }

        public void collectMatchingDataShares(RangerAccessRequest request, Collection<GdsDataShareEvaluator> dshEvaluators) {
            Collection<GdsSharedResourceEvaluator> evaluators = RangerResourceEvaluatorsRetriever.getEvaluators(resourceTries, request.getResource().getAsMap(), request.getResourceElementMatchingScopes());

            if (evaluators != null) {
                evaluators.stream().filter(e -> e.isAllowed(request)).map(GdsSharedResourceEvaluator::getDataShareId).distinct().map(dataShares::get).filter(Objects::nonNull).forEach(dshEvaluators::add);
            }
        }

        private Map<String, RangerResourceTrie<GdsSharedResourceEvaluator>> createResourceTries(List<GdsSharedResourceEvaluator> evaluators, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
            Map<String, RangerResourceTrie<GdsSharedResourceEvaluator>> ret = new HashMap<>();

            for (RangerResourceDef resourceDef : serviceDefHelper.getServiceDef().getResources()) {
                ret.put(resourceDef.getName(), new RangerResourceTrie<>(resourceDef, evaluators, true, pluginContext));
            }

            return ret;
        }
    }

    static class SharedResourceIter implements Iterator<GdsSharedResourceEvaluator> {
        private final Iterator<GdsDataShareEvaluator>      dataShareIter;
        private       Iterator<GdsSharedResourceEvaluator> sharedResourceIter = Collections.emptyIterator();
        private       GdsSharedResourceEvaluator           nextResource       = null;

        SharedResourceIter(Set<GdsDataShareEvaluator> evaluators) {
            if (evaluators == null) {
                dataShareIter = Collections.emptyIterator();
            } else {
                dataShareIter = evaluators.iterator();
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

                    sharedResourceIter = dataShareEvaluator.getResourceEvaluators().iterator();

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