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

package org.apache.ranger.rest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.GdsDBStore;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGds.DataShareInDatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DataShareSummary;
import org.apache.ranger.plugin.model.RangerGds.DatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DatasetsSummary;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerGrant;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicyHeader;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.model.RangerPrincipal.PrincipalType;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDataShareService;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.service.RangerGdsProjectService;
import org.apache.ranger.service.RangerGdsSharedResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Path("gds")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class GdsREST {
    private static final Logger LOG      = LoggerFactory.getLogger(GdsREST.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("rest.GdsREST");

    public static final String GDS_POLICY_EXPR_CONDITION = "expression";

    private static final String            PRINCIPAL_TYPE_USER             = RangerPrincipal.PrincipalType.USER.name().toLowerCase();
    private static final String            PRINCIPAL_TYPE_GROUP            = RangerPrincipal.PrincipalType.GROUP.name().toLowerCase();
    private static final String            PRINCIPAL_TYPE_ROLE             = RangerPrincipal.PrincipalType.ROLE.name().toLowerCase();
    private static final String            DEFAULT_PRINCIPAL_TYPE          = PRINCIPAL_TYPE_USER;
    private static final RangerAdminConfig config                          = RangerAdminConfig.getInstance();
    private static final int               SHARED_RESOURCES_MAX_BATCH_SIZE = config.getInt("ranger.admin.rest.gds.shared.resources.max.batch.size", 100);

    @Autowired
    GdsDBStore gdsStore;

    @Autowired
    RangerGdsDatasetService datasetService;

    @Autowired
    RangerGdsProjectService projectService;

    @Autowired
    RangerGdsDataShareService dataShareService;

    @Autowired
    RangerGdsSharedResourceService sharedResourceService;

    @Autowired
    RangerGdsDataShareInDatasetService dshidService;

    @Autowired
    RangerGdsDatasetInProjectService dipService;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    ServiceUtil serviceUtil;

    @Autowired
    ServiceDBStore serviceDBStore;

    @Autowired
    AssetMgr assetMgr;

    @POST
    @Path("/dataset")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_DATASET + "\")")
    public RangerDataset createDataset(RangerDataset dataset) {
        LOG.debug("==> GdsREST.createDataset({})", dataset);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createDataset(datasetName=" + dataset.getName() + ")");
            }

            ret = gdsStore.createDataset(dataset);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createDataset({}) failed", dataset, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.createDataset({}): {}", dataset, ret);

        return ret;
    }

    @POST
    @Path("/dataset/{id}/resources/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_SHARED_RESOURCES + "\")")
    public List<RangerSharedResource> addDatasetResources(@PathParam("id") Long datasetId, @PathParam("serviceName") String serviceName, @QueryParam("zoneName") @DefaultValue("") String zoneName, List<RangerSharedResource> resources) {
        LOG.debug("==> GdsREST.addDatasetResources(datasetId={} serviceName={} zoneNam={} resources={})", datasetId, serviceName, zoneName, resources);

        List<RangerSharedResource> ret  = new ArrayList<>();
        RangerPerfTracer           perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDatasetResources(datasetId=" + datasetId + ")");
            }

            Long serviceId   = validateAndGetServiceId(serviceName);
            Long zoneId      = validateAndGetZoneId(zoneName);
            Long dataShareId = getOrCreateDataShare(datasetId, serviceId, zoneId, serviceName);

            // Add resources to DataShare
            for (RangerSharedResource resource : resources) {
                resource.setDataShareId(dataShareId);

                RangerSharedResource rangerSharedResource = addSharedResource(resource);

                ret.add(rangerSharedResource);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("GdsREST.addDatasetResources(datasetId={} serviceName={} zoneName={} resources={}) failed!", datasetId, serviceName, zoneName, resources, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addDatasetResources(RangerSharedResources={})", ret);

        return ret;
    }

    @POST
    @Path("/dataset/{id}/datashare")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASHARE_IN_DATASET + "\")")
    public List<RangerDataShareInDataset> addDataSharesInDataset(@PathParam("id") Long datasetId, List<RangerDataShareInDataset> dataSharesInDataset) {
        LOG.debug("==> GdsREST.addDataSharesInDataset({}, {})", datasetId, dataSharesInDataset);

        List<RangerDataShareInDataset> ret;
        RangerPerfTracer               perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDataSharesInDataset(" + datasetId + ")");
            }

            if (CollectionUtils.isNotEmpty(dataSharesInDataset)) {
                for (RangerDataShareInDataset dshInDs : dataSharesInDataset) {
                    if (dshInDs == null || (dshInDs.getDatasetId() == null)) {
                        throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "missing datasetID", false);
                    } else if (!dshInDs.getDatasetId().equals(datasetId)) {
                        throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "incorrect datasetId=" + datasetId, false);
                    }
                }
            } else {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "empty dataShareInDataset list", false);
            }

            ret = gdsStore.addDataSharesInDataset(dataSharesInDataset);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addDataShareInDataset({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addDataSharesInDataset({}, {}): ret={}", datasetId, dataSharesInDataset, ret);

        return ret;
    }

    @PUT
    @Path("/dataset/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASET + "\")")
    public RangerDataset updateDataset(@PathParam("id") Long datasetId, RangerDataset dataset) {
        LOG.debug("==> GdsREST.updateDataset({}, {})", datasetId, dataset);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataset(datasetId=" + datasetId + ", datasetName=" + dataset.getName() + ")");
            }

            dataset.setId(datasetId);

            ret = gdsStore.updateDataset(dataset);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDataset({}, {}) failed", datasetId, dataset, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDataset({}, {}): {}", datasetId, dataset, ret);

        return ret;
    }

    @DELETE
    @Path("/dataset/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_DATASET + "\")")
    public void deleteDataset(@PathParam("id") Long datasetId, @Context HttpServletRequest request) {
        LOG.debug("==> deleteDataset({})", datasetId);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDataset(datasetId=" + datasetId + ")");
            }

            boolean forceDelete = Boolean.parseBoolean(request.getParameter("forceDelete"));

            gdsStore.deleteDataset(datasetId, forceDelete);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteDataset({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== deleteDataset({})", datasetId);
    }

    @GET
    @Path("/dataset/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET + "\")")
    public RangerDataset getDataset(@PathParam("id") Long datasetId) {
        LOG.debug("==> GdsREST.getDataset({})", datasetId);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataset(datasetId=" + datasetId + ")");
            }

            ret = gdsStore.getDataset(datasetId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no dataset with id=" + datasetId, false);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDataset({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDataset({}): {}", datasetId, ret);

        return ret;
    }

    @GET
    @Path("/dataset")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASETS + "\")")
    public PList<RangerDataset> searchDatasets(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDatasets()");

        PList<RangerDataset> ret;
        RangerPerfTracer     perf   = null;
        SearchFilter         filter = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasets()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            extractDatasetMultiValueParams(request, filter);

            ret = gdsStore.searchDatasets(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchDatasets({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.listDatasets(): {}", ret);

        return ret;
    }

    @GET
    @Path("/dataset/names")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LIST_DATASET_NAMES + "\")")
    public PList<String> listDatasetNames(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.listDatasetNames()");

        PList<String>    ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.listDatasetNames()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            ret = gdsStore.getDatasetNames(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("listDatasetNames({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.listDatasetNames(): {}", ret);

        return ret;
    }

    @GET
    @Path("/dataset/summary")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_SUMMARY + "\")")
    public PList<DatasetSummary> getDatasetSummary(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDatasetSummary()");

        PList<DatasetSummary> ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetSummary()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            extractDatasetMultiValueParams(request, filter);

            ret    = gdsStore.getDatasetSummary(filter);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Throwable ex) {
            LOG.error("getDatasetSummary({}) failed", filter, ex);

            throw restErrorUtil.createRESTException(ex.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDatasetSummary()");

        return ret;
    }

    @GET
    @Path("/dataset/enhancedsummary")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_SUMMARY + "\")")
    public DatasetsSummary getEnhancedDatasetSummary(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getEnhancedDatasetSummary()");

        DatasetsSummary ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getEnhancedDatasetSummary()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            extractDatasetMultiValueParams(request, filter);

            ret    = gdsStore.getEnhancedDatasetSummary(filter);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Throwable ex) {
            LOG.error("getEnhancedDatasetSummary({}) failed", filter, ex);

            throw restErrorUtil.createRESTException(ex.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getEnhancedDatasetSummary()");

        return ret;
    }

    @POST
    @Path(("/dataset/{id}/policy"))
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DATASET_POLICY + "\")")
    public RangerPolicy addDatasetPolicy(@PathParam("id") Long datasetId, RangerPolicy policy) {
        LOG.debug("==> GdsREST.addDatasetPolicy({}, {})", datasetId, policy);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDatasetPolicy()");

        try {
            ret = gdsStore.addDatasetPolicy(datasetId, policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addDatasetPolicy({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addDatasetPolicy({}, {}): ret={}", datasetId, policy, ret);

        return ret;
    }

    @PUT
    @Path(("/dataset/{id}/policy/{policyId}"))
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DATASET_POLICY + "\")")
    public RangerPolicy updateDatasetPolicy(@PathParam("id") Long datasetId, @PathParam("policyId") Long policyId, RangerPolicy policy) {
        LOG.debug("==> GdsREST.updateDatasetPolicy({}, {})", datasetId, policy);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDatasetPolicy()");

        try {
            policy.setId(policyId);

            ret = gdsStore.updateDatasetPolicy(datasetId, policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDatasetPolicy({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDatasetPolicy({}, {}): ret={}", datasetId, policy, ret);

        return ret;
    }

    @DELETE
    @Path(("/dataset/{id}/policy/{policyId}"))
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DATASET_POLICY + "\")")
    public void deleteDatasetPolicy(@PathParam("id") Long datasetId, @PathParam("policyId") Long policyId) {
        LOG.debug("==> GdsREST.deleteDatasetPolicy({}, {})", datasetId, policyId);

        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDatasetPolicy()");

        try {
            gdsStore.deleteDatasetPolicy(datasetId, policyId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteDatasetPolicy({}, {}) failed", datasetId, policyId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.deleteDatasetPolicy({}, {})", datasetId, policyId);
    }

    @GET
    @Path(("/dataset/{id}/policy/{policyId}"))
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DATASET_POLICY + "\")")
    public RangerPolicy getDatasetPolicy(@PathParam("id") Long datasetId, @PathParam("policyId") Long policyId) {
        LOG.debug("==> GdsREST.getDatasetPolicy({}, {})", datasetId, policyId);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetPolicy()");

        try {
            ret = gdsStore.getDatasetPolicy(datasetId, policyId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDatasetPolicy({}, {}) failed", datasetId, policyId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDatasetPolicy({}, {}): ret={}", datasetId, policyId, ret);

        return ret;
    }

    @GET
    @Path(("/dataset/{id}/policy"))
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DATASET_POLICY + "\")")
    public List<RangerPolicy> getDatasetPolicies(@PathParam("id") Long datasetId, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDatasetPolicies({})", datasetId);

        List<RangerPolicy> ret;
        RangerPerfTracer   perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetPolicies()");

        try {
            ret = gdsStore.getDatasetPolicies(datasetId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDatasetPolicies({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDatasetPolicies({}): ret={}", datasetId, ret);

        return ret;
    }

    @POST
    @Path("/project")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_PROJECT + "\")")
    public RangerProject createProject(RangerProject project) {
        LOG.debug("==> GdsREST.createProject({})", project);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createProject(projectName=" + project.getName() + ")");
            }

            ret = gdsStore.createProject(project);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createProject({}) failed", project, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.createProject({}): {}", project, ret);

        return ret;
    }

    @PUT
    @Path("/project/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_PROJECT + "\")")
    public RangerProject updateProject(@PathParam("id") Long projectId, RangerProject project) {
        LOG.debug("==> GdsREST.updateProject({}, {})", projectId, project);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateProject(projectId=" + projectId + ", projectName=" + project.getName() + ")");
            }

            project.setId(projectId);

            ret = gdsStore.updateProject(project);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateProject({}, {}) failed", projectId, project, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateProject({}, {}): {}", projectId, project, ret);

        return ret;
    }

    @DELETE
    @Path("/project/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_PROJECT + "\")")
    public void deleteProject(@PathParam("id") Long projectId, @Context HttpServletRequest request) {
        LOG.debug("==> deleteProject({})", projectId);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteProject(projectId=" + projectId + ")");
            }

            boolean forceDelete = Boolean.parseBoolean(request.getParameter("forceDelete"));

            gdsStore.deleteProject(projectId, forceDelete);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteProject({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== deleteProject({})", projectId);
    }

    @GET
    @Path("/project/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_PROJECT + "\")")
    public RangerProject getProject(@PathParam("id") Long projectId) {
        LOG.debug("==> GdsREST.getProject({})", projectId);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getProject(projectId=" + projectId + ")");
            }

            ret = gdsStore.getProject(projectId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no project with id=" + projectId, false);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getProject({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getProject({}): {}", projectId, ret);

        return ret;
    }

    @GET
    @Path("/project")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_PROJECTS + "\")")
    public PList<RangerProject> searchProjects(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchProjects()");

        PList<RangerProject> ret;
        RangerPerfTracer     perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
        SearchFilter         filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, projectService.sortFields);

            ret = gdsStore.searchProjects(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchProjects({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.listProjects(): {}", ret);

        return ret;
    }

    @GET
    @Path("/project/names")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LIST_PROJECT_NAMES + "\")")
    public PList<String> listProjectNames(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.listProjectNames()");

        PList<String>    ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, projectService.sortFields);

            ret = gdsStore.getProjectNames(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("listProjectNames({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.listProjectNames(): {}", ret);

        return ret;
    }

    @POST
    @Path(("/project/{id}/policy"))
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.PROJECT_POLICY + "\")")
    public RangerPolicy addProjectPolicy(@PathParam("id") Long projectId, RangerPolicy policy) {
        LOG.debug("==> GdsREST.addProjectPolicy({}, {})", projectId, policy);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addProjectPolicy()");

        try {
            ret = gdsStore.addProjectPolicy(projectId, policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addProjectPolicy({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addProjectPolicy({}, {}): ret={}", projectId, policy, ret);

        return ret;
    }

    @PUT
    @Path(("/project/{id}/policy/{policyId}"))
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.PROJECT_POLICY + "\")")
    public RangerPolicy updateProjectPolicy(@PathParam("id") Long projectId, @PathParam("policyId") Long policyId, RangerPolicy policy) {
        LOG.debug("==> GdsREST.updateProjectPolicy({}, {})", projectId, policy);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateProjectPolicy()");

        try {
            policy.setId(policyId);

            ret = gdsStore.updateProjectPolicy(projectId, policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateProjectPolicy({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateProjectPolicy({}, {}): ret={}", projectId, policy, ret);

        return ret;
    }

    @DELETE
    @Path(("/project/{id}/policy/{policyId}"))
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.PROJECT_POLICY + "\")")
    public void deleteProjectPolicy(@PathParam("id") Long projectId, @PathParam("policyId") Long policyId) {
        LOG.debug("==> GdsREST.deleteProjectPolicy({}, {})", projectId, policyId);

        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteProjectPolicy()");

        try {
            gdsStore.deleteProjectPolicy(projectId, policyId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteProjectPolicy({}, {}) failed", projectId, policyId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.deleteProjectPolicy({}, {})", projectId, policyId);
    }

    @GET
    @Path(("/project/{id}/policy/{policyId}"))
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.PROJECT_POLICY + "\")")
    public RangerPolicy getProjectPolicy(@PathParam("id") Long projectId, @PathParam("policyId") Long policyId) {
        LOG.debug("==> GdsREST.getProjectPolicy({}, {})", projectId, policyId);

        RangerPolicy     ret;
        RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getProjectPolicy()");

        try {
            ret = gdsStore.getProjectPolicy(projectId, policyId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getProjectPolicy({}, {}) failed", projectId, policyId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getProjectPolicy({}, {}): ret={}", projectId, policyId, ret);

        return ret;
    }

    @GET
    @Path(("/project/{id}/policy"))
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.PROJECT_POLICY + "\")")
    public List<RangerPolicy> getProjectPolicies(@PathParam("id") Long projectId, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getProjectPolicies({})", projectId);

        List<RangerPolicy> ret;
        RangerPerfTracer   perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getProjectPolicies()");

        try {
            ret = gdsStore.getProjectPolicies(projectId);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getProjectPolicies({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getProjectPolicies({}): ret={}", projectId, ret);

        return ret;
    }

    @POST
    @Path("/datashare")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_DATA_SHARE + "\")")
    public RangerDataShare createDataShare(RangerDataShare dataShare) {
        LOG.debug("==> GdsREST.createDataShare({})", dataShare);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createDataShare(" + dataShare + ")");
            }

            ret = gdsStore.createDataShare(dataShare);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createDataShare({}) failed", dataShare, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.createDataShare({}): {}", dataShare, ret);

        return ret;
    }

    @PUT
    @Path("/datashare/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATA_SHARE + "\")")
    public RangerDataShare updateDataShare(@PathParam("id") Long dataShareId, RangerDataShare dataShare) {
        LOG.debug("==> GdsREST.updateDataShare({}, {})", dataShareId, dataShare);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataShare(" + dataShare + ")");
            }

            dataShare.setId(dataShareId);

            ret = gdsStore.updateDataShare(dataShare);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDataShare({}, {}) failed", dataShareId, dataShare, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDataShare({}, {}): {}", dataShareId, dataShare, ret);

        return ret;
    }

    @DELETE
    @Path("/datashare/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_DATA_SHARE + "\")")
    public void deleteDataShare(@PathParam("id") Long dataShareId, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.deleteDataShare({})", dataShareId);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDataShare(" + dataShareId + ")");
            }

            String  forceDeleteStr = request.getParameter("forceDelete");
            boolean forceDelete    = !StringUtils.isEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr);

            gdsStore.deleteDataShare(dataShareId, forceDelete);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteDataShare({}) failed", dataShareId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.deleteDataShare({})", dataShareId);
    }

    @GET
    @Path("/datashare/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATA_SHARE + "\")")
    public RangerDataShare getDataShare(@PathParam("id") Long dataShareId) {
        LOG.debug("==> GdsREST.getDataShare({})", dataShareId);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataShare(" + dataShareId + ")");
            }

            ret = gdsStore.getDataShare(dataShareId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no dataShare with id=" + dataShareId, false);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDataShare({}) failed", dataShareId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDataShare({}): {}", dataShareId, ret);

        return ret;
    }

    @GET
    @Path("/datashare")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATA_SHARES + "\")")
    public PList<RangerDataShare> searchDataShares(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDataShares()");

        PList<RangerDataShare> ret;
        RangerPerfTracer       perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShares()");
        SearchFilter           filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dataShareService.sortFields);

            ret = gdsStore.searchDataShares(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchDataShares({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.searchDataShares({}): {}", filter, ret);

        return ret;
    }

    @GET
    @Path("/datashare/summary")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATA_SHARE_SUMMARY + "\")")
    public PList<DataShareSummary> getDataShareSummary(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDataShareSummary()");

        PList<DataShareSummary> ret;
        RangerPerfTracer        perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataShareSummary()");

        try {
            SearchFilter filter = searchUtil.getSearchFilter(request, dataShareService.sortFields);

            ret = gdsStore.getDataShareSummary(filter);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Throwable ex) {
            LOG.error("getDataShareSummary() failed", ex);

            throw restErrorUtil.createRESTException(ex.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDataShareSummary(): {}", ret);

        return ret;
    }

    @POST
    @Path("/resource")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_SHARED_RESOURCE + "\")")
    public RangerSharedResource addSharedResource(RangerSharedResource resource) {
        LOG.debug("==> GdsREST.addSharedResource({})", resource);

        RangerSharedResource ret;
        RangerPerfTracer     perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addSharedResource(" + resource + ")");
            }

            List<RangerSharedResource> sharedResources = gdsStore.addSharedResources(Collections.singletonList(resource));

            ret = CollectionUtils.isNotEmpty(sharedResources) ? sharedResources.get(0) : null;
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addSharedResource({}) failed", resource, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addSharedResource({}): {}", resource, ret);

        return ret;
    }

    @POST
    @Path("/resources")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_SHARED_RESOURCES + "\")")
    public List<RangerSharedResource> addSharedResources(List<RangerSharedResource> resources) {
        LOG.debug("==> GdsREST.addSharedResources({})", resources);

        List<RangerSharedResource> ret;
        RangerPerfTracer           perf = null;

        try {
            if (resources.size() > SHARED_RESOURCES_MAX_BATCH_SIZE) {
                throw new Exception("addSharedResources batch size exceeded the configured limit: Maximum allowed is " + SHARED_RESOURCES_MAX_BATCH_SIZE);
            }

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addSharedResources(" + resources + ")");
            }

            ret = gdsStore.addSharedResources(resources);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addSharedResources({}) failed", resources, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addSharedResources({}): {}", resources, ret);

        return ret;
    }

    @PUT
    @Path("/resource/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SHARED_RESOURCE + "\")")
    public RangerSharedResource updateSharedResource(@PathParam("id") Long resourceId, RangerSharedResource resource) {
        LOG.debug("==> GdsREST.updateSharedResource({}, {})", resourceId, resource);

        RangerSharedResource ret;
        RangerPerfTracer     perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateSharedResource(" + resource + ")");
            }

            resource.setId(resourceId);

            ret = gdsStore.updateSharedResource(resource);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateSharedResource({}, {}) failed", resourceId, resource, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateSharedResource({}, {}): {}", resourceId, resource, ret);

        return ret;
    }

    @DELETE
    @Path("/resource/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.REMOVE_SHARED_RESOURCE + "\")")
    public void removeSharedResource(@PathParam("id") Long resourceId) {
        LOG.debug("==> GdsREST.removeSharedResource({})", resourceId);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeSharedResource(" + resourceId + ")");
            }

            gdsStore.removeSharedResources(Collections.singletonList(resourceId));
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("removeSharedResource({}) failed", resourceId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeSharedResource({})", resourceId);
    }

    @DELETE
    @Path("/resources")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.REMOVE_SHARED_RESOURCES + "\")")
    public void removeSharedResources(List<Long> resourceIds) {
        LOG.debug("==> GdsREST.removeSharedResources({})", resourceIds);

        RangerPerfTracer perf = null;

        try {
            if (resourceIds.size() > SHARED_RESOURCES_MAX_BATCH_SIZE) {
                throw new Exception("removeSharedResources batch size exceeded the configured limit: Maximum allowed is " + SHARED_RESOURCES_MAX_BATCH_SIZE);
            }

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeSharedResources(" + resourceIds + ")");
            }

            gdsStore.removeSharedResources(resourceIds);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("removeSharedResources({}) failed", resourceIds, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeSharedResources({})", resourceIds);
    }

    @GET
    @Path("/resource/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SHARED_RESOURCE + "\")")
    public RangerSharedResource getSharedResource(@PathParam("id") Long resourceId) {
        LOG.debug("==> GdsREST.getSharedResource({})", resourceId);

        RangerSharedResource ret;
        RangerPerfTracer     perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getSharedResource(" + resourceId + ")");
            }

            ret = gdsStore.getSharedResource(resourceId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no shared-resource with id=" + resourceId, false);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getSharedResource({}) failed", resourceId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getSharedResource({}): {}", resourceId, ret);

        return ret;
    }

    @GET
    @Path("/resource")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_SHARED_RESOURCES + "\")")
    public PList<RangerSharedResource> searchSharedResources(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchSharedResources()");

        PList<RangerSharedResource> ret;
        RangerPerfTracer            perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchSharedResources()");
        SearchFilter                filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, sharedResourceService.sortFields);

            ret = gdsStore.searchSharedResources(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchSharedResources({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.searchSharedResources({}): {}", filter, ret);

        return ret;
    }

    @POST
    @Path("/datashare/dataset")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset datasetData) {
        LOG.debug("==> GdsREST.addDataShareInDataset({})", datasetData);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDataShareInDataset(" + datasetData + ")");
            }

            ret = gdsStore.addDataShareInDataset(datasetData);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addDataShareInDataset({}) failed", datasetData, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addDataShareInDataset({})", datasetData);

        return ret;
    }

    @PUT
    @Path("/datashare/dataset/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset updateDataShareInDataset(@PathParam("id") Long id, RangerDataShareInDataset dataShareInDataset) {
        LOG.debug("==> GdsREST.updateDataShareInDataset({}, {})", id, dataShareInDataset);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataShareInDataset(" + dataShareInDataset + ")");
            }

            dataShareInDataset.setId(id);

            ret = gdsStore.updateDataShareInDataset(dataShareInDataset);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDataShareInDataset({}) failed", dataShareInDataset, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDataShareInDataset({}, {})", id, dataShareInDataset);

        return ret;
    }

    @DELETE
    @Path("/datashare/dataset/{id}")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.REMOVE_DATASHARE_IN_DATASET + "\")")
    public void removeDataShareInDataset(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.removeDatasetData({})", id);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeDatasetData(" + id + ")");
            }

            gdsStore.removeDataShareInDataset(id);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("removeDatasetData({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeDatasetData({})", id);
    }

    @GET
    @Path("/datashare/dataset/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset getDataShareInDataset(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.updateDataShareInDataset({})", id);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataShareInDataset(" + id + ")");
            }

            ret = gdsStore.getDataShareInDataset(id);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDataShareInDataset({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDataShareInDataset({}): ret={}", id, ret);

        return ret;
    }

    @GET
    @Path("/datashare/dataset")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASHARE_IN_DATASET + "\")")
    public PList<RangerDataShareInDataset> searchDataShareInDatasets(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDataShareInDatasets()");

        PList<RangerDataShareInDataset> ret;
        RangerPerfTracer                perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShareInDatasets()");
        SearchFilter                    filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dshidService.sortFields);

            ret = gdsStore.searchDataShareInDatasets(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchDataShareInDatasets({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.searchDataShareInDatasets({}): {}", filter, ret);

        return ret;
    }

    @GET
    @Path("/datashare/dataset/summary")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASHARE_IN_DATASET_SUMMARY + "\")")
    public PList<DataShareInDatasetSummary> getDshInDsSummary(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDshInDsSummary()");

        PList<DataShareInDatasetSummary> ret;
        SearchFilter                     filter = null;
        RangerPerfTracer                 perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDshInDsSummary()");

        try {
            filter = searchUtil.getSearchFilter(request, dshidService.sortFields);

            ret = gdsStore.getDshInDsSummary(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDshInDsSummary({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDshInDsSummary({}): {}", filter, ret);

        return ret;
    }

    @POST
    @Path("/dataset/project")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject addDatasetInProject(RangerDatasetInProject projectData) {
        LOG.debug("==> GdsREST.addDatasetInProject({})", projectData);

        RangerDatasetInProject ret;
        RangerPerfTracer       perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDatasetInProject(" + projectData + ")");
            }

            ret = gdsStore.addDatasetInProject(projectData);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("addDatasetInProject({}) failed", projectData, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addDatasetInProject({})", projectData);

        return ret;
    }

    @PUT
    @Path("/dataset/project/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject updateDatasetInProject(@PathParam("id") Long id, RangerDatasetInProject dataShareInProject) {
        LOG.debug("==> GdsREST.updateDatasetInProject({}, {})", id, dataShareInProject);

        RangerDatasetInProject ret;
        RangerPerfTracer       perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDatasetInProject(" + dataShareInProject + ")");
            }

            dataShareInProject.setId(id);

            ret = gdsStore.updateDatasetInProject(dataShareInProject);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDatasetInProject({}) failed", dataShareInProject, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDatasetInProject({}, {})", id, dataShareInProject);

        return ret;
    }

    @DELETE
    @Path("/dataset/project/{id}")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.REMOVE_DATASET_IN_PROJECT + "\")")
    public void removeDatasetInProject(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.removeProjectData({})", id);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeProjectData(" + id + ")");
            }

            gdsStore.removeDatasetInProject(id);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("removeProjectData({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeProjectData({})", id);
    }

    @GET
    @Path("/dataset/project/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject getDatasetInProject(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.getDatasetInProject({})", id);

        RangerDatasetInProject ret;
        RangerPerfTracer       perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetInProject(" + id + ")");
            }

            ret = gdsStore.getDatasetInProject(id);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDatasetInProject({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDatasetInProject({}): ret={}", id, ret);

        return ret;
    }

    @GET
    @Path("/dataset/project")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASET_IN_PROJECT + "\")")
    public PList<RangerDatasetInProject> searchDatasetInProjects(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDatasetInProjects()");

        PList<RangerDatasetInProject> ret;
        RangerPerfTracer              perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasetInProjects()");
        SearchFilter                  filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dipService.sortFields);

            ret = gdsStore.searchDatasetInProjects(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("searchDatasetInProjects({}) failed", filter, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.searchDatasetInProjects({}): {}", filter, ret);

        return ret;
    }

    @GET
    @Path("/download/{serviceName}")
    @Produces("application/json")
    public ServiceGdsInfo getServiceGdsInfoIfUpdated(@PathParam("serviceName") String serviceName, @QueryParam("lastKnownGdsVersion") @DefaultValue("-1") Long lastKnownVersion, @QueryParam("lastActivationTime") @DefaultValue("0") Long lastActivationTime, @QueryParam("pluginId") String pluginId, @QueryParam("clusterName") @DefaultValue("") String clusterName, @QueryParam("pluginCapabilities") @DefaultValue("") String pluginCapabilities, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities);

        ServiceGdsInfo ret               = null;
        int            httpCode          = HttpServletResponse.SC_OK;
        Long           downloadedVersion = null;
        String         logMsg            = null;

        try {
            bizUtil.failUnauthenticatedDownloadIfNotAllowed();

            boolean isValid = serviceUtil.isValidateHttpsAuthentication(serviceName, request);

            if (isValid) {
                ret = gdsStore.getGdsInfoIfUpdated(serviceName, lastKnownVersion);

                if (ret == null) {
                    downloadedVersion = lastKnownVersion;
                    httpCode          = HttpServletResponse.SC_NOT_MODIFIED;
                    logMsg            = "No change since last update";
                } else {
                    downloadedVersion = ret.getGdsVersion();
                }
            }
        } catch (WebApplicationException webException) {
            httpCode = webException.getResponse().getStatus();
            logMsg   = webException.getResponse().getEntity().toString();
        } catch (Exception e) {
            httpCode = HttpServletResponse.SC_BAD_REQUEST;
            logMsg   = e.getMessage();

            LOG.error("GdsREST.getServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities, e);
        }

        assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_GDS, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode, clusterName, pluginCapabilities);

        if (httpCode != HttpServletResponse.SC_OK) {
            boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;

            throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
        }

        LOG.debug("<== GdsREST.getServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{}): ret={}", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities, ret);

        return ret;
    }

    @GET
    @Path("/secure/download/{serviceName}")
    @Produces("application/json")
    public ServiceGdsInfo getSecureServiceGdsInfoIfUpdated(@PathParam("serviceName") String serviceName, @QueryParam("lastKnownGdsVersion") @DefaultValue("-1") Long lastKnownVersion, @QueryParam("lastActivationTime") @DefaultValue("0") Long lastActivationTime, @QueryParam("pluginId") String pluginId, @QueryParam("clusterName") @DefaultValue("") String clusterName, @QueryParam("pluginCapabilities") @DefaultValue("") String pluginCapabilities, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getSecureServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities);

        ServiceGdsInfo ret               = null;
        int            httpCode          = HttpServletResponse.SC_OK;
        Long           downloadedVersion = null;
        String         logMsg            = null;

        try {
            bizUtil.failUnauthenticatedDownloadIfNotAllowed();

            boolean isValid = serviceUtil.isValidateHttpsAuthentication(serviceName, request);

            if (isValid) {
                ret = gdsStore.getGdsInfoIfUpdated(serviceName, lastKnownVersion);

                if (ret == null) {
                    downloadedVersion = lastKnownVersion;
                    httpCode          = HttpServletResponse.SC_NOT_MODIFIED;
                    logMsg            = "No change since last update";
                } else {
                    downloadedVersion = ret.getGdsVersion();
                }
            }
        } catch (WebApplicationException webException) {
            httpCode = webException.getResponse().getStatus();
            logMsg   = webException.getResponse().getEntity().toString();
        } catch (Exception e) {
            httpCode = HttpServletResponse.SC_BAD_REQUEST;
            logMsg   = e.getMessage();

            LOG.error("GdsREST.getServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities, e);
        }

        assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_GDS, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode, clusterName, pluginCapabilities);

        if (httpCode != HttpServletResponse.SC_OK) {
            boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;

            throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
        }

        LOG.debug("<== GdsREST.getSecureServiceGdsInfoIfUpdated(serviceName={}, lastKnownVersion={}, lastActivationTime={}, pluginId={}, clusterName={}, pluginCapabilities{}): ret={}", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, pluginCapabilities, ret);

        return ret;
    }

    @GET
    @Path("/dataset/{id}/grants")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_GRANTS + "\")")
    public List<RangerGrant> getDataSetGrants(@PathParam("id") Long id, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDataSetGrants(dataSetId: {})", id);

        RangerPerfTracer  perf = null;
        List<RangerGrant> ret  = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataSetGrants( DataSetId: " + id + ")");
            }

            List<RangerPolicy> policies = gdsStore.getDatasetPolicies(id);

            if (CollectionUtils.isNotEmpty(policies)) {
                List<RangerPolicyItem> filteredPolicyItems = filterPolicyItemsByRequest(policies.get(0), request);

                if (CollectionUtils.isNotEmpty(filteredPolicyItems)) {
                    ret = transformPolicyItemsToGrants(filteredPolicyItems);
                } else {
                    LOG.debug("getDataSetGrants(): no grants available in dataset(id={}), policy(id={}) for query {}", id, policies.get(0).getId(), request.getQueryString());
                }
            } else {
                LOG.debug("getDataSetGrants(): no policy found for dataset(id={})", id);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDataSetGrants (dataSetId: {}) failed!..error: {}", id, excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDataSetGrants(dataSetId: {}): ret= {}", id, ret);

        return ret != null ? ret : Collections.emptyList();
    }

    @PUT
    @Path("/dataset/{id}/grant")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASET_GRANTS + "\")")
    public RangerPolicyHeader updateDataSetGrants(@PathParam("id") Long id, List<RangerGrant> rangerGrants) {
        LOG.debug("==> GdsREST.updateDataSetGrants(dataSetId: {}, rangerGrants: {})", id, rangerGrants);

        RangerPerfTracer   perf = null;
        RangerPolicyHeader ret  = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataSetGrants( DataSetId: " + id + "rangerGrants: " + rangerGrants + ")");
            }

            List<RangerPolicy> policies                 = gdsStore.getDatasetPolicies(id);
            RangerPolicy       policy                   = CollectionUtils.isNotEmpty(policies) ? policies.get(0) : gdsStore.addDatasetPolicy(id, new RangerPolicy());
            RangerPolicy       policyWithModifiedGrants = updatePolicyWithModifiedGrants(policy, rangerGrants);

            if (policyWithModifiedGrants != null) {
                RangerPolicy updatedPolicy = gdsStore.updateDatasetPolicy(id, policyWithModifiedGrants);

                ret = rangerPolicyHeaderOf(updatedPolicy);
            } else {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_MODIFIED, "No action performed: The grant may already exist or may not be found for deletion.", false);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateDataSetGrants (dataSetId: {}, rangerGrants: {}) failed!..error: {}", id, rangerGrants, excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.updateDataSetGrants(dataSetId: {}, rangerGrants: {}): ret= {}", id, rangerGrants, ret);

        return ret;
    }

    @VisibleForTesting
    List<RangerPolicyItem> filterPolicyItemsByRequest(RangerPolicy rangerPolicy, HttpServletRequest request) {
        LOG.debug("==> GdsREST.filterPolicyItemsByRequest(rangerPolicy: {})", rangerPolicy);

        if (rangerPolicy == null || CollectionUtils.isEmpty(rangerPolicy.getPolicyItems())) {
            return Collections.emptyList();
        }

        List<RangerPolicyItem> policyItems          = rangerPolicy.getPolicyItems();
        String[]               filteringPrincipals  = searchUtil.getParamMultiValues(request, "principal");
        String[]               filteringAccessTypes = searchUtil.getParamMultiValues(request, "accessType");

        Predicate<RangerPolicyItem> byPrincipalPredicate  = filterByPrincipalsPredicate(filteringPrincipals);
        Predicate<RangerPolicyItem> byAccessTypePredicate = filterByAccessTypesPredicate(filteringAccessTypes);
        List<RangerPolicyItem>      filteredPolicyItems   = policyItems.stream().filter(byPrincipalPredicate.and(byAccessTypePredicate)).collect(Collectors.toList());

        LOG.debug("<== GdsREST.filterPolicyItemsByRequest(rangerPolicy: {}): filteredPolicyItems= {}", rangerPolicy, filteredPolicyItems);

        return filteredPolicyItems;
    }

    @VisibleForTesting
    List<RangerGrant> transformPolicyItemsToGrants(List<RangerPolicyItem> policyItems) {
        LOG.debug("==> GdsREST.transformPolicyItemsToGrants(policyItems: {})", policyItems);
        if (CollectionUtils.isEmpty(policyItems)) {
            return null;
        }

        List<RangerGrant>   ret         = new ArrayList<>();

        for (RangerPolicyItem policyItem : policyItems) {
            List<String> policyItemUsers  = policyItem.getUsers();
            List<String> policyItemGroups = policyItem.getGroups();
            List<String> policyItemRoles  = policyItem.getRoles();

            List<RangerPolicyItemAccess>    policyItemAccesses    = policyItem.getAccesses();
            List<RangerPolicyItemCondition> policyItemConditions  = policyItem.getConditions();
            List<String>                    policyItemAccessTypes = policyItemAccesses.stream().map(RangerPolicyItemAccess::getType).collect(Collectors.toList());

            List<RangerGrant.Condition> conditions = getGrantConditions(policyItemConditions);

            if (CollectionUtils.isNotEmpty(policyItemUsers)) {
                policyItemUsers.forEach(x -> ret.add(new RangerGrant(new RangerPrincipal(PrincipalType.USER, x), policyItemAccessTypes, conditions)));
            }

            if (CollectionUtils.isNotEmpty(policyItemGroups)) {
                policyItemGroups.forEach(x -> ret.add(new RangerGrant(new RangerPrincipal(PrincipalType.GROUP, x), policyItemAccessTypes, conditions)));
            }

            if (CollectionUtils.isNotEmpty(policyItemRoles)) {
                policyItemRoles.forEach(x -> ret.add(new RangerGrant(new RangerPrincipal(PrincipalType.ROLE, x), policyItemAccessTypes, conditions)));
            }
        }

        LOG.debug("<== GdsREST.transformPolicyItemsToGrants(policyItems: {}): ret= {}", policyItems, ret);

        return ret;
    }

    @VisibleForTesting
    RangerPolicy updatePolicyWithModifiedGrants(RangerPolicy policy, List<RangerGrant> rangerGrants) {
        LOG.debug("==> GdsREST.updatePolicyWithModifiedGrants(policy: {}, rangerGrants: {})", policy, rangerGrants);
        try {
            List<RangerPolicyItem> policyItems         = policy.getPolicyItems();
            List<RangerPolicyItem> policyItemsToUpdate = policyItems.stream().map(this::copyOf).collect(Collectors.toList());
            Set<RangerPrincipal>   principalsToUpdate  = rangerGrants.stream().map(RangerGrant::getPrincipal).collect(Collectors.toSet());

            for (RangerPrincipal principal : principalsToUpdate) {
                List<RangerPolicyItem> policyItemsToRemove = new ArrayList<>();

                policyItemsToUpdate.stream().filter(matchesPrincipalPredicate(principal)).forEach(policyItem -> {
                    removeMatchingPrincipalFromPolicyItem(policyItem, principal);

                    if (isPolicyItemEmpty(policyItem)) {
                        policyItemsToRemove.add(policyItem);
                    }
                });

                policyItemsToUpdate.removeAll(policyItemsToRemove);
            }

            for (RangerGrant grant : rangerGrants) {
                if (hasAccessTypes(grant)) {
                    policyItemsToUpdate.add(transformGrantToPolicyItem(grant));
                }
            }

            if (CollectionUtils.isEqualCollection(policyItems, policyItemsToUpdate)) {
                // Skip DataSet update if no policy changes detected, avoiding unnecessary updates.
                policy = null;
            } else {
                policy.setPolicyItems(policyItemsToUpdate);
            }
        } catch (Exception e) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, e.getMessage(), true);
        }

        LOG.debug("<== GdsREST.updatePolicyWithModifiedGrants(updatedPolicy: {})", policy);

        return policy;
    }

    private List<RangerGrant.Condition> getGrantConditions(List<RangerPolicy.RangerPolicyItemCondition> policyItemConditions) {
        List<RangerGrant.Condition> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(policyItemConditions)) {
            policyItemConditions.stream().map(condition -> new RangerGrant.Condition(condition.getType(), condition.getValues())).forEach(ret::add);
        }

        return ret;
    }

    private Long getOrCreateDataShare(Long datasetId, Long serviceId, Long zoneId, String serviceName) throws Exception {
        LOG.debug("==> GdsREST.getOrCreateDataShare(dataSetId={} serviceId={} zoneId={} serviceName={})", datasetId, serviceId, zoneId, serviceName);

        Long            ret;
        RangerDataShare rangerDataShare;
        RangerDataset   rangerDataset = gdsStore.getDataset(datasetId);
        String          dataShareName = "__dataset_" + datasetId + "__service_" + serviceId + "__zone_" + zoneId;

        SearchFilter filter = new SearchFilter();

        filter.setParam(SearchFilter.DATA_SHARE_NAME, dataShareName);

        PList<RangerDataShare> dataSharePList = gdsStore.searchDataShares(filter);
        List<RangerDataShare>  dataShareList  = dataSharePList.getList();

        if (CollectionUtils.isNotEmpty(dataShareList)) {
            List<RangerDataShare> rangerDataShares = dataSharePList.getList();

            rangerDataShare = rangerDataShares.get(0);
            ret             = rangerDataShare.getId();
        } else {
            //Create a DataShare
            RangerDataShare dataShare = new RangerDataShare();

            dataShare.setName(dataShareName);
            dataShare.setDescription(dataShareName);
            dataShare.setTermsOfUse(rangerDataset.getTermsOfUse());
            dataShare.setService(serviceName);
            dataShare.setDefaultAccessTypes(new HashSet<>());

            rangerDataShare = gdsStore.createDataShare(dataShare);

            //Add DataShare to DataSet
            List<RangerDataShareInDataset> rangerDataShareInDatasets = new ArrayList<>();
            RangerDataShareInDataset       rangerDataShareInDataset  = new RangerDataShareInDataset();

            rangerDataShareInDataset.setDataShareId(rangerDataShare.getId());
            rangerDataShareInDataset.setDatasetId(rangerDataset.getId());
            rangerDataShareInDataset.setStatus(RangerGds.GdsShareStatus.ACTIVE);
            rangerDataShareInDatasets.add(rangerDataShareInDataset);

            addDataSharesInDataset(rangerDataset.getId(), rangerDataShareInDatasets);

            ret = rangerDataShare.getId();
        }

        LOG.debug("<== GdsREST.getOrCreateDataShare(RangerDataShare={})", ret);

        return ret;
    }

    private Long validateAndGetServiceId(String serviceName) {
        Long ret;

        if (serviceName == null || serviceName.isEmpty()) {
            LOG.error("ServiceName not provided");

            throw restErrorUtil.createRESTException("ServiceName not provided.", MessageEnums.INVALID_INPUT_DATA);
        }

        RangerService service;

        try {
            service = serviceDBStore.getServiceByName(serviceName);
            ret     = service.getId();
        } catch (Exception e) {
            LOG.error("Requested Service not found. serviceName={}", serviceName);

            throw restErrorUtil.createRESTException("Service:" + serviceName + " not found", MessageEnums.DATA_NOT_FOUND);
        }

        if (service == null) {
            LOG.error("Requested Service not found. serviceName={}", serviceName);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, RangerServiceNotFoundException.buildExceptionMsg(serviceName), false);
        }

        if (!service.getIsEnabled()) {
            LOG.error("Requested Service is disabled. serviceName={}", serviceName);

            throw restErrorUtil.createRESTException("Unauthorized access.", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
        }

        return ret;
    }

    private Long validateAndGetZoneId(String zoneName) {
        Long ret = RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID;

        if (zoneName == null || zoneName.isEmpty()) {
            return ret;
        }

        RangerSecurityZone rangerSecurityZone;

        try {
            rangerSecurityZone = serviceDBStore.getSecurityZone(zoneName);
            ret                = rangerSecurityZone.getId();
        } catch (Exception e) {
            LOG.error("Requested Zone not found. ZoneName={}", zoneName);

            throw restErrorUtil.createRESTException("Zone:" + zoneName + " not found", MessageEnums.DATA_NOT_FOUND);
        }

        if (rangerSecurityZone == null) {
            LOG.error("Requested Zone not found. ZoneName={}", zoneName);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, RangerServiceNotFoundException.buildExceptionMsg(zoneName), false);
        }

        return ret;
    }

    private RangerPolicyHeader rangerPolicyHeaderOf(RangerPolicy rangerPolicy) {
        LOG.debug("==> GdsREST.rangerPolicyHeaderOf(rangerPolicy: {})", rangerPolicy);

        RangerPolicyHeader ret = null;

        if (rangerPolicy != null) {
            ret = new RangerPolicyHeader(rangerPolicy);
        }

        LOG.debug("<== GdsREST.rangerPolicyHeaderOf(rangerPolicy: {}): ret= {}", rangerPolicy, ret);

        return ret;
    }

    private boolean isPolicyItemEmpty(RangerPolicyItem policyItem) {
        return CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getRoles());
    }

    private void removeMatchingPrincipalFromPolicyItem(RangerPolicyItem policyItem, RangerPrincipal principal) {
        String        principalName = principal.getName();
        PrincipalType principalType = principal.getType();

        if (principalType == PrincipalType.USER && policyItem.getUsers() != null) {
            policyItem.getUsers().remove(principalName);
        } else if (principalType == PrincipalType.GROUP && policyItem.getGroups() != null) {
            policyItem.getGroups().remove(principalName);
        } else if (principalType == PrincipalType.ROLE && policyItem.getRoles() != null) {
            policyItem.getRoles().remove(principalName);
        }
    }

    private RangerPolicyItem transformGrantToPolicyItem(RangerGrant grant) {
        LOG.debug("==> GdsREST.transformGrantToPolicyItem(grant: {})", grant);

        if (grant == null) {
            return null;
        }

        RangerPolicyItem            policyItem  = new RangerPolicyItem();
        List<String>                permissions = grant.getAccessTypes();
        List<RangerGrant.Condition> conditions  = grant.getConditions();

        if (CollectionUtils.isNotEmpty(permissions)) {
            policyItem.setAccesses(permissions.stream()
                    .map(accessType -> new RangerPolicyItemAccess(accessType, true))
                    .collect(Collectors.toList()));
        }

        List<RangerPolicyItemCondition> policyItemConditions = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(conditions)) {
            conditions.stream().map(condition -> new RangerPolicyItemCondition(condition.getType(), condition.getValues())).forEach(policyItemConditions::add);
        }

        policyItem.setConditions(policyItemConditions);

        switch (grant.getPrincipal().getType()) {
            case USER:
                policyItem.setUsers(Collections.singletonList(grant.getPrincipal().getName()));
                break;
            case GROUP:
                policyItem.setGroups(Collections.singletonList(grant.getPrincipal().getName()));
                break;
            case ROLE:
                policyItem.setRoles(Collections.singletonList(grant.getPrincipal().getName()));
                break;
        }

        LOG.debug("<== GdsREST.transformGrantToPolicyItem(grant: {}): policyItem= {}", grant, policyItem);

        return policyItem;
    }

    private Predicate<RangerPolicyItem> matchesPrincipalPredicate(RangerPrincipal principal) {
        String        principalName = principal.getName();
        PrincipalType principalType = principal.getType();

        return policyItem -> {
            switch (principalType) {
                case USER:
                    return policyItem.getUsers().contains(principalName);
                case GROUP:
                    return policyItem.getGroups().contains(principalName);
                case ROLE:
                    return policyItem.getRoles().contains(principalName);
            }
            return false;
        };
    }

    private boolean hasAccessTypes(RangerGrant grant) {
        return grant.getAccessTypes() != null && !grant.getAccessTypes().isEmpty();
    }

    private Predicate<RangerPolicyItem> filterByPrincipalsPredicate(String[] filteringPrincipals) {
        if (ArrayUtils.isEmpty(filteringPrincipals)) {
            return policyItem -> true; // No filtering by principal if no principals specified
        }

        Map<String, Set<String>> principalCriteriaMap = new HashMap<>();

        for (String principal : filteringPrincipals) {
            String[] parts         = principal.split(":");
            String   principalType = parts.length > 1 ? parts[0] : DEFAULT_PRINCIPAL_TYPE;
            String   principalName = parts.length > 1 ? parts[1] : parts[0];

            principalCriteriaMap.computeIfAbsent(principalType.toLowerCase(), k -> new HashSet<>()).add(principalName);
        }

        return policyItem -> {
            Set<String> users  = principalCriteriaMap.getOrDefault(PRINCIPAL_TYPE_USER, Collections.emptySet());
            Set<String> groups = principalCriteriaMap.getOrDefault(PRINCIPAL_TYPE_GROUP, Collections.emptySet());
            Set<String> roles  = principalCriteriaMap.getOrDefault(PRINCIPAL_TYPE_ROLE, Collections.emptySet());

            return (policyItem.getUsers() != null && policyItem.getUsers().stream().anyMatch(users::contains)) || (policyItem.getGroups() != null && policyItem.getGroups().stream().anyMatch(groups::contains)) || (policyItem.getRoles() != null && policyItem.getRoles().stream().anyMatch(roles::contains));
        };
    }

    private Predicate<RangerPolicyItem> filterByAccessTypesPredicate(String[] filteringAccessTypes) {
        if (ArrayUtils.isEmpty(filteringAccessTypes)) {
            return policyItem -> true; // No filtering by access type if no access types specified
        }

        Set<String> accessTypeSet = new HashSet<>(Arrays.asList(filteringAccessTypes));

        return policyItem -> policyItem.getAccesses().stream().anyMatch(access -> accessTypeSet.contains(access.getType()));
    }

    private RangerPolicyItem copyOf(RangerPolicyItem policyItem) {
        RangerPolicyItem copy = new RangerPolicyItem();

        copy.setAccesses(new ArrayList<>(policyItem.getAccesses()));
        copy.setUsers(new ArrayList<>(policyItem.getUsers()));
        copy.setGroups(new ArrayList<>(policyItem.getGroups()));
        copy.setRoles(new ArrayList<>(policyItem.getRoles()));
        copy.setConditions(new ArrayList<>(policyItem.getConditions()));
        copy.setDelegateAdmin(policyItem.getDelegateAdmin());

        return copy;
    }

    private void extractDatasetMultiValueParams(HttpServletRequest request, SearchFilter filter) {
        searchUtil.extractStringList(request, filter, SearchFilter.DATASET_LABEL, "Dataset Label List", "datasetLabels", null, null);
        searchUtil.extractStringList(request, filter, SearchFilter.DATASET_KEYWORD, "Dataset Keyword List", "datasetKeywords", null, null);
    }
}
