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
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.GdsDBStore;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerGds.DatasetSummary;
import org.apache.ranger.plugin.model.RangerGds.DataShareSummary;
import org.apache.ranger.plugin.model.RangerGds.DataShareInDatasetSummary;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerGdsDatasetInProjectService;
import org.apache.ranger.service.RangerGdsDataShareInDatasetService;
import org.apache.ranger.service.RangerGdsDataShareService;
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
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.List;

@Path("gds")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class GdsREST {
    private static final Logger LOG      = LoggerFactory.getLogger(GdsREST.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("rest.GdsREST");

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
    AssetMgr assetMgr;


    @POST
    @Path("/dataset")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_DATASET + "\")")
    public RangerDataset createDataset(RangerDataset dataset) {
        LOG.debug("==> GdsREST.createDataset({})", dataset);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createDataset(datasetName=" + dataset.getName() + ")");
            }

            ret = gdsStore.createDataset(dataset);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("createDataset({}) failed", dataset, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.createDataset({}): {}", dataset, ret);

        return ret;
    }

    @POST
    @Path("/dataset/{id}/datashare")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASHARE_IN_DATASET + "\")")
    public List<RangerDataShareInDataset> addDataSharesInDataset(@PathParam("id") Long datasetId, List<RangerDataShareInDataset> dataSharesInDataset) {
        LOG.debug("==> GdsREST.addDataSharesInDataset({}, {})", datasetId, dataSharesInDataset);

        List<RangerDataShareInDataset> ret;
        RangerPerfTracer               perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDataSharesInDataset(" +  datasetId + ")");
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
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASET + "\")")
    public RangerDataset updateDataset(@PathParam("id") Long datasetId, RangerDataset dataset) {
        LOG.debug("==> GdsREST.updateDataset({}, {})", datasetId, dataset);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataset(datasetId=" + datasetId + ", datasetName=" + dataset.getName() + ")");
            }

            dataset.setId(datasetId);

            ret = gdsStore.updateDataset(dataset);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_DATASET + "\")")
    public void deleteDataset(@PathParam("id") Long datasetId, @Context HttpServletRequest request) {
        LOG.debug("==> deleteDataset({})", datasetId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDataset(datasetId=" + datasetId + ")");
            }

            boolean forceDelete = Boolean.parseBoolean(request.getParameter("forceDelete"));

            gdsStore.deleteDataset(datasetId, forceDelete);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteDataset({}) failed", datasetId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== deleteDataset({})", datasetId);
    }

    @GET
    @Path("/dataset/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET + "\")")
    public RangerDataset getDataset(@PathParam("id") Long datasetId) {
        LOG.debug("==> GdsREST.getDataset({})", datasetId);

        RangerDataset    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataset(datasetId=" + datasetId + ")");
            }

            ret = gdsStore.getDataset(datasetId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no dataset with id=" + datasetId, false);
            }
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASETS + "\")")
    public PList<RangerDataset> searchDatasets(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDatasets()");

        PList<RangerDataset> ret;
        RangerPerfTracer     perf   = null;
        SearchFilter         filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasets()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            ret = gdsStore.searchDatasets(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LIST_DATASET_NAMES + "\")")
    public PList<String> listDatasetNames(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.listDatasetNames()");

        PList<String>    ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.listDatasetNames()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            ret = gdsStore.getDatasetNames(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_SUMMARY + "\")")
    public PList<DatasetSummary> getDatasetSummary(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDatasetSummary()");

        PList<DatasetSummary> ret;
        RangerPerfTracer      perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetSummary()");
        SearchFilter          filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            ret = gdsStore.getDatasetSummary(filter);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Throwable ex) {
            LOG.error("getDatasetSummary({}) failed", filter, ex);

            throw restErrorUtil.createRESTException(ex.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.getDatasetSummary(): {}", ret);

        return ret;
    }

    @POST
    @Path(("/dataset/{id}/policy"))
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
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
    @Produces({ "application/json" })
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
    @Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_PROJECT + "\")")
    public RangerProject createProject(RangerProject project) {
        LOG.debug("==> GdsREST.createProject({})", project);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createProject(projectName=" + project.getName() + ")");
            }

            ret = gdsStore.createProject(project);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_PROJECT + "\")")
    public RangerProject updateProject(@PathParam("id") Long projectId, RangerProject project) {
        LOG.debug("==> GdsREST.updateProject({}, {})", projectId, project);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateProject(projectId=" + projectId + ", projectName=" + project.getName() + ")");
            }

            project.setId(projectId);

            ret = gdsStore.updateProject(project);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_PROJECT + "\")")
    public void deleteProject(@PathParam("id") Long projectId, @Context HttpServletRequest request) {
        LOG.debug("==> deleteProject({})", projectId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteProject(projectId=" + projectId + ")");
            }

            boolean forceDelete = Boolean.parseBoolean(request.getParameter("forceDelete"));

            gdsStore.deleteProject(projectId, forceDelete);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteProject({}) failed", projectId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== deleteProject({})", projectId);
    }

    @GET
    @Path("/project/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_PROJECT + "\")")
    public RangerProject getProject(@PathParam("id") Long projectId) {
        LOG.debug("==> GdsREST.getProject({})", projectId);

        RangerProject    ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getProject(projectId=" + projectId + ")");
            }

            ret = gdsStore.getProject(projectId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no project with id=" + projectId, false);
            }
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_PROJECTS + "\")")
    public PList<RangerProject> searchProjects(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchProjects()");

        PList<RangerProject> ret;
        RangerPerfTracer     perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
        SearchFilter         filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, projectService.sortFields);

            ret = gdsStore.searchProjects(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LIST_PROJECT_NAMES + "\")")
    public PList<String> listProjectNames(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.listProjectNames()");

        PList<String>    ret;
        RangerPerfTracer perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
        SearchFilter     filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, projectService.sortFields);

            ret = gdsStore.getProjectNames(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
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
    @Produces({ "application/json" })
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
    @Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_DATA_SHARE + "\")")
    public RangerDataShare createDataShare(RangerDataShare dataShare) {
        LOG.debug("==> GdsREST.createDataShare({})", dataShare);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.createDataShare(" + dataShare + ")");
            }

            ret = gdsStore.createDataShare(dataShare);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATA_SHARE + "\")")
    public RangerDataShare updateDataShare(@PathParam("id") Long dataShareId, RangerDataShare dataShare) {
        LOG.debug("==> GdsREST.updateDataShare({}, {})", dataShareId, dataShare);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataShare(" + dataShare + ")");
            }

            dataShare.setId(dataShareId);

            ret = gdsStore.updateDataShare(dataShare);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_DATA_SHARE + "\")")
    public void deleteDataShare(@PathParam("id") Long dataShareId, @Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.deleteDataShare({})", dataShareId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDataShare(" + dataShareId + ")");
            }

            String forceDeleteStr = request.getParameter("forceDelete");
            boolean forceDelete = !StringUtils.isEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr);

            gdsStore.deleteDataShare(dataShareId, forceDelete);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteDataShare({}) failed", dataShareId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.deleteDataShare({})", dataShareId);
    }

    @GET
    @Path("/datashare/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATA_SHARE + "\")")
    public RangerDataShare getDataShare(@PathParam("id") Long dataShareId) {
        LOG.debug("==> GdsREST.getDataShare({})", dataShareId);

        RangerDataShare  ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataShare(" + dataShareId + ")");
            }

            ret = gdsStore.getDataShare(dataShareId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no dataShare with id=" + dataShareId, false);
            }
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATA_SHARES + "\")")
    public PList<RangerDataShare> searchDataShares(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDataShares()");

        PList<RangerDataShare> ret;
        RangerPerfTracer       perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShares()");
        SearchFilter           filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dataShareService.sortFields);

            ret = gdsStore.searchDataShares(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_SHARED_RESOURCE + "\")")
    public RangerSharedResource addSharedResource(RangerSharedResource resource) {
        LOG.debug("==> GdsREST.addSharedResource({})", resource);

        RangerSharedResource ret;
        RangerPerfTracer     perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addSharedResource(" + resource + ")");
            }

            ret = gdsStore.addSharedResource(resource);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("addSharedResource({}) failed", resource, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.addSharedResource({}): {}", resource, ret);

        return ret;
    }

    @PUT
    @Path("/resource/{id}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SHARED_RESOURCE + "\")")
    public RangerSharedResource updateSharedResource(@PathParam("id") Long resourceId, RangerSharedResource resource) {
        LOG.debug("==> GdsREST.updateSharedResource({}, {})", resourceId, resource);

        RangerSharedResource  ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateSharedResource(" + resource + ")");
            }

            resource.setId(resourceId);

            ret = gdsStore.updateSharedResource(resource);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.REMOVE_SHARED_RESOURCE + "\")")
    public void removeSharedResource(@PathParam("id") Long resourceId) {
        LOG.debug("==> GdsREST.removeSharedResource({})", resourceId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeSharedResource(" + resourceId + ")");
            }

            gdsStore.removeSharedResource(resourceId);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("removeSharedResource({}) failed", resourceId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeSharedResource({})", resourceId);
    }

    @GET
    @Path("/resource/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SHARED_RESOURCE + "\")")
    public RangerSharedResource getSharedResource(@PathParam("id") Long resourceId) {
        LOG.debug("==> GdsREST.getSharedResource({})", resourceId);

        RangerSharedResource  ret;
        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getSharedResource(" + resourceId + ")");
            }

            ret = gdsStore.getSharedResource(resourceId);

            if (ret == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "no shared-resource with id=" + resourceId, false);
            }
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_SHARED_RESOURCES + "\")")
    public PList<RangerSharedResource> searchSharedResources(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchSharedResources()");

        PList<RangerSharedResource> ret;
        RangerPerfTracer            perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchSharedResources()");
        SearchFilter                filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, sharedResourceService.sortFields);

            ret = gdsStore.searchSharedResources(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset addDataShareInDataset(RangerDataShareInDataset datasetData) {
        LOG.debug("==> GdsREST.addDataShareInDataset({})", datasetData);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDataShareInDataset(" +  datasetData + ")");
            }

            ret = gdsStore.addDataShareInDataset(datasetData);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset updateDataShareInDataset(@PathParam("id") Long id, RangerDataShareInDataset dataShareInDataset) {
        LOG.debug("==> GdsREST.updateDataShareInDataset({}, {})", id, dataShareInDataset);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDataShareInDataset(" +  dataShareInDataset + ")");
            }

            dataShareInDataset.setId(id);

            ret = gdsStore.updateDataShareInDataset(dataShareInDataset);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeDatasetData(" +  id + ")");
            }

            gdsStore.removeDataShareInDataset(id);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("removeDatasetData({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeDatasetData({})", id);
    }

    @GET
    @Path("/datashare/dataset/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASHARE_IN_DATASET + "\")")
    public RangerDataShareInDataset getDataShareInDataset(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.updateDataShareInDataset({})", id);

        RangerDataShareInDataset ret;
        RangerPerfTracer         perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDataShareInDataset(" +  id + ")");
            }

            ret = gdsStore.getDataShareInDataset(id);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASHARE_IN_DATASET + "\")")
    public PList<RangerDataShareInDataset> searchDataShareInDatasets(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDataShareInDatasets()");

        PList<RangerDataShareInDataset> ret;
        RangerPerfTracer         perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShareInDatasets()");
        SearchFilter             filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dshidService.sortFields);

            ret = gdsStore.searchDataShareInDatasets(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
	@Produces({ "application/json" })
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ADD_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject addDatasetInProject(RangerDatasetInProject projectData) {
        LOG.debug("==> GdsREST.addDatasetInProject({})", projectData);

        RangerDatasetInProject ret;
        RangerPerfTracer         perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.addDatasetInProject(" +  projectData + ")");
            }

            ret = gdsStore.addDatasetInProject(projectData);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject updateDatasetInProject(@PathParam("id") Long id, RangerDatasetInProject dataShareInProject) {
        LOG.debug("==> GdsREST.updateDatasetInProject({}, {})", id, dataShareInProject);

        RangerDatasetInProject ret;
        RangerPerfTracer  perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.updateDatasetInProject(" +  dataShareInProject + ")");
            }

            dataShareInProject.setId(id);

            ret = gdsStore.updateDatasetInProject(dataShareInProject);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.removeProjectData(" +  id + ")");
            }

            gdsStore.removeDatasetInProject(id);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("removeProjectData({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== GdsREST.removeProjectData({})", id);
    }

    @GET
    @Path("/dataset/project/{id}")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASET_IN_PROJECT + "\")")
    public RangerDatasetInProject getDatasetInProject(@PathParam("id") Long id) {
        LOG.debug("==> GdsREST.getDatasetInProject({})", id);

        RangerDatasetInProject ret;
        RangerPerfTracer       perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.getDatasetInProject(" +  id + ")");
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
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_DATASET_IN_PROJECT + "\")")
    public PList<RangerDatasetInProject> searchDatasetInProjects(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.searchDatasetInProjects()");

        PList<RangerDatasetInProject> ret;
        RangerPerfTracer              perf   = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasetInProjects()");
        SearchFilter                  filter = null;

        try {
            filter = searchUtil.getSearchFilter(request, dipService.sortFields);

            ret = gdsStore.searchDatasetInProjects(filter);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
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
    @Produces({ "application/json" })
    public ServiceGdsInfo getServiceGdsInfoIfUpdated(@PathParam("serviceName") String serviceName,
                                                     @QueryParam("lastKnownGdsVersion") @DefaultValue("-1") Long lastKnownVersion,
                                                     @QueryParam("lastActivationTime") @DefaultValue("0") Long lastActivationTime,
                                                     @QueryParam("pluginId") String pluginId,
                                                     @QueryParam("clusterName") @DefaultValue("") String clusterName,
                                                     @QueryParam("pluginCapabilities") @DefaultValue("") String pluginCapabilities,
                                                     @Context HttpServletRequest request) {
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
    @Produces({ "application/json" })
    public ServiceGdsInfo getSecureServiceGdsInfoIfUpdated(@PathParam("serviceName") String serviceName,
                                                           @QueryParam("lastKnownGdsVersion") @DefaultValue("-1") Long lastKnownVersion,
                                                           @QueryParam("lastActivationTime") @DefaultValue("0") Long lastActivationTime,
                                                           @QueryParam("pluginId") String pluginId,
                                                           @QueryParam("clusterName") @DefaultValue("") String clusterName,
                                                           @QueryParam("pluginCapabilities") @DefaultValue("") String pluginCapabilities,
                                                           @Context HttpServletRequest request) {
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
}
