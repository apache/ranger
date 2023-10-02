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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.GdsDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerGdsDatasetService;
import org.apache.ranger.plugin.model.RangerDatasetHeader.RangerDatasetHeaderInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;

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
    RangerSearchUtil searchUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;


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
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_DATASET + "\")")
    public void deleteDataset(@PathParam("id") Long datasetId) {
        LOG.debug("==> deleteDataset({})", datasetId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteDataset(datasetId=" + datasetId + ")");
            }

            gdsStore.deleteDataset(datasetId);
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
                throw new Exception("no dataset with id=" + datasetId);
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
        RangerPerfTracer perf   = null;
        SearchFilter     filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasets()");
            }

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
    @Path("/dataset/info")
    @Produces({ "application/json" })
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DATASETS_HEADER + "\")")
    public PList<RangerDatasetHeaderInfo> getDatasetHeaderInfo(@Context HttpServletRequest request) {
        LOG.debug("==> GdsREST.getDatasetHeaderInfo()");

        PList<RangerDatasetHeaderInfo> ret;

        try {
            SearchFilter filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

            ret = gdsStore.getDatasetHeaders(filter);
        } catch (WebApplicationException we) {
            LOG.error("getDatasets() failed", we);

            throw restErrorUtil.createRESTException(we.getMessage());
        } catch (Throwable ex) {
            LOG.error("getDatasets() failed", ex);

            throw restErrorUtil.createRESTException(ex.getMessage());
        }

        LOG.debug("<== GdsREST.getDatasetHeaderInfo(): {}", ret);

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
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_PROJECT + "\")")
    public void deleteProject(@PathParam("id") Long projectId) {
        LOG.debug("==> deleteProject({})", projectId);

        RangerPerfTracer perf = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.deleteProject(projectId=" + projectId + ")");
            }

            gdsStore.deleteProject(projectId);
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
                throw new Exception("no project with id=" + projectId);
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
        RangerPerfTracer     perf   = null;
        SearchFilter         filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
        RangerPerfTracer perf   = null;
        SearchFilter     filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchProjects()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
                throw new Exception("no resourceset with id=" + dataShareId);
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
        RangerPerfTracer       perf   = null;
        SearchFilter           filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShares()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
                throw new Exception("no shared-resource with id=" + resourceId);
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
        RangerPerfTracer       perf   = null;
        SearchFilter           filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchSharedResources()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
        RangerPerfTracer  perf = null;

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
        RangerPerfTracer  perf = null;

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
    @Consumes({ "application/json" })
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
        RangerPerfTracer         perf   = null;
        SearchFilter             filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDataShareInDatasets()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
    @Consumes({ "application/json" })
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
        RangerPerfTracer              perf   = null;
        SearchFilter                  filter = null;

        try {
            if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "GdsREST.searchDatasetInProjects()");
            }

            filter = searchUtil.getSearchFilter(request, datasetService.sortFields);

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
}
