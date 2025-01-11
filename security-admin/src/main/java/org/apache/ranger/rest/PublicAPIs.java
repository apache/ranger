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

import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.annotation.RangerAnnotationClassName;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPolicy;
import org.apache.ranger.view.VXPolicyList;
import org.apache.ranger.view.VXRepository;
import org.apache.ranger.view.VXRepositoryList;
import org.apache.ranger.view.VXResource;
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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import java.util.List;

@Path("public")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("PublicMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PublicAPIs {
    private static final Logger logger = LoggerFactory.getLogger(PublicAPIs.class);

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    XAssetService xAssetService;

    @Autowired
    RangerPolicyService policyService;

    @Autowired
    StringUtil stringUtil;

    @Autowired
    ServiceUtil serviceUtil;

    @Autowired
    ServiceREST serviceREST;

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    AssetREST assetREST;

    @GET
    @Path("/api/repository/{id}")
    @Produces("application/json")
    public VXRepository getRepository(@PathParam("id") Long id) {
        logger.debug("==> PublicAPIs.getRepository({})", id);

        RangerService service = serviceREST.getService(id);
        VXRepository  ret     = serviceUtil.toVXRepository(service);

        logger.debug("<= PublicAPIs.getRepository({})", id);

        return ret;
    }

    @POST
    @Path("/api/repository/")
    @Consumes("application/json")
    @Produces("application/json")
    public VXRepository createRepository(VXRepository vXRepository) {
        logger.debug("==> PublicAPIs.createRepository({})", vXRepository);

        VXAsset       vXAsset        = serviceUtil.publicObjecttoVXAsset(vXRepository);
        RangerService service        = serviceUtil.toRangerService(vXAsset);
        RangerService createdService = serviceREST.createService(service);
        VXAsset       retvXAsset     = serviceUtil.toVXAsset(createdService);
        VXRepository  ret            = serviceUtil.vXAssetToPublicObject(retvXAsset);

        logger.debug("<== PublicAPIs.createRepository({})", ret);

        return ret;
    }

    @PUT
    @Path("/api/repository/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    public VXRepository updateRepository(VXRepository vXRepository, @PathParam("id") Long id) {
        logger.debug("==> PublicAPIs.updateRepository({})", id);

        XXService existing = daoMgr.getXXService().getById(id);

        if (existing == null) {
            throw restErrorUtil.createRESTException("Repository not found for Id: " + id, MessageEnums.DATA_NOT_FOUND);
        }

        vXRepository.setId(id);

        VXAsset       vXAsset = serviceUtil.publicObjecttoVXAsset(vXRepository);
        RangerService service = serviceUtil.toRangerService(vXAsset);

        service.setVersion(existing.getVersion());

        RangerService updatedService = serviceREST.updateService(service, null);
        VXAsset       retvXAsset     = serviceUtil.toVXAsset(updatedService);
        VXRepository  ret            = serviceUtil.vXAssetToPublicObject(retvXAsset);

        logger.debug("<== PublicAPIs.updateRepository({})", ret);

        return ret;
    }

    @DELETE
    @Path("/api/repository/{id}")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    @RangerAnnotationClassName(class_name = VXAsset.class)
    public void deleteRepository(@PathParam("id") Long id, @Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.deleteRepository({})", id);

        serviceREST.deleteService(id);

        logger.debug("<== PublicAPIs.deleteRepository({})", id);
    }

    @GET
    @Path("/api/repository/")
    @Produces("application/json")
    public VXRepositoryList searchRepositories(@Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.searchRepositories()");

        SearchFilter        filter      = searchUtil.getSearchFilterFromLegacyRequestForRepositorySearch(request, xAssetService.sortFields);
        List<RangerService> serviceList = serviceREST.getServices(filter);
        VXRepositoryList    ret         = null;

        if (serviceList != null) {
            ret = serviceUtil.rangerServiceListToPublicObjectList(serviceList);
        }

        logger.debug("<== PublicAPIs.searchRepositories(): count={}", (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    @GET
    @Path("/api/repository/count")
    @Produces("application/json")
    public VXLong countRepositories(@Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.countRepositories()");

        VXLong ret = assetREST.countXAssets(request);

        logger.debug("<== PublicAPIs.countRepositories(): count={}", ret);

        return ret;
    }

    @GET
    @Path("/api/policy/{id}")
    @Produces("application/json")
    public VXPolicy getPolicy(@PathParam("id") Long id) {
        logger.debug("==> PublicAPIs.getPolicy() {}", id);

        RangerService service = null;
        RangerPolicy  policy  = serviceREST.getPolicy(id);

        if (policy != null) {
            service = serviceREST.getServiceByName(policy.getService());
        }

        VXPolicy ret = serviceUtil.toVXPolicy(policy, service);

        logger.debug("<== PublicAPIs.getPolicy(){}", ret);

        return ret;
    }

    @POST
    @Path("/api/policy")
    @Consumes("application/json")
    @Produces("application/json")
    public VXPolicy createPolicy(VXPolicy vXPolicy) {
        logger.debug("==> PublicAPIs.createPolicy()");

        if (vXPolicy == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Policy object is null in create policy api", false);
        }

        RangerService service = serviceREST.getServiceByName(vXPolicy.getRepositoryName());
        RangerPolicy  policy  = serviceUtil.toRangerPolicy(vXPolicy, service);

        VXPolicy ret = null;
        if (policy != null) {
            logger.debug("RangerPolicy: {}", policy);

            RangerPolicy createdPolicy = serviceREST.createPolicy(policy, null);

            ret = serviceUtil.toVXPolicy(createdPolicy, service);
        }

        logger.debug("<== PublicAPIs.createPolicy({}): {}", policy, ret);

        return ret;
    }

    @PUT
    @Path("/api/policy/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    public VXPolicy updatePolicy(VXPolicy vXPolicy, @PathParam("id") Long id) {
        logger.debug("==> PublicAPIs.updatePolicy(): {}", vXPolicy);

        if (vXPolicy == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Policy object is null in update policy api", false);
        }

        XXPolicy existing = daoMgr.getXXPolicy().getById(id);

        if (existing == null) {
            throw restErrorUtil.createRESTException("Policy not found for Id: " + id, MessageEnums.DATA_NOT_FOUND);
        }

        vXPolicy.setId(id);

        RangerService service = serviceREST.getServiceByName(vXPolicy.getRepositoryName());
        RangerPolicy  policy  = serviceUtil.toRangerPolicy(vXPolicy, service);

        VXPolicy ret = null;
        if (policy != null) {
            policy.setVersion(existing.getVersion());

            RangerPolicy updatedPolicy = serviceREST.updatePolicy(policy, policy.getId());

            ret = serviceUtil.toVXPolicy(updatedPolicy, service);
        }

        logger.debug("<== PublicAPIs.updatePolicy({}):{}", policy, ret);

        return ret;
    }

    @DELETE
    @Path("/api/policy/{id}")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    @RangerAnnotationClassName(class_name = VXResource.class)
    public void deletePolicy(@PathParam("id") Long id, @Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.deletePolicy(): {}", id);

        serviceREST.deletePolicy(id);

        logger.debug("<== PublicAPIs.deletePolicy(): {}", id);
    }

    @GET
    @Path("/api/policy")
    @Produces("application/json")
    public VXPolicyList searchPolicies(@Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.searchPolicies(): ");

        SearchFilter filter = searchUtil.getSearchFilterFromLegacyRequest(request, policyService.sortFields);

        // get all policies from the store; pick the page to return after applying filter
        int savedStartIndex = filter.getStartIndex();
        int savedMaxRows    = filter.getMaxRows();

        filter.setStartIndex(0);
        filter.setMaxRows(Integer.MAX_VALUE);

        List<RangerPolicy> rangerPolicyList = serviceREST.getPolicies(filter);

        filter.setStartIndex(savedStartIndex);
        filter.setMaxRows(savedMaxRows);

        VXPolicyList vXPolicyList = null;

        if (rangerPolicyList != null) {
            vXPolicyList = serviceUtil.rangerPolicyListToPublic(rangerPolicyList, filter);
        }

        logger.debug("<== PublicAPIs.searchPolicies(): {}", vXPolicyList);
        return vXPolicyList;
    }

    @GET
    @Path("/api/policy/count")
    @Produces("application/json")
    public VXLong countPolicies(@Context HttpServletRequest request) {
        logger.debug("==> PublicAPIs.countPolicies(): ");

        VXLong ret = assetREST.countXResources(request);

        logger.debug("<== PublicAPIs.countPolicies(): {}", ret);

        return ret;
    }
}
