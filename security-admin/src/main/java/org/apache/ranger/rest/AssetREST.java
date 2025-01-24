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
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.annotation.RangerAnnotationClassName;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerTrxLogV2Service;
import org.apache.ranger.service.XAccessAuditService;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.service.XCredentialStoreService;
import org.apache.ranger.service.XPolicyExportAuditService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.util.RestUtil;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAssetList;
import org.apache.ranger.view.VXCredentialStore;
import org.apache.ranger.view.VXCredentialStoreList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPolicy;
import org.apache.ranger.view.VXPolicyExportAuditList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResourceList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXTrxLogList;
import org.apache.ranger.view.VXUgsyncAuditInfoList;
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
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.util.RestUtil.convertToTimeZone;

@Path("assets")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("AssetMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class AssetREST {
    private static final Logger logger = LoggerFactory.getLogger(AssetREST.class);

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    AssetMgr assetMgr;

    @Autowired
    XAssetService xAssetService;

    @Autowired
    XResourceService xResourceService;

    @Autowired
    XPolicyService xPolicyService;

    @Autowired
    XCredentialStoreService xCredentialStoreService;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    XPolicyExportAuditService xPolicyExportAudits;

    @Autowired
    RangerTrxLogV2Service xTrxLogService;

    @Autowired
    RangerBizUtil msBizUtil;

    @Autowired
    XAccessAuditService xAccessAuditService;

    @Autowired
    ServiceUtil serviceUtil;

    @Autowired
    ServiceREST serviceREST;

    @Autowired
    RangerDaoManager daoManager;

    @GET
    @Path("/assets/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_X_ASSET + "\")")
    public VXAsset getXAsset(@PathParam("id") Long id) {
        logger.debug("==> AssetREST.getXAsset({})", id);

        RangerService service = serviceREST.getService(id);
        VXAsset       ret     = serviceUtil.toVXAsset(service);

        logger.debug("<== AssetREST.getXAsset({}): {}", id, ret);

        return ret;
    }

    @POST
    @Path("/assets")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_X_ASSET + "\")")
    public VXAsset createXAsset(VXAsset vXAsset) {
        logger.debug("==> AssetREST.createXAsset({})", vXAsset);

        RangerService service        = serviceUtil.toRangerService(vXAsset);
        RangerService createdService = serviceREST.createService(service);
        VXAsset       ret            = serviceUtil.toVXAsset(createdService);

        logger.debug("<== AssetREST.createXAsset({}):{}", vXAsset, ret);

        return ret;
    }

    @PUT
    @Path("/assets/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_X_ASSET + "\")")
    public VXAsset updateXAsset(VXAsset vXAsset) {
        logger.debug("==> AssetREST.updateXAsset({})", vXAsset);

        RangerService service        = serviceUtil.toRangerService(vXAsset);
        RangerService updatedService = serviceREST.updateService(service, null);
        VXAsset       ret            = serviceUtil.toVXAsset(updatedService);

        logger.debug("<== AssetREST.updateXAsset({}):{}", vXAsset, ret);

        return ret;
    }

    @DELETE
    @Path("/assets/{id}")
    @RangerAnnotationClassName(class_name = VXAsset.class)
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_X_ASSET + "\")")
    public void deleteXAsset(@PathParam("id") Long id, @Context HttpServletRequest request) {
        logger.debug("==> AssetREST.deleteXAsset({})", id);

        serviceREST.deleteService(id);

        logger.debug("<== AssetREST.deleteXAsset({})", id);
    }

    @POST
    @Path("/assets/testConfig")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.TEST_CONFIG + "\")")
    public VXResponse configTest(VXAsset vXAsset) {
        logger.debug("==> AssetREST.configTest({})", vXAsset);

        RangerService service = serviceUtil.toRangerService(vXAsset);
        VXResponse    ret     = serviceREST.validateConfig(service);

        logger.debug("<== AssetREST.testConfig({}):{}", vXAsset, ret);

        return ret;
    }

    @GET
    @Path("/assets")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_ASSETS + "\")")
    public VXAssetList searchXAssets(@Context HttpServletRequest request) {
        logger.debug("==> AssetREST.searchXAssets()");

        VXAssetList         ret      = new VXAssetList();
        SearchFilter        filter   = searchUtil.getSearchFilterFromLegacyRequestForRepositorySearch(request, xAssetService.sortFields);
        List<RangerService> services = serviceREST.getServices(filter);

        if (services != null) {
            List<VXAsset> assets = new ArrayList<>();

            for (RangerService service : services) {
                VXAsset asset = serviceUtil.toVXAsset(service);

                if (asset != null) {
                    assets.add(asset);
                }
            }

            ret.setVXAssets(assets);
            ret.setTotalCount(assets.size());
            ret.setResultSize(assets.size());
        }

        logger.debug("<== AssetREST.searchXAssets(): count={}", ret.getListSize());

        return ret;
    }

    @GET
    @Path("/assets/count")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_X_ASSETS + "\")")
    public VXLong countXAssets(@Context HttpServletRequest request) {
        logger.debug("==> AssetREST.countXAssets()");

        SearchFilter filter = searchUtil.getSearchFilterFromLegacyRequest(request, xResourceService.sortFields);

        filter.setMaxRows(Integer.MAX_VALUE);

        List<RangerService> services      = serviceREST.getServices(filter);
        int                 servicesCount = 0;

        if (services != null) {
            for (RangerService service : services) {
                VXAsset asset = serviceUtil.toVXAsset(service);

                if (asset != null) {
                    servicesCount++;
                }
            }
        }

        VXLong ret = new VXLong();

        ret.setValue(servicesCount);

        logger.debug("<== AssetREST.countXAssets(): {}", ret);

        return ret;
    }

    @GET
    @Path("/resources/{id}")
    @Produces("application/json")
    public VXResource getXResource(@PathParam("id") Long id) {
        logger.debug("==> AssetREST.getXResource({})", id);

        RangerService service = null;
        RangerPolicy  policy  = serviceREST.getPolicy(id);

        if (policy != null) {
            service = serviceREST.getServiceByName(policy.getService());
        }

        VXResource ret = serviceUtil.toVXResource(policy, service);

        logger.debug("<== AssetREST.getXResource({}): {}", id, ret);

        return ret;
    }

    @POST
    @Path("/resources")
    @Consumes("application/json")
    @Produces("application/json")
    public VXResource createXResource(VXResource vXResource) {
        logger.debug("==> AssetREST.createXResource({})", vXResource);

        RangerService service      = serviceREST.getService(vXResource.getAssetId());
        RangerPolicy  policy       = serviceUtil.toRangerPolicy(vXResource, service);
        RangerPolicy createdPolicy = serviceREST.createPolicy(policy, null);
        VXResource   ret           = serviceUtil.toVXResource(createdPolicy, service);

        logger.debug("<== AssetREST.createXResource({}): {}", vXResource, ret);

        return ret;
    }

    @PUT
    @Path("/resources/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    public VXResource updateXResource(VXResource vXResource, @PathParam("id") Long id) {
        logger.debug("==> AssetREST.updateXResource({})", vXResource);

        // if vXResource.id is specified, it should be same as the param 'id'
        if (vXResource.getId() == null) {
            vXResource.setId(id);
        } else if (!vXResource.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "resource Id mismatch", true);
        }

        RangerService service       = serviceREST.getService(vXResource.getAssetId());
        RangerPolicy  policy        = serviceUtil.toRangerPolicy(vXResource, service);
        RangerPolicy  updatedPolicy = serviceREST.updatePolicy(policy, policy.getId());
        VXResource    ret           = serviceUtil.toVXResource(updatedPolicy, service);

        logger.debug("<== AssetREST.updateXResource({}): {}", vXResource, ret);

        return ret;
    }

    @DELETE
    @Path("/resources/{id}")
    @RangerAnnotationClassName(class_name = VXResource.class)
    public void deleteXResource(@PathParam("id") Long id, @Context HttpServletRequest request) {
        logger.debug("==> AssetREST.deleteXResource({})", id);

        serviceREST.deletePolicy(id);

        logger.debug("<== AssetREST.deleteXResource({})", id);
    }

    @GET
    @Path("/resources")
    @Produces("application/json")
    public VXResourceList searchXResources(@Context HttpServletRequest request) {
        logger.debug("==> AssetREST.searchXResources()");

        VXResourceList     ret      = new VXResourceList();
        SearchFilter       filter   = searchUtil.getSearchFilterFromLegacyRequest(request, xResourceService.sortFields);
        List<RangerPolicy> policies = serviceREST.getPolicies(filter);

        if (policies != null) {
            List<VXResource> resources = new ArrayList<>();

            for (RangerPolicy policy : policies) {
                RangerService service  = serviceREST.getServiceByName(policy.getService());
                VXResource    resource = serviceUtil.toVXResource(policy, service);

                if (resource != null) {
                    resources.add(resource);
                }
            }

            ret.setVXResources(resources);
            ret.setTotalCount(resources.size());
            ret.setResultSize(resources.size());
        }

        logger.debug("<== AssetREST.searchXResources(): count={}", ret.getResultSize());

        return ret;
    }

    @GET
    @Path("/resources/count")
    @Produces("application/json")
    public VXLong countXResources(@Context HttpServletRequest request) {
        logger.debug("==> AssetREST.countXResources()");

        SearchFilter filter = searchUtil.getSearchFilterFromLegacyRequest(request, xResourceService.sortFields);

        filter.setMaxRows(Integer.MAX_VALUE);

        List<RangerPolicy> policies      = serviceREST.getPolicies(filter);
        int                policiesCount = 0;

        if (policies != null) {
            Map<String, RangerService> services = new HashMap<>();

            for (RangerPolicy policy : policies) {
                RangerService service = services.get(policy.getService());

                if (service == null) {
                    service = serviceREST.getServiceByName(policy.getService());

                    services.put(policy.getService(), service);
                }

                VXResource resource = serviceUtil.toVXResource(policy, service);

                if (resource != null) {
                    policiesCount++;
                }
            }
        }

        VXLong ret = new VXLong();

        ret.setValue(policiesCount);

        logger.debug("<== AssetREST.countXResources(): {}", ret);

        return ret;
    }

    @GET
    @Path("/credstores/{id}")
    @Produces("application/json")
    public VXCredentialStore getXCredentialStore(@PathParam("id") Long id) {
        return assetMgr.getXCredentialStore(id);
    }

    @POST
    @Path("/credstores")
    @Consumes("application/json")
    @Produces("application/json")
    public VXCredentialStore createXCredentialStore(VXCredentialStore vXCredentialStore) {
        return assetMgr.createXCredentialStore(vXCredentialStore);
    }

    @PUT
    @Path("/credstores")
    @Consumes("application/json")
    @Produces("application/json")
    public VXCredentialStore updateXCredentialStore(VXCredentialStore vXCredentialStore) {
        return assetMgr.updateXCredentialStore(vXCredentialStore);
    }

    @DELETE
    @Path("/credstores/{id}")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    @RangerAnnotationClassName(class_name = VXCredentialStore.class)
    public void deleteXCredentialStore(@PathParam("id") Long id, @Context HttpServletRequest request) {
        boolean force = false;

        assetMgr.deleteXCredentialStore(id, force);
    }

    @GET
    @Path("/credstores")
    @Produces("application/json")
    public VXCredentialStoreList searchXCredentialStores(@Context HttpServletRequest request) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xCredentialStoreService.sortFields);

        return assetMgr.searchXCredentialStores(searchCriteria);
    }

    @GET
    @Path("/credstores/count")
    @Produces("application/json")
    public VXLong countXCredentialStores(@Context HttpServletRequest request) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xCredentialStoreService.sortFields);

        return assetMgr.getXCredentialStoreSearchCount(searchCriteria);
    }

    @GET
    @Path("/exportAudit")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_X_POLICY_EXPORT_AUDITS + "\")")
    public VXPolicyExportAuditList searchXPolicyExportAudits(@Context HttpServletRequest request) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xPolicyExportAudits.sortFields);

        searchUtil.extractString(request, searchCriteria, "agentId", "The XA agent id pulling the policies.", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "clientIP", "The XA agent ip pulling the policies.", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "repositoryName", "Repository name for which export was done.", StringUtil.VALIDATION_TEXT);
        searchUtil.extractInt(request, searchCriteria, "httpRetCode", "HTTP response code for exported policy.");
        searchUtil.extractDate(request, searchCriteria, "startDate", "Start Date", null);
        searchUtil.extractDate(request, searchCriteria, "endDate", "End Date", null);
        searchUtil.extractString(request, searchCriteria, "cluster", "Cluster Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "zoneName", "Zone Name", StringUtil.VALIDATION_TEXT);

        return assetMgr.searchXPolicyExportAudits(searchCriteria);
    }

    @GET
    @Path("/report")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_REPORT_LOGS + "\")")
    public VXTrxLogList getReportLogs(@Context HttpServletRequest request) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xTrxLogService.getSortFields());

        searchUtil.extractInt(request, searchCriteria, "objectClassType", "audit type.");
        searchUtil.extractInt(request, searchCriteria, "objectId", "Object ID");
        searchUtil.extractString(request, searchCriteria, "attributeName", "Attribute Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "action", "CRUD Action Type", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "sessionId", "Session Id", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "owner", "Owner", StringUtil.VALIDATION_TEXT);
        searchUtil.extractDate(request, searchCriteria, "startDate", "Trasaction date since", "MM/dd/yyyy");
        searchUtil.extractDate(request, searchCriteria, "endDate", "Trasaction date till", "MM/dd/yyyy");

        return assetMgr.getReportLogs(searchCriteria);
    }

    @GET
    @Path("/report/{transactionId}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_TRANSACTION_REPORT + "\")")
    public VXTrxLogList getTransactionReport(@Context HttpServletRequest request, @PathParam("transactionId") String transactionId) {
        return assetMgr.getTransactionReport(transactionId);
    }

    @GET
    @Path("/accessAudit")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_ACCESS_LOGS + "\")")
    public VXAccessAuditList getAccessLogs(@Context HttpServletRequest request, @QueryParam("timeZone") String timeZone) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xAccessAuditService.sortFields);

        searchUtil.extractString(request, searchCriteria, "accessType", "Access Type", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "aclEnforcer", "Access Enforcer", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "agentId", "Application", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "repoName", "Service Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "sessionId", "Session ID", StringUtil.VALIDATION_TEXT);
        searchUtil.extractStringList(request, searchCriteria, "requestUser", "Users", "requestUser", null, StringUtil.VALIDATION_TEXT);
        searchUtil.extractStringList(request, searchCriteria, "excludeUser", "Exclude Users", "-requestUser", null, StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "requestData", "Request Data", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "resourcePath", "Resource Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "clientIP", "Client IP", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "resourceType", "Resource Type", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "excludeServiceUser", "Exclude Service User", StringUtil.VALIDATION_TEXT);

        searchUtil.extractInt(request, searchCriteria, "auditType", "Audit Type");
        searchUtil.extractInt(request, searchCriteria, "accessResult", "Result");
        searchUtil.extractInt(request, searchCriteria, "assetId", "Asset ID");
        searchUtil.extractLong(request, searchCriteria, "policyId", "Policy ID");
        searchUtil.extractInt(request, searchCriteria, "repoType", "Service Type");
        searchUtil.extractDate(request, searchCriteria, "startDate", "Start Date", "MM/dd/yyyy");
        searchUtil.extractDate(request, searchCriteria, "endDate", "End Date", "MM/dd/yyyy");
        searchUtil.extractString(request, searchCriteria, "tags", "tags", null);
        searchUtil.extractString(request, searchCriteria, "cluster", "Cluster Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractStringList(request, searchCriteria, "zoneName", "Zone Name List", "zoneName", null, null);
        searchUtil.extractString(request, searchCriteria, "agentHost", "Agent Host Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "eventId", "Event Id", null);

        boolean      isKeyAdmin      = msBizUtil.isKeyAdmin();
        boolean      isAuditKeyAdmin = msBizUtil.isAuditKeyAdmin();
        XXServiceDef xxServiceDef    = daoManager.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KMS_NAME);

        if (isKeyAdmin && xxServiceDef != null || isAuditKeyAdmin && xxServiceDef != null) {
            searchCriteria.getParamList().put("repoType", xxServiceDef.getId());
        } else if (xxServiceDef != null) {
            searchCriteria.getParamList().put("-repoType", xxServiceDef.getId());
        }

        VXAccessAuditList vxAccessAuditList = assetMgr.getAccessLogs(searchCriteria);

        if (timeZone != null && !StringUtils.isBlank(timeZone)) {
            vxAccessAuditList.getVXAccessAudits().forEach(vxAccessAudit -> {
                String zonedEventTime = convertToTimeZone(vxAccessAudit.getEventTime(), timeZone);

                if (zonedEventTime == null || zonedEventTime.isEmpty()) {
                    throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Passed timeZone value is invalid", true);
                }

                vxAccessAudit.setZonedEventTime(zonedEventTime);
            });
        } else {
            vxAccessAuditList.getVXAccessAudits().forEach(vxAccessAudit -> vxAccessAudit.setZonedEventTime(new SimpleDateFormat(RestUtil.ZONED_EVENT_TIME_FORMAT).format(vxAccessAudit.getEventTime())));
        }

        return vxAccessAuditList;
    }

    @POST
    @Path("/resources/grant")
    @Consumes("application/json")
    @Produces("application/json")
    public VXPolicy grantPermission(@Context HttpServletRequest request, VXPolicy vXPolicy) {
        RESTResponse ret;

        logger.debug("==> AssetREST.grantPermission({})", vXPolicy);

        if (vXPolicy != null) {
            String             serviceName        = vXPolicy.getRepositoryName();
            GrantRevokeRequest grantRevokeRequest = serviceUtil.toGrantRevokeRequest(vXPolicy);

            try {
                ret = serviceREST.grantAccess(serviceName, grantRevokeRequest, request);
            } catch (WebApplicationException excp) {
                throw excp;
            } catch (Throwable e) {
                logger.error("{} Grant Access Failed for the request {}", HttpServletResponse.SC_BAD_REQUEST, vXPolicy, e);

                throw restErrorUtil.createRESTException("Grant Access Failed for the request: " + vXPolicy + ". " + e.getMessage());
            }
        } else {
            logger.error("{} Bad Request parameter", HttpServletResponse.SC_BAD_REQUEST);

            throw restErrorUtil.createRESTException("Bad Request parameter");
        }

        logger.debug("<== AssetREST.grantPermission({})", ret);

        // TO DO Current Grant REST doesn't return a policy so returning a null value. Has to be replace with VXpolicy.
        return vXPolicy;
    }

    @POST
    @Path("/resources/revoke")
    @Consumes("application/json")
    @Produces("application/json")
    public VXPolicy revokePermission(@Context HttpServletRequest request, VXPolicy vXPolicy) {
        RESTResponse ret;

        logger.debug("==> AssetREST.revokePermission({})", vXPolicy);

        if (vXPolicy != null) {
            String             serviceName        = vXPolicy.getRepositoryName();
            GrantRevokeRequest grantRevokeRequest = serviceUtil.toGrantRevokeRequest(vXPolicy);

            try {
                ret = serviceREST.revokeAccess(serviceName, grantRevokeRequest, request);
            } catch (WebApplicationException excp) {
                throw excp;
            } catch (Throwable e) {
                logger.error("{} Revoke Access Failed for the request {}", HttpServletResponse.SC_BAD_REQUEST, vXPolicy, e);

                throw restErrorUtil.createRESTException("Revoke Access Failed for the request: " + vXPolicy + ". " + e.getMessage());
            }
        } else {
            logger.error("{} Bad Request parameter", HttpServletResponse.SC_BAD_REQUEST);

            throw restErrorUtil.createRESTException("Bad Request parameter");
        }

        logger.debug("<== AssetREST.revokePermission({})", ret);

        return vXPolicy;
    }

    @GET
    @Path("/ugsyncAudits")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_UGSYNC_AUDITS + "\")")
    public VXUgsyncAuditInfoList getUgsyncAudits(@Context HttpServletRequest request) {
        SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(request, xAccessAuditService.sortFields);

        searchUtil.extractString(request, searchCriteria, "userName", "User Name", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "sessionId", "Session Id", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "syncSource", "Sync Source", StringUtil.VALIDATION_TEXT);
        searchUtil.extractString(request, searchCriteria, "syncSourceInfo", "Sync Source Info", StringUtil.VALIDATION_TEXT);
        searchUtil.extractLong(request, searchCriteria, "noOfUsers", "No of Users");
        searchUtil.extractLong(request, searchCriteria, "noOfGroups", "No of Groups");
        searchUtil.extractDate(request, searchCriteria, "startDate", "Start Date", "MM/dd/yyyy");
        searchUtil.extractDate(request, searchCriteria, "endDate", "End Date", "MM/dd/yyyy");

        return assetMgr.getUgsyncAudits(searchCriteria);
    }

    @GET
    @Path("/ugsyncAudits/{syncSource}")
    @Encoded
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_UGSYNC_AUDITS_BY_SYNCSOURCE + "\")")
    public VXUgsyncAuditInfoList getUgsyncAuditsBySyncSource(@PathParam("syncSource") String syncSource) {
        return assetMgr.getUgsyncAuditsBySyncSource(syncSource);
    }
}
