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

import com.google.gson.JsonSyntaxException;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.PolicyRefUpdater;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.RangerPolicyAdmin;
import org.apache.ranger.biz.RangerPolicyAdminCacheForEngineOptions;
import org.apache.ranger.biz.RoleDBStore;
import org.apache.ranger.biz.SecurityZoneDBStore;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceDBStore.JSON_FILE_NAME_TYPE;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXRoleDao;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXPolicyLabel;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXSecurityZoneRefService;
import org.apache.ranger.entity.XXSecurityZoneRefTagService;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceHeaderInfo;
import org.apache.ranger.plugin.model.ServiceDeleteResponse;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerPurgeResult;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.web.filter.RangerCSRFPreventionFilter;
import org.apache.ranger.service.RangerPluginInfoService;
import org.apache.ranger.service.RangerPolicyLabelsService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerExportPolicyList;
import org.apache.ranger.view.RangerPluginInfoList;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
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
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME;

@Path("plugins")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ServiceREST {
    private static final Logger LOG      = LoggerFactory.getLogger(ServiceREST.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("rest.ServiceREST");

    public static final String PARAM_SERVICE_NAME                   = "serviceName";
    public static final String PARAM_SERVICE_TYPE                   = "serviceType";
    public static final String PARAM_POLICY_NAME                    = "policyName";
    public static final String PARAM_ZONE_NAME                      = "zoneName";
    public static final String PARAM_UPDATE_IF_EXISTS               = "updateIfExists";
    public static final String PARAM_MERGE_IF_EXISTS                = "mergeIfExists";
    public static final String PARAM_DELETE_IF_EXISTS               = "deleteIfExists";
    public static final String PARAM_IMPORT_IN_PROGRESS             = "importInProgress";
    public static final String Allowed_User_List_For_Download       = "policy.download.auth.users";
    public static final String Allowed_User_List_For_Grant_Revoke   = "policy.grantrevoke.auth.users";
    public static final String isCSRF_ENABLED                       = "ranger.rest-csrf.enabled";
    public static final String BROWSER_USER_AGENT_PARAM             = "ranger.rest-csrf.browser-useragents-regex";
    public static final String CUSTOM_METHODS_TO_IGNORE_PARAM       = "ranger.rest-csrf.methods-to-ignore";
    public static final String CUSTOM_HEADER_PARAM                  = "ranger.rest-csrf.custom-header";
    public static final String CSRF_TOKEN_LENGTH                    = "ranger.rest-csrf.token.length";
    public static final String POLICY_MATCHING_ALGO_BY_POLICYNAME   = "matchByName";
    public static final String POLICY_MATCHING_ALGO_BY_RESOURCE     = "matchByPolicySignature";
    public static final String PARAM_POLICY_MATCHING_ALGORITHM      = "policyMatchingAlgorithm";
    public static final String PURGE_RECORD_TYPE_LOGIN_LOGS         = "login_records";
    public static final String PURGE_RECORD_TYPE_TRX_LOGS           = "trx_records";
    public static final String PURGE_RECORD_TYPE_POLICY_EXPORT_LOGS = "policy_export_logs";

    private final RangerAdminConfig config                              = RangerAdminConfig.getInstance();
    private final int               maxPolicyNameLength                 = config.getInt("ranger.policyname.maxlength", 255);
    private final boolean           isPolicyNameLengthValidationEnabled = config.getBoolean("ranger.policyname.maxlength.validation.enabled", true);

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    ServiceMgr serviceMgr;

    @Autowired
    XUserService xUserService;

    @Autowired
    AssetMgr assetMgr;

    @Autowired
    XUserMgr userMgr;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RoleDBStore roleDBStore;

    @Autowired
    SecurityZoneDBStore zoneStore;

    @Autowired
    ServiceUtil serviceUtil;

    @Autowired
    RangerPolicyService policyService;

    @Autowired
    RangerPolicyLabelsService<XXPolicyLabel, ?> policyLabelsService;

    @Autowired
    RangerServiceService svcService;

    @Autowired
    RangerServiceDefService serviceDefService;

    @Autowired
    RangerPluginInfoService pluginInfoService;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    GUIDUtil guidUtil;

    @Autowired
    RangerValidatorFactory validatorFactory;

    @Autowired
    RangerDaoManager daoManager;

    @Autowired
    TagDBStore tagStore;

    @Autowired
    RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;

    private RangerPolicyEngineOptions delegateAdminOptions;
    private RangerPolicyEngineOptions policySearchAdminOptions;
    private RangerPolicyEngineOptions defaultAdminOptions;

    public static Map<String, Object> getAccessResourceObjectMap(Map<String, String> map) {
        Map<String, Object> ret = null;

        if (map != null) {
            ret = new HashMap<>(map.size());

            for (Map.Entry<String, String> e : map.entrySet()) {
                if (e.getValue().contains(",")) {
                    List<String> values = Arrays.asList(e.getValue().split(","));

                    ret.put(e.getKey(), values);
                } else {
                    ret.put(e.getKey(), e.getValue());
                }
            }
        }

        return ret;
    }

    @PostConstruct
    public void initStore() {
        tagStore.setServiceStore(svcStore);

        delegateAdminOptions     = getDelegatedAdminPolicyEngineOptions();
        policySearchAdminOptions = getPolicySearchRangerAdminPolicyEngineOptions();
        defaultAdminOptions      = getDefaultRangerAdminPolicyEngineOptions();
    }

    @POST
    @Path("/definitions")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE_DEF + "\")")
    public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
        LOG.debug("==> ServiceREST.createServiceDef({})", serviceDef);

        RangerServiceDef ret;
        RangerPerfTracer perf = null;

        /**
         * If display name is blank (EMPTY String or NULL), use name.
         */
        if (StringUtils.isBlank(serviceDef.getDisplayName())) {
            serviceDef.setDisplayName(serviceDef.getName());
        }

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createServiceDef(serviceDefName=" + serviceDef.getName() + ")");
            }

            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(serviceDef, Action.CREATE);

            bizUtil.hasAdminPermissions("Service-Def");
            bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());
            bizUtil.blockAuditorRoleUser();

            ret = svcStore.createServiceDef(serviceDef);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createServiceDef({}) failed", serviceDef, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.createServiceDef({}): {}", serviceDef, ret);

        return ret;
    }

    @PUT
    @Path("/definitions/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE_DEF + "\")")
    public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef, @PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.updateServiceDef(serviceDefName={})", serviceDef.getName());

        // if serviceDef.id and param 'id' are specified, serviceDef.id should be same as the param 'id'
        // if serviceDef.id is null, then set param 'id' into serviceDef Object
        if (serviceDef.getId() == null) {
            serviceDef.setId(id);
        } else if (StringUtils.isBlank(serviceDef.getName()) && !serviceDef.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "serviceDef Id mismatch", true);
        }

        RangerServiceDef ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateServiceDef(" + serviceDef.getName() + ")");
            }

            /**
             * If display name is blank (EMPTY String or NULL), use previous display name.
             */
            if (StringUtils.isBlank(serviceDef.getDisplayName())) {
                RangerServiceDef rangerServiceDef = svcStore.getServiceDef(serviceDef.getId());

                // If previous display name is blank (EMPTY String or NULL), user name.
                if (Objects.isNull(rangerServiceDef) || StringUtils.isBlank(rangerServiceDef.getDisplayName())) {
                    serviceDef.setDisplayName(serviceDef.getName());
                } else {
                    serviceDef.setDisplayName(rangerServiceDef.getDisplayName());
                }
            }

            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(serviceDef, Action.UPDATE);

            bizUtil.hasAdminPermissions("Service-Def");
            bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());
            bizUtil.blockAuditorRoleUser();

            ret = svcStore.updateServiceDef(serviceDef);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateServiceDef({}) failed", serviceDef, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.updateServiceDef({}): {}", serviceDef, ret);

        return ret;
    }

    @DELETE
    @Path("/definitions/{id}")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE_DEF + "\")")
    public void deleteServiceDef(@PathParam("id") Long id, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.deleteServiceDef({})", id);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteServiceDef(serviceDefId=" + id + ")");
            }

            RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);

            validator.validate(id, Action.DELETE);

            bizUtil.hasAdminPermissions("Service-Def");

            XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);

            if (xServiceDef != null) {
                bizUtil.hasKMSPermissions("Service-Def", xServiceDef.getImplclassname());

                String  forceDeleteStr = request.getParameter("forceDelete");
                boolean forceDelete    = !StringUtils.isEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr);

                svcStore.deleteServiceDef(id, forceDelete);
            } else {
                LOG.error("Cannot retrieve service-definition:[{}] for deletion", id);

                throw new Exception("deleteServiceDef(" + id + ") failed");
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteServiceDef({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.deleteServiceDef({})", id);
    }

    @GET
    @Path("/definitions/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF + "\")")
    public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.getServiceDef({})", id);

        RangerServiceDef ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDef(serviceDefId=" + id + ")");
            }

            XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);

            if (xServiceDef != null) {
                if (EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(xServiceDef.getName())) {
                    if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_TAG_BASED_POLICIES)) {
                        throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the tag module.", true);
                    }
                }

                if (!bizUtil.hasAccess(xServiceDef, null)) {
                    throw restErrorUtil.createRESTException("User is not allowed to access service-def, id: " + xServiceDef.getId(), MessageEnums.OPER_NO_PERMISSION);
                }
            }

            ret = svcStore.getServiceDef(id);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServiceDef({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getServiceDef({}): {}", id, ret);

        return ret;
    }

    @GET
    @Path("/definitions/name/{name}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF_BY_NAME + "\")")
    public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
        LOG.debug("==> ServiceREST.getServiceDefByName(serviceDefName={})", name);

        RangerServiceDef ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefByName(" + name + ")");
            }

            XXServiceDef xServiceDef = daoManager.getXXServiceDef().findByName(name);

            if (xServiceDef != null) {
                if (EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(xServiceDef.getName())) {
                    if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_TAG_BASED_POLICIES)) {
                        throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the tag module", true);
                    }
                }

                if (!bizUtil.hasAccess(xServiceDef, null)) {
                    throw restErrorUtil.createRESTException("User is not allowed to access service-def: " + xServiceDef.getName(), MessageEnums.OPER_NO_PERMISSION);
                }
            }

            ret = svcStore.getServiceDefByName(name);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServiceDefByName({}) failed", name, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getServiceDefByName({}): {}", name, ret);

        return ret;
    }

    @GET
    @Path("/definitions")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEFS + "\")")
    public RangerServiceDefList getServiceDefs(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getServiceDefs()");

        if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_RESOURCE_BASED_POLICIES)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the " + RangerConstants.MODULE_RESOURCE_BASED_POLICIES + " module.", true);
        }

        RangerServiceDefList ret        = null;
        RangerPerfTracer     perf       = null;
        SearchFilter         filter     = searchUtil.getSearchFilter(request, serviceDefService.sortFields);
        String               pageSource = request.getParameter("pageSource");

        if (pageSource != null) {
            filter.setParam("pageSource", pageSource);
        }

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefs()");
            }

            PList<RangerServiceDef> paginatedSvcDefs = svcStore.getPaginatedServiceDefs(filter);

            if (paginatedSvcDefs != null) {
                ret = new RangerServiceDefList();

                ret.setServiceDefs(paginatedSvcDefs.getList());
                ret.setPageSize(paginatedSvcDefs.getPageSize());
                ret.setResultSize(paginatedSvcDefs.getResultSize());
                ret.setStartIndex(paginatedSvcDefs.getStartIndex());
                ret.setTotalCount(paginatedSvcDefs.getTotalCount());
                ret.setSortBy(paginatedSvcDefs.getSortBy());
                ret.setSortType(paginatedSvcDefs.getSortType());
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServiceDefs() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getServiceDefs(): count={}", (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    @GET
    @Path("/policies/{serviceDefName}/for-resource")
    @Produces("application/json")
    public List<RangerPolicy> getPoliciesForResource(@PathParam("serviceDefName") String serviceDefName, @DefaultValue("") @QueryParam("serviceName") String serviceName, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPoliciesForResource(service-type={}, service-name={})", serviceDefName, serviceName);

        List<RangerPolicy>  ret               = new ArrayList<>();
        List<RangerService> services          = new ArrayList<>();
        Map<String, Object> resource          = new HashMap<>();
        String              validationMessage = validateResourcePoliciesRequest(serviceDefName, serviceName, request, services, resource);

        if (StringUtils.isNotEmpty(validationMessage)) {
            LOG.error("Invalid request: [{}]", validationMessage);

            throw restErrorUtil.createRESTException(validationMessage, MessageEnums.INVALID_INPUT_DATA);
        } else {
            RangerService service = services.get(0);

            LOG.debug("getServicePolicies with service-name={}", service.getName());

            RangerPolicyAdmin policyAdmin;

            try {
                policyAdmin = getPolicyAdminForSearch(service.getName());
            } catch (Exception e) {
                LOG.error("Cannot initialize Policy-Engine", e);

                throw restErrorUtil.createRESTException("Cannot initialize Policy Engine", MessageEnums.ERROR_SYSTEM);
            }

            if (policyAdmin != null) {
                ret = policyAdmin.getMatchingPolicies(new RangerAccessResourceImpl(resource));
                ret = applyAdminAccessFilter(ret);
            }
        }

        LOG.debug("<== ServiceREST.getPoliciesForResource(service-type={}, service-name={}) : {}", serviceDefName, serviceName, ret);

        return ret;
    }

    @POST
    @Path("/services")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE + "\")")
    public RangerService createService(RangerService service) {
        LOG.debug("==> ServiceREST.createService({})", service);

        RangerService    ret;
        RangerPerfTracer perf = null;

        /**
         * If display name is blank (EMPTY String or NULL), use name.
         */
        if (StringUtils.isBlank(service.getDisplayName())) {
            service.setDisplayName(service.getName());
        }

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createService(serviceName=" + service.getName() + ")");
            }

            RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);

            validator.validate(service, Action.CREATE);

            if (!StringUtils.isEmpty(service.getName().trim())) {
                service.setName(service.getName().trim());
            }

            if (!StringUtils.isEmpty(service.getDisplayName().trim())) {
                service.setDisplayName(service.getDisplayName().trim());
            }

            UserSessionBase session      = ContextUtil.getCurrentUserSession();
            XXServiceDef    xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());

            if (session != null && !session.isSpnegoEnabled()) {
                bizUtil.hasAdminPermissions("Services");

                // TODO: As of now we are allowing SYS_ADMIN to create all the
                // services including KMS
                bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
            }

            if (session != null && session.isSpnegoEnabled()) {
                if (session.isKeyAdmin() && !EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xxServiceDef.getImplclassname())) {
                    throw restErrorUtil.createRESTException("KeyAdmin can create/update/delete only KMS ", MessageEnums.OPER_NO_PERMISSION);
                }

                if ((!session.isKeyAdmin() && !session.isUserAdmin()) && EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xxServiceDef.getImplclassname())) {
                    throw restErrorUtil.createRESTException("User cannot create/update/delete KMS Service", MessageEnums.OPER_NO_PERMISSION);
                }
            }

            bizUtil.blockAuditorRoleUser();

            String serviceType = xxServiceDef != null ? xxServiceDef.getName() : null;

            if (StringUtils.isBlank(service.getTagService()) && !StringUtils.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME, serviceType) && !StringUtils.equals(EMBEDDED_SERVICEDEF_GDS_NAME, serviceType) && !StringUtils.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KMS_NAME, serviceType)) {
                createOrGetLinkedServices(service);
            }

            ret = svcStore.createService(service);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createService({}) failed", service, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.createService({}): {}", service, ret);

        return ret;
    }

    @PUT
    @Path("/services/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE + "\")")
    public RangerService updateService(RangerService service, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.updateService(): {}", service);

        RangerService    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateService(serviceName=" + service.getName() + ")");
            }

            /**
             * If display name is blank (EMPTY String or NULL), use previous display name.
             */
            if (StringUtils.isBlank(service.getDisplayName())) {
                RangerService rangerService = svcStore.getService(service.getId());

                // If previous display name is blank (EMPTY String or NULL), user name.
                if (Objects.isNull(rangerService) || StringUtils.isBlank(rangerService.getDisplayName())) {
                    service.setDisplayName(service.getName());
                } else {
                    service.setDisplayName(rangerService.getDisplayName());
                }
            }

            RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);

            validator.validate(service, Action.UPDATE);

            if (!StringUtils.isEmpty(service.getName().trim())) {
                service.setName(service.getName().trim());
            }

            if (!StringUtils.isEmpty(service.getDisplayName().trim())) {
                service.setDisplayName(service.getDisplayName().trim());
            }

            bizUtil.hasAdminPermissions("Services");

            // TODO: As of now we are allowing SYS_ADMIN to create all the
            // services including KMS

            XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());

            bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
            bizUtil.blockAuditorRoleUser();

            Map<String, Object> options = getOptions(request);

            ret = svcStore.updateService(service, options);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updateService({}) failed", service, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.updateService({}): {}", service, ret);

        return ret;
    }

    @DELETE
    @Path("/services/{id}")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE + "\")")
    public void deleteService(@PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.deleteService({})", id);

        String deletedServiceName = deleteServiceById(id);

        LOG.debug("<== ServiceREST.deleteService() - [id={}],[deletedServiceName={}]", deletedServiceName, deletedServiceName);
    }

    @GET
    @Path("/services/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE + "\")")
    public RangerService getService(@PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.getService({})", id);

        RangerService    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceId=" + id + ")");
            }

            ret = svcStore.getService(id);

            if (ret != null) {
                UserSessionBase userSession = ContextUtil.getCurrentUserSession();

                if (userSession != null && userSession.getLoginId() != null) {
                    VXUser loggedInVXUser = xUserService.getXUserByUserName(userSession.getLoginId());

                    if (loggedInVXUser != null) {
                        if (loggedInVXUser.getUserRoleList().size() == 1 && loggedInVXUser.getUserRoleList().contains(RangerConstants.ROLE_USER)) {
                            hideCriticalServiceDetailsForRoleUser(ret);
                        }
                    }
                }
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getService({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getService({}): {}", id, ret);

        return ret;
    }

    @GET
    @Path("/services/name/{name}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_BY_NAME + "\")")
    public RangerService getServiceByName(@PathParam("name") String name) {
        LOG.debug("==> ServiceREST.getServiceByName({})", name);

        RangerService    ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceName=" + name + ")");
            }

            ret = svcStore.getServiceByName(name);

            if (ret != null) {
                UserSessionBase userSession = ContextUtil.getCurrentUserSession();

                if (userSession != null && userSession.getLoginId() != null) {
                    VXUser loggedInVXUser = xUserService.getXUserByUserName(userSession.getLoginId());

                    if (loggedInVXUser != null) {
                        if (loggedInVXUser.getUserRoleList().size() == 1 && loggedInVXUser.getUserRoleList().contains(RangerConstants.ROLE_USER)) {
                            hideCriticalServiceDetailsForRoleUser(ret);
                        }
                    }
                }
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServiceByName({}) failed", name, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getServiceByName({}): {}", name, ret);

        return ret;
    }

    @GET
    @Path("/services")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICES + "\")")
    public RangerServiceList getServices(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getServices()");

        RangerServiceList ret    = null;
        RangerPerfTracer  perf   = null;
        SearchFilter      filter = searchUtil.getSearchFilter(request, svcService.sortFields);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
            }

            PList<RangerService> paginatedSvcs = svcStore.getPaginatedServices(filter);

            if (paginatedSvcs != null && !paginatedSvcs.getList().isEmpty()) {
                UserSessionBase userSession = ContextUtil.getCurrentUserSession();

                if (userSession != null && userSession.getLoginId() != null) {
                    VXUser loggedInVXUser = xUserService.getXUserByUserName(userSession.getLoginId());

                    if (loggedInVXUser != null) {
                        if (loggedInVXUser.getUserRoleList().size() == 1 && loggedInVXUser.getUserRoleList().contains(RangerConstants.ROLE_USER)) {
                            List<RangerService> updateServiceList = new ArrayList<>();

                            for (RangerService rangerService : paginatedSvcs.getList()) {
                                if (rangerService != null) {
                                    updateServiceList.add(hideCriticalServiceDetailsForRoleUser(rangerService));
                                }
                            }

                            if (!updateServiceList.isEmpty()) {
                                paginatedSvcs.setList(updateServiceList);
                            }
                        }
                    }
                }
            }

            if (paginatedSvcs != null) {
                ret = new RangerServiceList();

                ret.setServices(paginatedSvcs.getList());
                ret.setPageSize(paginatedSvcs.getPageSize());
                ret.setResultSize(paginatedSvcs.getResultSize());
                ret.setStartIndex(paginatedSvcs.getStartIndex());
                ret.setTotalCount(paginatedSvcs.getTotalCount());
                ret.setSortBy(paginatedSvcs.getSortBy());
                ret.setSortType(paginatedSvcs.getSortType());
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServices() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getServices(): count={}", (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    public List<RangerService> getServices(SearchFilter filter) {
        LOG.debug("==> ServiceREST.getServices():");

        List<RangerService> ret;
        RangerPerfTracer    perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
            }

            ret = svcStore.getServices(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServices() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getServices(): count={}", (ret == null ? 0 : ret.size()));

        return ret;
    }

    public List<RangerServiceHeaderInfo> getServiceHeaders(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getServiceHeaders()");

        String  namePrefix         = request.getParameter(SearchFilter.SERVICE_NAME_PREFIX);
        String  svcType            = request.getParameter(SearchFilter.SERVICE_TYPE);
        boolean filterByNamePrefix = StringUtils.isNotBlank(namePrefix);
        boolean filterByType       = StringUtils.isNotBlank(svcType);

        List<RangerServiceHeaderInfo> ret = daoManager.getXXService().findServiceHeaders();

        for (ListIterator<RangerServiceHeaderInfo> iter = ret.listIterator(); iter.hasNext(); ) {
            RangerServiceHeaderInfo serviceHeader = iter.next();

            if (EMBEDDED_SERVICEDEF_GDS_NAME.equals(serviceHeader.getType())) {
                iter.remove();
            } else if (filterByNamePrefix && !StringUtils.startsWithIgnoreCase(serviceHeader.getName(), namePrefix)) {
                iter.remove();
            } else if (filterByType && !StringUtils.equals(serviceHeader.getType(), svcType)) {
                iter.remove();
            } else if (!bizUtil.hasAccess(null, serviceHeader)) {
                iter.remove();
            }
        }

        LOG.debug("<== ServiceREST.getServiceHeaders(namePrefix={}, svcType={}): ret={}", namePrefix, svcType, ret);

        return ret;
    }

    @GET
    @Path("/services/count")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_SERVICES + "\")")
    public Long countServices(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.countServices():");

        Long             ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countService()");
            }

            List<RangerService> services = getServices(request).getServices();

            ret = services == null ? 0L : services.size();
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("countServices() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.countServices(): {}", ret);

        return ret;
    }

    @POST
    @Path("/services/validateConfig")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.VALIDATE_CONFIG + "\")")
    public VXResponse validateConfig(RangerService service) {
        LOG.debug("==> ServiceREST.validateConfig({})", service);

        VXResponse       ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.validateConfig(serviceName=" + service.getName() + ")");
            }

            ret = serviceMgr.validateConfig(service, svcStore);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("validateConfig({}) failed", service, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.validateConfig({}) :{}", service, ret);

        return ret;
    }

    @POST
    @Path("/services/lookupResource/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LOOKUP_RESOURCE + "\")")
    public List<String> lookupResource(@PathParam("serviceName") String serviceName, ResourceLookupContext context) {
        LOG.debug("==> ServiceREST.lookupResource({})", serviceName);

        List<String>     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.lookupResource(serviceName=" + serviceName + ")");
            }

            ret = serviceMgr.lookupResource(serviceName, context, svcStore);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("lookupResource({}, {}) failed", serviceName, context, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.lookupResource({}) :{}", serviceName, ret);

        return ret;
    }

    @POST
    @Path("/services/grant/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    public RESTResponse grantAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest grantRequest, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.grantAccess({}, {})", serviceName, grantRequest);

        RESTResponse     ret  = new RESTResponse();
        RangerPerfTracer perf = null;

        if (grantRequest != null) {
            if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
                try {
                    bizUtil.failUnauthenticatedIfNotAllowed();

                    if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.grantAccess(serviceName=" + serviceName + ")");
                    }

                    // This is an open API - dont care about who calls it. Caller is treated as privileged user
                    boolean hasAdminPrivilege = true;
                    String  loggedInUser      = null;

                    validateGrantRevokeRequest(grantRequest, hasAdminPrivilege, loggedInUser);

                    String               userName    = grantRequest.getGrantor();
                    Set<String>          userGroups  = CollectionUtils.isNotEmpty(grantRequest.getGrantorGroups()) ? grantRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
                    String               ownerUser   = grantRequest.getOwnerUser();
                    RangerAccessResource resource    = new RangerAccessResourceImpl(getAccessResourceObjectMap(grantRequest.getResource()), ownerUser);
                    Set<String>          accessTypes = grantRequest.getAccessTypes();
                    VXUser               vxUser      = xUserService.getXUserByUserName(userName);

                    if (vxUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN_AUDITOR) || vxUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)) {
                        VXResponse vXResponse = new VXResponse();

                        vXResponse.setStatusCode(HttpServletResponse.SC_FORBIDDEN);
                        vXResponse.setMsgDesc("Operation denied. LoggedInUser=" + vxUser.getId() + " is not permitted to perform the action.");

                        throw restErrorUtil.generateRESTException(vXResponse);
                    }

                    RangerService rangerService = svcStore.getServiceByName(serviceName);
                    String        zoneName      = getRangerAdminZoneName(serviceName, grantRequest);
                    boolean       isAdmin       = bizUtil.isUserRangerAdmin(userName) || bizUtil.isUserServiceAdmin(rangerService, userName) || hasAdminAccess(serviceName, zoneName, userName, userGroups, resource, accessTypes);

                    if (!isAdmin) {
                        throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to grant access");
                    }

                    RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, zoneName, userName);

                    if (policy != null) {
                        boolean policyUpdated = ServiceRESTUtil.processGrantRequest(policy, grantRequest);

                        if (policyUpdated) {
                            policy.setZoneName(zoneName);

                            ensureAdminAccess(policy);

                            svcStore.updatePolicy(policy);
                        } else {
                            LOG.error("processGrantRequest processing failed");

                            throw new Exception("processGrantRequest processing failed");
                        }
                    } else {
                        policy = new RangerPolicy();

                        policy.setService(serviceName);
                        policy.setName("grant-" + System.currentTimeMillis()); // TODO: better policy name
                        policy.setDescription("created by grant");
                        policy.setIsAuditEnabled(grantRequest.getEnableAudit());
                        policy.setCreatedBy(userName);

                        Map<String, RangerPolicyResource> policyResources = new HashMap<>();
                        Set<String>                       resourceNames   = resource.getKeys();

                        if (!CollectionUtils.isEmpty(resourceNames)) {
                            for (String resourceName : resourceNames) {
                                policyResources.put(resourceName, getPolicyResource(resource.getValue(resourceName), grantRequest));
                            }
                        }

                        policy.setResources(policyResources);

                        RangerPolicyItem policyItem = new RangerPolicyItem();

                        policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
                        policyItem.addUsers(grantRequest.getUsers());
                        policyItem.addGroups(grantRequest.getGroups());
                        policyItem.addRoles(grantRequest.getRoles());

                        for (String accessType : grantRequest.getAccessTypes()) {
                            policyItem.addAccess(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
                        }

                        policy.addPolicyItem(policyItem);
                        policy.setZoneName(zoneName);

                        ensureAdminAccess(policy);

                        svcStore.createPolicy(policy);
                    }
                } catch (WebApplicationException excp) {
                    throw excp;
                } catch (Throwable excp) {
                    LOG.error("grantAccess({}, {}) failed", serviceName, grantRequest, excp);

                    throw restErrorUtil.createRESTException(excp.getMessage());
                } finally {
                    RangerPerfTracer.log(perf);
                }

                ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
            }
        }

        LOG.debug("<== ServiceREST.grantAccess({}, {}) :{}", serviceName, grantRequest, ret);

        return ret;
    }

    @POST
    @Path("/secure/services/grant/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    public RESTResponse secureGrantAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest grantRequest, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.secureGrantAccess({}, {})", serviceName, grantRequest);

        RESTResponse     ret  = new RESTResponse();
        RangerPerfTracer perf = null;

        bizUtil.blockAuditorRoleUser();

        if (grantRequest != null) {
            if (serviceUtil.isValidService(serviceName, request)) {
                try {
                    if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.scureGrantAccess(serviceName=" + serviceName + ")");
                    }

                    XXService     xService          = daoManager.getXXService().findByName(serviceName);
                    XXServiceDef  xServiceDef       = daoManager.getXXServiceDef().getById(xService.getType());
                    RangerService rangerService     = svcStore.getServiceByName(serviceName);
                    String        loggedInUser      = bizUtil.getCurrentUserLoginId();
                    boolean       hasAdminPrivilege = bizUtil.isAdmin() || bizUtil.isUserServiceAdmin(rangerService, loggedInUser) || bizUtil.isUserAllowedForGrantRevoke(rangerService, loggedInUser);

                    validateGrantRevokeRequest(grantRequest, hasAdminPrivilege, loggedInUser);

                    String               userName    = grantRequest.getGrantor();
                    Set<String>          userGroups  = grantRequest.getGrantorGroups();
                    String               ownerUser   = grantRequest.getOwnerUser();
                    RangerAccessResource resource    = new RangerAccessResourceImpl(getAccessResourceObjectMap(grantRequest.getResource()), ownerUser);
                    Set<String>          accessTypes = grantRequest.getAccessTypes();
                    String               zoneName    = getRangerAdminZoneName(serviceName, grantRequest);
                    boolean              isAllowed   = false;

                    if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
                        if (bizUtil.isKeyAdmin() || bizUtil.isUserAllowedForGrantRevoke(rangerService, loggedInUser)) {
                            isAllowed = true;
                        }
                    } else {
                        isAllowed = bizUtil.isUserRangerAdmin(userName) || bizUtil.isUserServiceAdmin(rangerService, userName) || hasAdminAccess(serviceName, zoneName, userName, userGroups, resource, accessTypes);
                    }

                    if (isAllowed) {
                        RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, zoneName, userName);

                        if (policy != null) {
                            boolean policyUpdated = ServiceRESTUtil.processGrantRequest(policy, grantRequest);

                            if (policyUpdated) {
                                policy.setZoneName(zoneName);

                                ensureAdminAccess(policy);

                                svcStore.updatePolicy(policy);
                            } else {
                                LOG.error("processSecureGrantRequest processing failed");

                                throw new Exception("processSecureGrantRequest processing failed");
                            }
                        } else {
                            policy = new RangerPolicy();

                            policy.setService(serviceName);
                            policy.setName("grant-" + System.currentTimeMillis()); // TODO: better policy name
                            policy.setDescription("created by grant");
                            policy.setIsAuditEnabled(grantRequest.getEnableAudit());
                            policy.setCreatedBy(userName);

                            Map<String, RangerPolicyResource> policyResources = new HashMap<>();
                            Set<String>                       resourceNames   = resource.getKeys();

                            if (!CollectionUtils.isEmpty(resourceNames)) {
                                for (String resourceName : resourceNames) {
                                    policyResources.put(resourceName, getPolicyResource(resource.getValue(resourceName), grantRequest));
                                }
                            }

                            policy.setResources(policyResources);

                            RangerPolicyItem policyItem = new RangerPolicyItem();

                            policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
                            policyItem.addUsers(grantRequest.getUsers());
                            policyItem.addGroups(grantRequest.getGroups());
                            policyItem.addRoles(grantRequest.getRoles());

                            for (String accessType : grantRequest.getAccessTypes()) {
                                policyItem.addAccess(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
                            }

                            policy.addPolicyItem(policyItem);
                            policy.setZoneName(zoneName);

                            ensureAdminAccess(policy);

                            svcStore.createPolicy(policy);
                        }
                    } else {
                        LOG.error("secureGrantAccess({}, {}) failed as User doesn't have permission to grant Policy", serviceName, grantRequest);

                        throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to grant access");
                    }
                } catch (WebApplicationException excp) {
                    throw excp;
                } catch (Throwable excp) {
                    LOG.error("secureGrantAccess({}, {}) failed", serviceName, grantRequest, excp);

                    throw restErrorUtil.createRESTException(excp.getMessage());
                } finally {
                    RangerPerfTracer.log(perf);
                }

                ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
            }
        }

        LOG.debug("<== ServiceREST.secureGrantAccess({}, {}) :{}", serviceName, grantRequest, ret);

        return ret;
    }

    @POST
    @Path("/services/revoke/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    public RESTResponse revokeAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest revokeRequest, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.revokeAccess({}, {})", serviceName, revokeRequest);

        RESTResponse     ret  = new RESTResponse();
        RangerPerfTracer perf = null;

        if (revokeRequest != null) {
            if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
                try {
                    bizUtil.failUnauthenticatedIfNotAllowed();

                    if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.revokeAccess(serviceName=" + serviceName + ")");
                    }

                    // This is an open API - dont care about who calls it. Caller is treated as privileged user
                    boolean hasAdminPrivilege = true;
                    String  loggedInUser      = null;

                    validateGrantRevokeRequest(revokeRequest, hasAdminPrivilege, loggedInUser);

                    String               userName    = revokeRequest.getGrantor();
                    Set<String>          userGroups  = CollectionUtils.isNotEmpty(revokeRequest.getGrantorGroups()) ? revokeRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
                    String               ownerUser   = revokeRequest.getOwnerUser();
                    RangerAccessResource resource    = new RangerAccessResourceImpl(getAccessResourceObjectMap(revokeRequest.getResource()), ownerUser);
                    Set<String>          accessTypes = revokeRequest.getAccessTypes();
                    VXUser               vxUser      = xUserService.getXUserByUserName(userName);

                    if (vxUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN_AUDITOR) || vxUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)) {
                        VXResponse vXResponse = new VXResponse();

                        vXResponse.setStatusCode(HttpServletResponse.SC_FORBIDDEN);
                        vXResponse.setMsgDesc("Operation denied. LoggedInUser=" + vxUser.getId() + " is not permitted to perform the action.");

                        throw restErrorUtil.generateRESTException(vXResponse);
                    }

                    RangerService rangerService = svcStore.getServiceByName(serviceName);
                    String        zoneName      = getRangerAdminZoneName(serviceName, revokeRequest);
                    boolean       isAdmin       = bizUtil.isUserRangerAdmin(userName) || bizUtil.isUserServiceAdmin(rangerService, userName) || hasAdminAccess(serviceName, zoneName, userName, userGroups, resource, accessTypes);

                    if (!isAdmin) {
                        throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to revoke access");
                    }

                    RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, zoneName, userName);

                    if (policy != null) {
                        boolean policyUpdated = ServiceRESTUtil.processRevokeRequest(policy, revokeRequest);

                        if (policyUpdated) {
                            policy.setZoneName(zoneName);

                            ensureAdminAccess(policy);

                            svcStore.updatePolicy(policy);
                        } else {
                            LOG.error("processRevokeRequest processing failed");
                            throw new Exception("processRevokeRequest processing failed");
                        }
                    }
                } catch (WebApplicationException excp) {
                    throw excp;
                } catch (Throwable excp) {
                    LOG.error("secureGrantAccess({}, {}) failed", serviceName, revokeRequest, excp);

                    throw restErrorUtil.createRESTException(excp.getMessage());
                } finally {
                    RangerPerfTracer.log(perf);
                }

                ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
            }
        }

        LOG.debug("<== ServiceREST.revokeAccess({}, {}) :{}", serviceName, revokeRequest, ret);

        return ret;
    }

    @POST
    @Path("/secure/services/revoke/{serviceName}")
    @Consumes("application/json")
    @Produces("application/json")
    public RESTResponse secureRevokeAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest revokeRequest, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.secureRevokeAccess({}, {})", serviceName, revokeRequest);

        RESTResponse     ret  = new RESTResponse();
        RangerPerfTracer perf = null;

        bizUtil.blockAuditorRoleUser();

        if (revokeRequest != null) {
            if (serviceUtil.isValidService(serviceName, request)) {
                try {
                    if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.secureRevokeAccess(serviceName=" + serviceName + ")");
                    }

                    XXService     xService          = daoManager.getXXService().findByName(serviceName);
                    XXServiceDef  xServiceDef       = daoManager.getXXServiceDef().getById(xService.getType());
                    RangerService rangerService     = svcStore.getServiceByName(serviceName);
                    String        loggedInUser      = bizUtil.getCurrentUserLoginId();
                    boolean       hasAdminPrivilege = bizUtil.isAdmin() || bizUtil.isUserServiceAdmin(rangerService, loggedInUser) || bizUtil.isUserAllowedForGrantRevoke(rangerService, loggedInUser);

                    validateGrantRevokeRequest(revokeRequest, hasAdminPrivilege, loggedInUser);

                    String               userName    = revokeRequest.getGrantor();
                    Set<String>          userGroups  = revokeRequest.getGrantorGroups();
                    String               ownerUser   = revokeRequest.getOwnerUser();
                    RangerAccessResource resource    = new RangerAccessResourceImpl(getAccessResourceObjectMap(revokeRequest.getResource()), ownerUser);
                    Set<String>          accessTypes = revokeRequest.getAccessTypes();
                    String               zoneName    = getRangerAdminZoneName(serviceName, revokeRequest);
                    boolean              isAllowed   = false;

                    if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
                        if (bizUtil.isKeyAdmin() || bizUtil.isUserAllowedForGrantRevoke(rangerService, loggedInUser)) {
                            isAllowed = true;
                        }
                    } else {
                        isAllowed = bizUtil.isUserRangerAdmin(userName) || bizUtil.isUserServiceAdmin(rangerService, userName) || hasAdminAccess(serviceName, zoneName, userName, userGroups, resource, accessTypes);
                    }

                    if (isAllowed) {
                        RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, zoneName, userName);

                        if (policy != null) {
                            boolean policyUpdated = ServiceRESTUtil.processRevokeRequest(policy, revokeRequest);

                            if (policyUpdated) {
                                policy.setZoneName(zoneName);

                                ensureAdminAccess(policy);

                                svcStore.updatePolicy(policy);
                            } else {
                                LOG.error("processSecureRevokeRequest processing failed");

                                throw new Exception("processSecureRevokeRequest processing failed");
                            }
                        }
                    } else {
                        LOG.error("secureRevokeAccess({}, {}) failed as User doesn't have permission to revoke Policy", serviceName, revokeRequest);

                        throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to revoke access");
                    }
                } catch (WebApplicationException excp) {
                    throw excp;
                } catch (Throwable excp) {
                    LOG.error("secureRevokeAccess({}, {}) failed", serviceName, revokeRequest, excp);

                    throw restErrorUtil.createRESTException(excp.getMessage());
                } finally {
                    RangerPerfTracer.log(perf);
                }

                ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
            }
        }

        LOG.debug("<== ServiceREST.secureRevokeAccess({}, {}) :{}", serviceName, revokeRequest, ret);

        return ret;
    }

    @POST
    @Path("/policies")
    @Consumes("application/json")
    @Produces("application/json")
    public RangerPolicy createPolicy(RangerPolicy policy, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.createPolicy({})", policy);

        RangerPolicy     ret  = null;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createPolicy(policyName=" + policy.getName() + ")");
            }

            if (request != null) {
                boolean deleteIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_DELETE_IF_EXISTS)));

                if (deleteIfExists) {
                    boolean importInProgress = "true".equalsIgnoreCase(StringUtils.trimToEmpty(String.valueOf(request.getAttribute(PARAM_IMPORT_IN_PROGRESS))));

                    if (!importInProgress) {
                        List<RangerPolicy> policies = new ArrayList<RangerPolicy>() {
                            {
                                add(policy);
                            }
                        };

                        deleteExactMatchPolicyForResource(policies, request.getRemoteUser(), null);
                    }
                }

                boolean updateIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_UPDATE_IF_EXISTS)));
                boolean mergeIfExists  = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_MERGE_IF_EXISTS)));

                // Default POLICY_MATCHING_ALGO_BY_RESOURCE
                String policyMatchingAlgo = POLICY_MATCHING_ALGO_BY_POLICYNAME.equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_POLICY_MATCHING_ALGORITHM))) ? POLICY_MATCHING_ALGO_BY_POLICYNAME : POLICY_MATCHING_ALGO_BY_RESOURCE;

                LOG.debug(" policyMatchingAlgo: {} updateIfExists: {} mergeIfExists: {} deleteIfExists: {}", policyMatchingAlgo, updateIfExists, mergeIfExists, deleteIfExists);

                if (mergeIfExists && updateIfExists) {
                    LOG.warn("Cannot use both updateIfExists and mergeIfExists for a createPolicy. mergeIfExists will override updateIfExists for policy :[{}]", policy.getName());
                }

                if (!mergeIfExists && !updateIfExists) {
                    ret = createPolicyUnconditionally(policy);
                } else if (mergeIfExists) {
                    ret = applyPolicy(policy, request);
                } else if (policyMatchingAlgo.equalsIgnoreCase(POLICY_MATCHING_ALGO_BY_RESOURCE)) {
                    ret = applyPolicy(policy, request);
                } else if (policyMatchingAlgo.equalsIgnoreCase(POLICY_MATCHING_ALGO_BY_POLICYNAME)) {
                    RangerPolicy existingPolicy = getPolicyMatchByName(policy, request);

                    if (existingPolicy != null) {
                        policy.setId(existingPolicy.getId());

                        ret = updatePolicy(policy, null);
                    } else {
                        ret = createPolicyUnconditionally(policy);
                    }
                }

                LOG.debug("<== ServiceREST.createPolicy({}): {}", policy, ret);

                return ret;
            }

            if (ret == null) {
                ret = createPolicyUnconditionally(policy);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createPolicy({}) failed", policy, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.createPolicy({}): {}", policy, ret);

        return ret;
    }

    /*
    The verb for applyPolicy is POST as it could be partial update or a create
    */

    @POST
    @Path("/policies/apply")
    @Consumes("application/json")
    @Produces("application/json")
    public RangerPolicy applyPolicy(RangerPolicy policy, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.applyPolicy({})", policy);

        RangerPolicy ret;

        if (policy != null && StringUtils.isNotBlank(policy.getService())) {
            try {
                final RangerPolicy existingPolicy;
                String             signature                     = (new RangerPolicyResourceSignature(policy)).getSignature();
                List<RangerPolicy> policiesWithMatchingSignature = svcStore.getPoliciesByResourceSignature(policy.getService(), signature, true);

                if (CollectionUtils.isNotEmpty(policiesWithMatchingSignature)) {
                    if (policiesWithMatchingSignature.size() == 1) {
                        existingPolicy = policiesWithMatchingSignature.get(0);
                    } else {
                        throw new Exception("Multiple policies with matching policy-signature are found. Cannot determine target for applying policy");
                    }
                } else {
                    existingPolicy = null;
                }

                if (existingPolicy == null) {
                    if (StringUtils.isNotEmpty(policy.getName())) {
                        String   policyName  = StringUtils.isNotBlank(policy.getName()) ? policy.getName() : null;
                        String   serviceName = StringUtils.isNotBlank(policy.getService()) ? policy.getService() : null;
                        String   zoneName    = StringUtils.isNotBlank(policy.getZoneName()) ? policy.getZoneName() : null;
                        XXPolicy dbPolicy    = daoManager.getXXPolicy().findPolicy(policyName, serviceName, zoneName);
                        //XXPolicy dbPolicy = daoManager.getXXPolicy().findPolicy(policy.getName(), policy.getService(), policy.getZoneName());

                        if (dbPolicy != null) {
                            policy.setName(policy.getName() + System.currentTimeMillis());
                        }
                    }

                    ret = createPolicy(policy, null);
                } else {
                    boolean mergeIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_MERGE_IF_EXISTS)));

                    if (!mergeIfExists) {
                        boolean updateIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_UPDATE_IF_EXISTS)));

                        if (updateIfExists) {
                            // Called with explicit intent of updating an existing policy
                            mergeIfExists = false;
                        } else {
                            // Invoked through REST API. Merge with existing policy unless 'mergeIfExists' is explicitly set to false in HttpServletRequest
                            mergeIfExists = !"false".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_MERGE_IF_EXISTS)));
                        }
                    }

                    if (mergeIfExists) {
                        if (!existingPolicy.getIsDenyAllElse() && policy.getIsDenyAllElse()) {
                            LOG.error("Attempt to change the isDenyAllElse flag from false to true! Not supported!!");

                            throw new Exception("Merging existing policy(isDenyAllElse=false) with another policy(isDenyAllElse=true) is not allowed!");
                        }

                        ServiceRESTUtil.processApplyPolicy(existingPolicy, policy);

                        policy = existingPolicy;
                    } else {
                        policy.setId(existingPolicy.getId());
                    }

                    ret = updatePolicy(policy, policy.getId());
                }
            } catch (WebApplicationException excp) {
                throw excp;
            } catch (Exception exception) {
                LOG.error("Failed to apply policy:", exception);

                throw restErrorUtil.createRESTException(exception.getMessage());
            }
        } else {
            throw restErrorUtil.createRESTException("Non-existing service specified:");
        }

        LOG.debug("<== ServiceREST.applyPolicy({}): {}", policy, ret);

        return ret;
    }

    @PUT
    @Path("/policies/{id}")
    @Consumes("application/json")
    @Produces("application/json")
    public RangerPolicy updatePolicy(RangerPolicy policy, @PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.updatePolicy({})", policy);

        // if policy.id and param 'id' are specified, policy.id should be same as the param 'id'
        // if policy.id is null, then set param 'id' into policy Object
        if (policy.getId() == null) {
            policy.setId(id);
        } else if (!policy.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "policyID mismatch", true);
        }

        RangerPolicy     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updatePolicy(policyId=" + policy.getId() + ")");
            }

            if (isPolicyNameLengthValidationEnabled) {
                if (policy.getName().length() > maxPolicyNameLength) {
                    throw restErrorUtil.createRESTException("Policy name should not be longer than " + maxPolicyNameLength + " characters", MessageEnums.INPUT_DATA_OUT_OF_BOUND, null, "policy name", policy.getName());
                }
            }

            RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);

            validator.validate(policy, Action.UPDATE, bizUtil.isAdmin() || isServiceAdmin(policy.getService()) || isZoneAdmin(policy.getZoneName()));

            ensureAdminAccess(policy);

            bizUtil.blockAuditorRoleUser();

            ret = svcStore.updatePolicy(policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("updatePolicy({}) failed", policy, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.updatePolicy({}): {}", policy, ret);

        return ret;
    }

    @DELETE
    @Path("/policies/{id}")
    public void deletePolicy(@PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.deletePolicy({})", id);

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deletePolicy(policyId=" + id + ")");
            }

            RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);

            validator.validate(id, Action.DELETE);

            RangerPolicy policy = svcStore.getPolicy(id);

            ensureAdminAccess(policy);

            bizUtil.blockAuditorRoleUser();

            svcStore.deletePolicy(policy);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deletePolicy({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.deletePolicy({})", id);
    }

    @GET
    @Path("/policies/{id}")
    @Produces("application/json")
    public RangerPolicy getPolicy(@PathParam("id") Long id) {
        LOG.debug("==> ServiceREST.getPolicy({})", id);

        RangerPolicy     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicy(policyId=" + id + ")");
            }

            ret = svcStore.getPolicy(id);

            if (ret != null) {
                ensureAdminAndAuditAccess(ret);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPolicy({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getPolicy({}): {}", id, ret);

        return ret;
    }

    @GET
    @Path("/policyLabels")
    @Produces("application/json")
    public List<String> getPolicyLabels(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPolicyLabels()");

        List<String>     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicyLabels()");
            }

            SearchFilter filter = searchUtil.getSearchFilter(request, policyLabelsService.sortFields);

            ret = svcStore.getPolicyLabels(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPolicyLabels() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getPolicyLabels()");

        return ret;
    }

    @GET
    @Path("/policies")
    @Produces("application/json")
    public RangerPolicyList getPolicies(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPolicies()");

        RangerPolicyList ret;
        RangerPerfTracer perf   = null;
        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
            }

            // get all policies from the store; pick the page to return after applying filter
            final int savedStartIndex = filter.getStartIndex();
            final int savedMaxRows    = filter.getMaxRows();

            filter.setStartIndex(0);
            filter.setMaxRows(Integer.MAX_VALUE);

            List<RangerPolicy> policies = svcStore.getPolicies(filter);

            filter.setStartIndex(savedStartIndex);
            filter.setMaxRows(savedMaxRows);

            policies = applyAdminAccessFilter(policies);
            ret      = toRangerPolicyList(policies, filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPolicies() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getPolicies(): count={}", (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    /**
     * Resets/ removes service policy cache for given service.
     *
     * @param serviceName non-empty serviceName
     * @return {@code true} if successfully reseted/ removed for given service, {@code false} otherwise.
     */
    @GET
    @Path("/policies/cache/reset")
    @Produces("application/json")
    public boolean resetPolicyCache(@QueryParam("serviceName") String serviceName) {
        LOG.debug("==> ServiceREST.resetPolicyCache({})", serviceName);

        if (StringUtils.isEmpty(serviceName)) {
            throw restErrorUtil.createRESTException("Required parameter [serviceName] is missing.", MessageEnums.INVALID_INPUT_DATA);
        }

        RangerService rangerService = null;

        try {
            rangerService = svcStore.getServiceByName(serviceName);
        } catch (Exception e) {
            LOG.error(" {} No Service Found for ServiceName:{}", HttpServletResponse.SC_BAD_REQUEST, serviceName);
        }

        if (rangerService == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Invalid service name", true);
        }

        // check for ADMIN access
        if (!bizUtil.isAdmin()) {
            boolean isServiceAdmin = false;
            String  loggedInUser   = bizUtil.getCurrentUserLoginId();

            try {
                isServiceAdmin = bizUtil.isUserServiceAdmin(rangerService, loggedInUser);
            } catch (Exception e) {
                LOG.warn("Failed to find if user [{}] has service admin privileges on service [{}]", loggedInUser, serviceName, e);
            }

            if (!isServiceAdmin) {
                throw restErrorUtil.createRESTException("User cannot reset policy cache", MessageEnums.OPER_NO_PERMISSION);
            }
        }

        boolean ret = svcStore.resetPolicyCache(serviceName);

        LOG.debug("<== ServiceREST.resetPolicyCache(): ret={}", ret);

        return ret;
    }

    /**
     * Resets/ removes service policy cache for all.
     *
     * @return {@code true} if successfully reseted/ removed, {@code false} otherwise.
     */
    @GET
    @Path("/policies/cache/reset-all")
    @Produces("application/json")
    public boolean resetPolicyCacheAll() {
        LOG.debug("==> ServiceREST.resetPolicyCacheAll()");

        // check for ADMIN access
        if (!bizUtil.isAdmin()) {
            throw restErrorUtil.createRESTException("User cannot reset policy cache", MessageEnums.OPER_NO_PERMISSION);
        }

        boolean ret = svcStore.resetPolicyCache(null);

        LOG.debug("<== ServiceREST.resetPolicyCacheAll(): ret={}", ret);

        return ret;
    }

    @Deprecated
    @GET
    @Path("/policies/downloadExcel")
    @Produces("application/ms-excel")
    public void getPoliciesInExcel(@Context HttpServletRequest request, @Context HttpServletResponse response) {
        LOG.debug("==> ServiceREST.getPoliciesInExcel()");

        RangerPerfTracer perf   = null;
        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPoliciesInExcel()");
            }

            List<RangerPolicy> policyLists = new ArrayList<>();

            policyLists = getAllFilteredPolicyList(filter, request, policyLists);

            if (CollectionUtils.isNotEmpty(policyLists)) {
                Map<String, String> mapServiceTypeAndImplClass = new HashMap<>();

                for (RangerPolicy rangerPolicy : policyLists) {
                    if (rangerPolicy != null) {
                        ensureAdminAndAuditAccess(rangerPolicy, mapServiceTypeAndImplClass);
                    }
                }

                svcStore.getPoliciesInExcel(policyLists, response);
            } else {
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);

                LOG.error("No policies found to download!");
            }

            RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();

            rangerExportPolicyList.setMetaDataInfo(svcStore.getMetaDataInfo());

            String metaDataInfo = JsonUtilsV2.mapToJson(rangerExportPolicyList.getMetaDataInfo());

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "EXPORT EXCEL"), "Export Excel", metaDataInfo, null);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error while downloading policy report", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    @Deprecated
    @GET
    @Path("/policies/csv")
    @Produces("text/csv")
    public void getPoliciesInCsv(@Context HttpServletRequest request, @Context HttpServletResponse response) throws IOException {
        LOG.debug("==> ServiceREST.getPoliciesInCsv()");

        RangerPerfTracer perf   = null;
        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPoliciesInCsv()");
            }

            List<RangerPolicy> policyLists = new ArrayList<>();

            policyLists = getAllFilteredPolicyList(filter, request, policyLists);

            if (CollectionUtils.isNotEmpty(policyLists)) {
                Map<String, String> mapServiceTypeAndImplClass = new HashMap<>();

                for (RangerPolicy rangerPolicy : policyLists) {
                    if (rangerPolicy != null) {
                        ensureAdminAndAuditAccess(rangerPolicy, mapServiceTypeAndImplClass);
                    }
                }

                svcStore.getPoliciesInCSV(policyLists, response);
            } else {
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);

                LOG.error("No policies found to download!");
            }

            RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();

            rangerExportPolicyList.setMetaDataInfo(svcStore.getMetaDataInfo());

            String metaDataInfo = JsonUtilsV2.mapToJson(rangerExportPolicyList.getMetaDataInfo());

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "EXPORT CSV"), "Export CSV", metaDataInfo, null);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error while downloading policy report", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    @GET
    @Path("/policies/exportJson")
    @Produces("text/json")
    public void getPoliciesInJson(@Context HttpServletRequest request, @Context HttpServletResponse response, @QueryParam("checkPoliciesExists") Boolean checkPoliciesExists) {
        LOG.debug("==> ServiceREST.getPoliciesInJson()");

        RangerPerfTracer perf   = null;
        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);

        requestParamsValidation(filter);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPoliciesInJson()");
            }

            if (checkPoliciesExists == null) {
                checkPoliciesExists = false;
            }

            List<RangerPolicy> policyLists = new ArrayList<>();

            policyLists = getAllFilteredPolicyList(filter, request, policyLists);

            if (CollectionUtils.isNotEmpty(policyLists)) {
                Map<String, String> mapServiceTypeAndImplClass = new HashMap<>();

                for (RangerPolicy rangerPolicy : policyLists) {
                    if (rangerPolicy != null) {
                        ensureAdminAndAuditAccess(rangerPolicy, mapServiceTypeAndImplClass);
                    }
                }

                bizUtil.blockAuditorRoleUser();

                svcStore.getObjectInJson(policyLists, response, JSON_FILE_NAME_TYPE.POLICY);
            } else {
                checkPoliciesExists = true;

                response.setStatus(HttpServletResponse.SC_NO_CONTENT);

                LOG.error("There is no Policy to Export!!");
            }

            if (!checkPoliciesExists) {
                RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();

                rangerExportPolicyList.setMetaDataInfo(svcStore.getMetaDataInfo());

                String metaDataInfo = JsonUtilsV2.mapToJson(rangerExportPolicyList.getMetaDataInfo());

                policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "EXPORT JSON"), "Export Json", metaDataInfo, null);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error while exporting policy file!!", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    @POST
    @Path("/policies/importPoliciesFromFile")
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAdminOrKeyAdminRole()")
    public void importPoliciesFromFile(@Context HttpServletRequest request, @FormDataParam("servicesMapJson") InputStream serviceMapStream, @FormDataParam("zoneMapJson") InputStream zoneMapStream, @FormDataParam("file") InputStream uploadedInputStream, @FormDataParam("file") FormDataContentDisposition fileDetail, @QueryParam("isOverride") Boolean isOverride, @QueryParam("importType") String importType) {
        LOG.debug("==> ServiceREST.importPoliciesFromFile()");

        RangerContextHolder.getOrCreateOpContext().setBulkModeContext(true);

        RangerPerfTracer perf         = null;
        String           metaDataInfo = null;

        request.setAttribute(PARAM_IMPORT_IN_PROGRESS, true);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.importPoliciesFromFile()");
            }

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "IMPORT START"), "Import", "IMPORT START", null);

            if (isOverride == null) {
                isOverride = false;
            }

            List<String> serviceNameList = new ArrayList<>();

            getServiceNameList(request, serviceNameList);

            Map<String, String> servicesMappingMap  = new LinkedHashMap<>();
            List<String>        sourceServices      = new ArrayList<>();
            List<String>        destinationServices = new ArrayList<>();
            Map<String, String> zoneMappingMap      = new LinkedHashMap<>();
            List<String>        sourceZones         = new ArrayList<>();
            List<String>        destinationZones    = new ArrayList<>();

            if (zoneMapStream != null) {
                zoneMappingMap = svcStore.getMapFromInputStream(zoneMapStream);

                processZoneMapping(zoneMappingMap, sourceZones, destinationZones);
            }

            if (serviceMapStream != null) {
                servicesMappingMap = svcStore.getMapFromInputStream(serviceMapStream);

                processServiceMapping(servicesMappingMap, sourceServices, destinationServices);
            }

            String                    fileName               = fileDetail.getFileName();
            int                       totalPolicyCreate      = 0;
            String                    zoneNameInJson         = null;
            Map<String, RangerPolicy> policiesMap            = new LinkedHashMap<>();
            List<String>              dataFileSourceServices = new ArrayList<>();

            if (fileName.endsWith("json")) {
                try {
                    RangerExportPolicyList rangerExportPolicyList = processPolicyInputJsonForMetaData(uploadedInputStream, null);

                    if (rangerExportPolicyList != null && !CollectionUtils.sizeIsEmpty(rangerExportPolicyList.getMetaDataInfo())) {
                        metaDataInfo = JsonUtilsV2.mapToJson(rangerExportPolicyList.getMetaDataInfo());
                    } else {
                        LOG.info("metadata info is not provided!!");
                    }

                    List<RangerPolicy> policies = getPoliciesFromProvidedJson(rangerExportPolicyList);

                    int i = 0;
                    if (CollectionUtils.sizeIsEmpty(servicesMappingMap) && isOverride) {
                        if (policies != null && !CollectionUtils.sizeIsEmpty(policies)) {
                            for (RangerPolicy policyInJson : policies) {
                                if (policyInJson != null) {
                                    if (i == 0 && StringUtils.isNotBlank(policyInJson.getZoneName())) {
                                        zoneNameInJson = policyInJson.getZoneName().trim();
                                    }

                                    if (StringUtils.isNotEmpty(policyInJson.getService().trim())) {
                                        String serviceName = policyInJson.getService().trim();

                                        if (CollectionUtils.isNotEmpty(serviceNameList) && serviceNameList.contains(serviceName) && !sourceServices.contains(serviceName) && !destinationServices.contains(serviceName)) {
                                            sourceServices.add(serviceName);
                                            destinationServices.add(serviceName);
                                        } else if (CollectionUtils.isEmpty(serviceNameList) && !sourceServices.contains(serviceName) && !destinationServices.contains(serviceName)) {
                                            sourceServices.add(serviceName);
                                            destinationServices.add(serviceName);
                                        }
                                    } else {
                                        LOG.error("Service Name or Policy Name is not provided!!");

                                        throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
                                    }
                                }

                                i++;
                            }
                        }
                    } else if (!CollectionUtils.sizeIsEmpty(servicesMappingMap)) {
                        if (policies != null && !CollectionUtils.sizeIsEmpty(policies)) {
                            i = 0;

                            for (RangerPolicy policyInJson : policies) {
                                if (policyInJson != null) {
                                    if (i == 0 && StringUtils.isNotBlank(policyInJson.getZoneName())) {
                                        zoneNameInJson = policyInJson.getZoneName().trim();
                                    }

                                    if (StringUtils.isNotEmpty(policyInJson.getService().trim())) {
                                        dataFileSourceServices.add(policyInJson.getService().trim());
                                    } else {
                                        LOG.error("Service Name or Policy Name is not provided!!");

                                        throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
                                    }

                                    i++;
                                }
                            }

                            if (!dataFileSourceServices.containsAll(sourceServices)) {
                                LOG.error("Json File does not contain specified source service name.");

                                throw restErrorUtil.createRESTException("Json File does not contain specified source service name.");
                            }
                        }
                    }

                    boolean deleteIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_DELETE_IF_EXISTS)));
                    boolean updateIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_UPDATE_IF_EXISTS)));
                    String  polResource    = request.getParameter(SearchFilter.POL_RESOURCE);

                    if (updateIfExists) {
                        isOverride = false;
                    }

                    String destinationZoneName = getDestinationZoneName(destinationZones, zoneNameInJson);

                    if (isOverride && !updateIfExists && StringUtils.isEmpty(polResource)) {
                        LOG.debug("Deleting Policy from provided services in servicesMapJson file...");

                        if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)) {
                            deletePoliciesProvidedInServiceMap(sourceServices, destinationServices, destinationZoneName); //In order to delete Zone specific policies from service
                        }
                    } else if (updateIfExists && StringUtils.isNotEmpty(polResource)) {
                        LOG.debug("Deleting Policy from provided services in servicesMapJson file for specific resource...");

                        if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)) {
                            deletePoliciesForResource(sourceServices, destinationServices, request, policies, destinationZoneName); //In order to delete Zone specific policies from service
                        }
                    }

                    if (policies != null && !CollectionUtils.sizeIsEmpty(policies)) {
                        for (RangerPolicy policyInJson : policies) {
                            if (policyInJson != null) {
                                if (StringUtils.isNotBlank(destinationZoneName)) {
                                    boolean isZoneServiceExistAtDestination = validateDestZoneServiceMapping(destinationZoneName, policyInJson, servicesMappingMap);

                                    if (!isZoneServiceExistAtDestination) {
                                        LOG.warn("provided service of policy in File is not associated with zone");

                                        continue;
                                    }
                                }

                                policiesMap = svcStore.createPolicyMap(zoneMappingMap, sourceZones, destinationZoneName, servicesMappingMap, sourceServices, destinationServices, policyInJson, policiesMap); // zone Info is also sent for creating policy map
                            }
                        }

                        if (deleteIfExists) {
                            //deleting target policies if already exist
                            deleteExactMatchPolicyForResource(policies, request.getRemoteUser(), destinationZoneName);
                        }
                    }

                    totalPolicyCreate = createPolicesBasedOnPolicyMap(request, policiesMap, serviceNameList, updateIfExists, totalPolicyCreate);

                    if (!(totalPolicyCreate > 0)) {
                        LOG.error("zero policy is created from provided data file!!");

                        throw restErrorUtil.createRESTException("zero policy is created from provided data file!!");
                    }
                } catch (IOException e) {
                    LOG.error(e.getMessage());

                    throw restErrorUtil.createRESTException(e.getMessage());
                }
            } else {
                LOG.error("Provided file format is not supported!!");

                throw restErrorUtil.createRESTException("Provided file format is not supported!!");
            }
        } catch (JsonSyntaxException ex) {
            LOG.error("Provided json file is not valid!!", ex);

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "IMPORT ERROR"), "Import failed", StringUtils.isNotEmpty(metaDataInfo) ? metaDataInfo : null, null);

            throw restErrorUtil.createRESTException(ex.getMessage());
        } catch (WebApplicationException excp) {
            LOG.error("Error while importing policy from file!!", excp);

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "IMPORT ERROR"), "Import failed", StringUtils.isNotEmpty(metaDataInfo) ? metaDataInfo : null, null);

            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error while importing policy from file!!", excp);

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "IMPORT ERROR"), "Import failed", StringUtils.isNotEmpty(metaDataInfo) ? metaDataInfo : null, null);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);

            policyService.createTransactionLog(new XXTrxLogV2(AppConstants.CLASS_TYPE_RANGER_POLICY, null, null, "IMPORT END"), "IMPORT END", StringUtils.isNotEmpty(metaDataInfo) ? metaDataInfo : null, null);

            LOG.debug("<== ServiceREST.importPoliciesFromFile()");
        }
    }

    public List<RangerPolicy> getPolicies(SearchFilter filter) {
        LOG.debug("==> ServiceREST.getPolicies(filter)");

        List<RangerPolicy> ret;
        RangerPerfTracer   perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
            }

            ret = svcStore.getPolicies(filter);
            ret = applyAdminAccessFilter(ret);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPolicies() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getPolicies(filter): count={}", (ret == null ? 0 : ret.size()));

        return ret;
    }

    @GET
    @Path("/policies/count")
    @Produces("application/json")
    public Long countPolicies(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.countPolicies():");

        Long             ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countPolicies()");
            }

            List<RangerPolicy> policies = getPolicies(request).getPolicies();

            policies = applyAdminAccessFilter(policies);

            ret = policies == null ? 0L : policies.size();
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("countPolicies() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.countPolicies(): {}", ret);

        return ret;
    }

    @GET
    @Path("/policies/service/{id}")
    @Produces("application/json")
    public RangerPolicyList getServicePolicies(@PathParam("id") Long serviceId, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getServicePolicies({})", serviceId);

        RangerPolicyList ret;
        RangerPerfTracer perf   = null;
        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceId=" + serviceId + ")");
            }

            String policyTypeStr = filter.getParam(SearchFilter.POLICY_TYPE);

            if (policyTypeStr != null) {
                int policyType = Integer.parseInt(policyTypeStr);

                if (IntStream.of(RangerPolicy.POLICY_TYPES).noneMatch(x -> x == policyType)) {
                    throw restErrorUtil.createRESTException("policyTypes with id: " + policyTypeStr + " does not exist", MessageEnums.DATA_NOT_FOUND, Long.parseLong(policyTypeStr), null, "readResource : No Object found with given id.");
                }
            }

            // get all policies from the store; pick the page to return after applying filter
            int savedStartIndex = filter.getStartIndex();
            int savedMaxRows    = filter.getMaxRows();

            filter.setStartIndex(0);
            filter.setMaxRows(Integer.MAX_VALUE);

            List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceId, filter);

            filter.setStartIndex(savedStartIndex);
            filter.setMaxRows(savedMaxRows);

            servicePolicies = applyAdminAccessFilter(servicePolicies);

            ret = toRangerPolicyList(servicePolicies, filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServicePolicies({}) failed", serviceId, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getServicePolicies({}): count={}", serviceId, (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    @GET
    @Path("/policies/service/name/{name}")
    @Produces("application/json")
    public RangerPolicyList getServicePoliciesByName(@PathParam("name") String serviceName, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getServicePolicies({})", serviceName);

        SearchFilter     filter = searchUtil.getSearchFilter(request, policyService.sortFields);
        RangerPolicyList ret    = getServicePolicies(serviceName, filter);

        LOG.debug("<== ServiceREST.getServicePolicies({}): count={}", serviceName, (ret == null ? 0 : ret.getListSize()));

        return ret;
    }

    @GET
    @Path("/policies/download/{serviceName}")
    @Produces("application/json")
    public ServicePolicies getServicePoliciesIfUpdated(@PathParam("serviceName") String serviceName, @DefaultValue("-1") @QueryParam("lastKnownVersion") Long lastKnownVersion, @DefaultValue("0") @QueryParam("lastActivationTime") Long lastActivationTime, @QueryParam("pluginId") String pluginId, @DefaultValue("") @QueryParam("clusterName") String clusterName, @DefaultValue("") @QueryParam("zoneName") String zoneName, @DefaultValue("false") @QueryParam("supportsPolicyDeltas") Boolean supportsPolicyDeltas, @DefaultValue("") @QueryParam("pluginCapabilities") String pluginCapabilities, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.getServicePoliciesIfUpdated({}, {}, {}, {}, {}, {})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, supportsPolicyDeltas);

        ServicePolicies  ret               = null;
        int              httpCode          = HttpServletResponse.SC_OK;
        String           logMsg            = null;
        RangerPerfTracer perf              = null;
        Long             downloadedVersion = null;
        boolean          isValid           = false;

        try {
            bizUtil.failUnauthenticatedDownloadIfNotAllowed();

            isValid = serviceUtil.isValidateHttpsAuthentication(serviceName, request);
        } catch (WebApplicationException webException) {
            httpCode = webException.getResponse().getStatus();
            logMsg   = webException.getResponse().getEntity().toString();
        } catch (Exception e) {
            httpCode = HttpServletResponse.SC_BAD_REQUEST;
            logMsg   = e.getMessage();
        }

        if (isValid) {
            try {
                if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                    perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePoliciesIfUpdated(serviceName=" + serviceName + ",lastKnownVersion=" + lastKnownVersion + ",lastActivationTime=" + lastActivationTime + ")");
                }

                ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, !supportsPolicyDeltas);

                if (ret == null) {
                    downloadedVersion = lastKnownVersion;
                    httpCode          = HttpServletResponse.SC_NOT_MODIFIED;
                    logMsg            = "No change since last update";
                } else {
                    downloadedVersion = ret.getPolicyVersion();
                    logMsg            = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : (ret.getPolicyDeltas() != null ? ret.getPolicyDeltas().size() : 0)) + " policies. Policy version=" + ret.getPolicyVersion();
                }
            } catch (Throwable excp) {
                LOG.error("getServicePoliciesIfUpdated({}, {}, {}) failed", serviceName, lastKnownVersion, lastActivationTime, excp);

                httpCode = HttpServletResponse.SC_BAD_REQUEST;
                logMsg   = excp.getMessage();
            } finally {
                createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, httpCode, clusterName, zoneName, request);
                RangerPerfTracer.log(perf);
            }
        }
        assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_POLICIES, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode, clusterName, pluginCapabilities);

        if (httpCode != HttpServletResponse.SC_OK) {
            boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;

            throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
        }

        LOG.debug("<== ServiceREST.getServicePoliciesIfUpdated({}, {}, {}, {}, {}, {}) : count={}", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, supportsPolicyDeltas, ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));

        return ret;
    }

    @GET
    @Path("/secure/policies/download/{serviceName}")
    @Produces("application/json")
    public ServicePolicies getSecureServicePoliciesIfUpdated(@PathParam("serviceName") String serviceName, @DefaultValue("-1") @QueryParam("lastKnownVersion") Long lastKnownVersion, @DefaultValue("0") @QueryParam("lastActivationTime") Long lastActivationTime, @QueryParam("pluginId") String pluginId, @DefaultValue("") @QueryParam("clusterName") String clusterName, @DefaultValue("") @QueryParam("zoneName") String zoneName, @DefaultValue("false") @QueryParam("supportsPolicyDeltas") Boolean supportsPolicyDeltas, @DefaultValue("") @QueryParam("pluginCapabilities") String pluginCapabilities, @Context HttpServletRequest request) throws Exception {
        LOG.debug("==> ServiceREST.getSecureServicePoliciesIfUpdated({}, {}, {}, {}, {}, {})", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, supportsPolicyDeltas);

        ServicePolicies  ret               = null;
        int              httpCode          = HttpServletResponse.SC_OK;
        String           logMsg            = null;
        RangerPerfTracer perf              = null;
        boolean          isAllowed         = false;
        boolean          isAdmin           = bizUtil.isAdmin();
        boolean          isKeyAdmin        = bizUtil.isKeyAdmin();
        Long             downloadedVersion = null;
        boolean          isValid           = false;

        request.setAttribute("downloadPolicy", "secure");

        try {
            isValid = serviceUtil.isValidService(serviceName, request);
        } catch (WebApplicationException webException) {
            httpCode = webException.getResponse().getStatus();
            logMsg   = webException.getResponse().getEntity().toString();
        } catch (Exception e) {
            httpCode = HttpServletResponse.SC_BAD_REQUEST;
            logMsg   = e.getMessage();
        }

        if (isValid) {
            try {
                if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                    perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getSecureServicePoliciesIfUpdated(serviceName=" + serviceName + ",lastKnownVersion=" + lastKnownVersion + ",lastActivationTime=" + lastActivationTime + ")");
                }

                XXService     xService      = daoManager.getXXService().findByName(serviceName);
                XXServiceDef  xServiceDef   = daoManager.getXXServiceDef().getById(xService.getType());
                RangerService rangerService;

                if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
                    rangerService = svcStore.getServiceByNameForDP(serviceName);

                    if (isKeyAdmin) {
                        isAllowed = true;
                    } else {
                        if (rangerService != null) {
                            isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Download);

                            if (!isAllowed) {
                                isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Grant_Revoke);
                            }
                        }
                    }
                } else {
                    rangerService = svcStore.getServiceByName(serviceName);

                    if (isAdmin) {
                        isAllowed = true;
                    } else {
                        if (rangerService != null) {
                            isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Download);

                            if (!isAllowed) {
                                isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Grant_Revoke);
                            }
                        }
                    }
                }

                if (isAllowed) {
                    ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion, !supportsPolicyDeltas);

                    if (ret == null) {
                        downloadedVersion = lastKnownVersion;
                        httpCode          = HttpServletResponse.SC_NOT_MODIFIED;
                        logMsg            = "No change since last update";
                    } else {
                        downloadedVersion = ret.getPolicyVersion();
                        logMsg            = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : (ret.getPolicyDeltas() != null ? ret.getPolicyDeltas().size() : 0)) + " policies. Policy version=" + ret.getPolicyVersion();
                    }
                } else {
                    LOG.error("getSecureServicePoliciesIfUpdated({}, {}) failed as User doesn't have permission to download Policy", serviceName, lastKnownVersion);

                    httpCode = HttpServletResponse.SC_FORBIDDEN; // assert user is authenticated.
                    logMsg   = "User doesn't have permission to download policy";
                }
            } catch (Throwable excp) {
                LOG.error("getSecureServicePoliciesIfUpdated({}, {}, {}) failed", serviceName, lastKnownVersion, lastActivationTime, excp);

                httpCode = HttpServletResponse.SC_BAD_REQUEST;
                logMsg   = excp.getMessage();
            } finally {
                createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, httpCode, clusterName, zoneName, request);
                RangerPerfTracer.log(perf);
            }
        }

        assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_POLICIES, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode, clusterName, pluginCapabilities);

        if (httpCode != HttpServletResponse.SC_OK) {
            boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;

            throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
        }

        LOG.debug("<== ServiceREST.getSecureServicePoliciesIfUpdated({}, {}, {}, {}, {}, {}) : count={}", serviceName, lastKnownVersion, lastActivationTime, pluginId, clusterName, supportsPolicyDeltas, ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));

        return ret;
    }

    @DELETE
    @Path("/server/policydeltas")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deletePolicyDeltas(@DefaultValue("7") @QueryParam("days") Integer olderThan, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.deletePolicyDeltas({})", olderThan);

        svcStore.resetPolicyUpdateLog(olderThan, RangerPolicyDelta.CHANGE_TYPE_INVALIDATE_POLICY_DELTAS);

        LOG.debug("<== ServiceREST.deletePolicyDeltas({})", olderThan);
    }

    @DELETE
    @Path("/server/purgepolicies/{serviceName}")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void purgeEmptyPolicies(@PathParam("serviceName") String serviceName, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.purgeEmptyPolicies({})", serviceName);

        if (serviceName == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Invalid service name", true);
        }

        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.purgeEmptyPolicies(serviceName=" + serviceName + ")");
            }

            if (svcStore.getServiceByName(serviceName) == null) {
                throw new Exception("service does not exist - name=" + serviceName);
            }

            ServicePolicies servicePolicies = svcStore.getServicePolicies(serviceName, -1L);

            if (servicePolicies != null && CollectionUtils.isNotEmpty(servicePolicies.getPolicies())) {
                for (RangerPolicy policy : servicePolicies.getPolicies()) {
                    if (CollectionUtils.isEmpty(PolicyRefUpdater.getAllPolicyItems(policy))) {
                        deletePolicy(policy.getId());
                    }
                }
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("purgeEmptyPolicies({}) failed", serviceName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.purgeEmptyPolicies({})", serviceName);
    }

    @GET
    @Path("/policies/eventTime")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FROM_EVENT_TIME + "\")")
    public RangerPolicy getPolicyFromEventTime(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPolicyFromEventTime()");

        String eventTimeStr = request.getParameter("eventTime");
        String policyIdStr  = request.getParameter("policyId");
        String versionNoStr = request.getParameter("versionNo");

        if (StringUtils.isEmpty(eventTimeStr) || StringUtils.isEmpty(policyIdStr)) {
            throw restErrorUtil.createRESTException("EventTime or policyId cannot be null or empty string.", MessageEnums.INVALID_INPUT_DATA);
        }

        Long         policyId = Long.parseLong(policyIdStr);
        RangerPolicy policy   = null;

        if (!StringUtil.isEmpty(versionNoStr)) {
            int policyVersion = Integer.parseInt(versionNoStr);

            try {
                policy = svcStore.getPolicyForVersionNumber(policyId, policyVersion);

                if (policy != null) {
                    ensureAdminAndAuditAccess(policy);
                }
            } catch (WebApplicationException excp) {
                throw excp;
            } catch (Throwable excp) {
                // Ignore any other exception and go for fetching the policy by eventTime
            }
        }

        if (policy == null) {
            try {
                policy = svcStore.getPolicyFromEventTime(eventTimeStr, policyId);

                if (policy != null) {
                    ensureAdminAndAuditAccess(policy);
                }
            } catch (WebApplicationException excp) {
                throw excp;
            } catch (Throwable excp) {
                LOG.error("getPolicy({}) failed", policyId, excp);

                throw restErrorUtil.createRESTException(excp.getMessage());
            }
        }

        if (policy == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getPolicy({}): {}", policyId, policy);
        LOG.debug("<== ServiceREST.getPolicyFromEventTime()");

        return policy;
    }

    @GET
    @Path("/policy/{policyId}/versionList")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_VERSION_LIST + "\")")
    public VXString getPolicyVersionList(@PathParam("policyId") Long policyId) {
        return svcStore.getPolicyVersionList(policyId);
    }

    @GET
    @Path("/policy/{policyId}/version/{versionNo}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FOR_VERSION_NO + "\")")
    public RangerPolicy getPolicyForVersionNumber(@PathParam("policyId") Long policyId, @PathParam("versionNo") int versionNo) {
        RangerPolicy policy = svcStore.getPolicyForVersionNumber(policyId, versionNo);

        if (policy != null) {
            ensureAdminAndAuditAccess(policy);
        }

        return policy;
    }

    @GET
    @Path("/plugins/info")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_PLUGINS_INFO + "\")")
    public RangerPluginInfoList getPluginsInfo(@Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPluginsInfo()");

        RangerPluginInfoList ret    = null;
        SearchFilter         filter = searchUtil.getSearchFilter(request, pluginInfoService.getSortFields());

        try {
            PList<RangerPluginInfo> paginatedPluginsInfo = pluginInfoService.searchRangerPluginInfo(filter);
            if (paginatedPluginsInfo != null) {
                ret = new RangerPluginInfoList();

                ret.setPluginInfoList(paginatedPluginsInfo.getList());
                ret.setPageSize(paginatedPluginsInfo.getPageSize());
                ret.setResultSize(paginatedPluginsInfo.getResultSize());
                ret.setStartIndex(paginatedPluginsInfo.getStartIndex());
                ret.setTotalCount(paginatedPluginsInfo.getTotalCount());
                ret.setSortBy(paginatedPluginsInfo.getSortBy());
                ret.setSortType(paginatedPluginsInfo.getSortType());
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPluginsInfo() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        LOG.debug("<== ServiceREST.getPluginsInfo()");

        return ret;
    }

    public void blockIfGdsService(String serviceName) {
        String serviceType = daoManager.getXXServiceDef().findServiceDefTypeByServiceName(serviceName);

        if (EMBEDDED_SERVICEDEF_GDS_NAME.equals(serviceType)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, EMBEDDED_SERVICEDEF_GDS_NAME.toUpperCase() + " policies can't be managed via this API", true);
        }
    }

    public RangerPolicyAdmin getPolicyAdminForDelegatedAdmin(String serviceName) {
        return RangerPolicyAdminCacheForEngineOptions.getInstance().getServicePoliciesAdmin(serviceName, svcStore, zoneStore, roleDBStore, delegateAdminOptions);
    }

    public List<RangerPolicy> getPoliciesWithMetaAttributes(List<RangerPolicy> policies) {
        return svcStore.getPoliciesWithMetaAttributes(policies);
    }

    @GET
    @Path("/checksso")
    @Produces(MediaType.TEXT_PLAIN)
    public String checkSSO() {
        return String.valueOf(bizUtil.isSSOEnabled());
    }

    @GET
    @Path("/csrfconf")
    @Produces("application/json")
    public HashMap<String, Object> getCSRFProperties(@Context HttpServletRequest request) {
        return getCSRFPropertiesMap(request);
    }

    @GET
    @Path("/metrics/type/{type}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_METRICS_BY_TYPE + "\")")
    public String getMetricByType(@PathParam("type") String type) {
        LOG.debug("==> ServiceREST.getMetricByType(serviceDefName={})", type);

        // as of now we are allowing only users with Admin role to access this
        // API
        bizUtil.checkSystemAdminAccess();
        bizUtil.blockAuditorRoleUser();

        String ret;

        try {
            ServiceDBStore.METRIC_TYPE metricType = ServiceDBStore.METRIC_TYPE.getMetricTypeByName(type);

            if (metricType == null) {
                throw restErrorUtil.createRESTException("Metric type=" + type + ", not supported.");
            }

            ret = svcStore.getMetricByType(metricType);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getMetricByType({}) failed", type, excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getMetricByType({}): {}", type, ret);

        return ret;
    }

    /**
     * Delete services/ repos associated with cluster.
     * Only users with Ranger UserAdmin OR KeyAdmin are allowed to access this API.
     *
     * @param clusterName
     * @return List of {@link ServiceDeleteResponse serviceDeleteResponse}.
     */
    @DELETE
    @Path("/cluster-services/{clusterName}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_CLUSTER_SERVICES + "\")")
    public ResponseEntity<List<ServiceDeleteResponse>> deleteClusterServices(@PathParam("clusterName") String clusterName) {
        LOG.debug("==> ServiceREST.deleteClusterServices({})", clusterName);

        List<ServiceDeleteResponse> deletedServices = new ArrayList<>();
        HttpStatus                  responseStatus  = HttpStatus.OK;

        try {
            //check if user has ADMIN privileges
            bizUtil.hasAdminPermissions("Services");

            //get all service/ repo IDs to delete
            List<Long> serviceIdsToBeDeleted = daoManager.getXXServiceConfigMap().findServiceIdsByClusterName(clusterName);

            if (serviceIdsToBeDeleted.isEmpty()) {
                responseStatus = HttpStatus.NOT_FOUND;
            } else {
                //delete each service/ repo one by one
                for (Long serviceId : serviceIdsToBeDeleted) {
                    ServiceDeleteResponse deleteResponse = new ServiceDeleteResponse(serviceId);

                    try {
                        String serviceName = this.deleteServiceById(serviceId);

                        deleteResponse.setServiceName(serviceName);
                        deleteResponse.setIsDeleted(Boolean.TRUE);
                    } catch (Throwable e) {
                        //log and proceed
                        LOG.warn("Skipping deletion of service with ID={}", serviceId);

                        e.printStackTrace();
                        deleteResponse.setIsDeleted(Boolean.FALSE);
                        deleteResponse.setErrorMsg(e.getMessage());
                    }

                    deletedServices.add(deleteResponse);
                }
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Deleting services associated with cluster=({}) failed", clusterName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        LOG.debug("<== ServiceREST.deleteClusterServices() - deletedServices: {}", deletedServices);

        return new ResponseEntity<>(deletedServices, responseStatus);
    }

    @GET
    @Path("/policies/guid/{guid}")
    @Produces("application/json")
    public RangerPolicy getPolicyByGUIDAndServiceNameAndZoneName(@PathParam("guid") String guid, @DefaultValue("") @QueryParam("serviceName") String serviceName, @DefaultValue("") @QueryParam("zoneName") String zoneName) {
        LOG.debug("==> ServiceREST.getPolicyByGUIDAndServiceNameAndZoneName({}, {}, {})", guid, serviceName, zoneName);

        RangerPolicy     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicyByGUIDAndServiceNameAndZoneName(policyGUID=" + guid + ", serviceName=" + serviceName + ", zoneName=" + zoneName + ")");
            }

            ret = svcStore.getPolicy(guid, serviceName, zoneName);

            if (ret != null) {
                ensureAdminAndAuditAccess(ret);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getPolicyByGUIDAndServiceNameAndZoneName({}, {}, {}) failed", guid, serviceName, zoneName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        if (ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        LOG.debug("<== ServiceREST.getPolicyByGUIDAndServiceNameAndZoneName({}, {}, {}) : {}", guid, serviceName, zoneName, ret);

        return ret;
    }

    @DELETE
    @Path("/policies/guid/{guid}")
    public void deletePolicyByGUIDAndServiceNameAndZoneName(@PathParam("guid") String guid, @DefaultValue("") @QueryParam("serviceName") String serviceName, @DefaultValue("") @QueryParam("zoneName") String zoneName) {
        LOG.debug("==> ServiceREST.deletePolicyByGUIDAndServiceNameAndZoneName({}, {}, {})", guid, serviceName, zoneName);

        RangerPolicy     ret;
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deletePolicyByGUIDAndServiceNameAndZoneName(policyGUID=" + guid + ", serviceName=" + serviceName + ", zoneName=" + zoneName + ")");
            }

            ret = getPolicyByGUIDAndServiceNameAndZoneName(guid, serviceName, zoneName);

            if (ret != null) {
                deletePolicy(ret.getId());
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deletePolicyByGUIDAndServiceNameAndZoneName({}, {}, {}) failed", guid, serviceName, zoneName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.deletePolicyByGUIDAndServiceNameAndZoneName({}, {}, {}) : {}", guid, serviceName, zoneName, ret);
    }

    @DELETE
    @Path("/server/purge/records")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public List<RangerPurgeResult> purgeRecords(@QueryParam("type") String recordType, @DefaultValue("180") @QueryParam("retentionDays") Integer olderThan, @Context HttpServletRequest request) {
        LOG.debug("==> ServiceREST.purgeRecords({}, {})", recordType, olderThan);

        List<RangerPurgeResult> ret  = new ArrayList<>();
        RangerPerfTracer        perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.purgeRecords(recordType=" + recordType + ", olderThan=" + olderThan + ")");
            }

            if (olderThan < 1) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "Retention days can't be lesser than 1", true);
            }

            if (PURGE_RECORD_TYPE_LOGIN_LOGS.equalsIgnoreCase(recordType)) {
                svcStore.removeAuthSessions(olderThan, ret);
            } else if (PURGE_RECORD_TYPE_TRX_LOGS.equalsIgnoreCase(recordType)) {
                svcStore.removeTransactionLogs(olderThan, ret);
            } else if (PURGE_RECORD_TYPE_POLICY_EXPORT_LOGS.equalsIgnoreCase(recordType)) {
                svcStore.removePolicyExportLogs(olderThan, ret);
            } else {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, recordType + ": invalid record type. Valid values: [ " + PURGE_RECORD_TYPE_LOGIN_LOGS + ", " + PURGE_RECORD_TYPE_TRX_LOGS + ", " + PURGE_RECORD_TYPE_POLICY_EXPORT_LOGS + " ]", true);
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("purgeRecords({}, {}) failed", recordType, olderThan, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.purgeRecords({}, {}) : {}", recordType, olderThan, ret);

        return ret;
    }

    public List<Long> deleteBulkPolicies(String serviceName, HttpServletRequest request) {
        LOG.debug("==> ServiceREST.deleteBulkPolicies({})", serviceName);

        Set<RangerPolicy> policies = new HashSet<>(getBulkPolicies(serviceName, request));

        ensureAdminAccessForServicePolicies(serviceName, policies);

        List<Long> ret = new ArrayList<>();

        try {
            svcStore.deletePolicies(policies, serviceName, ret);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteBulkPolicies(): failed after deleting {} of {} policies", ret.size(), policies.size(), excp);

            throw restErrorUtil.createRESTException("Failed after deleting " + ret.size() + " of " + policies.size() + " policies. Error " + excp);
        }

        LOG.debug("<== ServiceREST.deleteBulkPolicies(): count={}", ret.size());

        return ret;
    }

    public RangerPolicyResource getPolicyResource(Object resourceName, GrantRevokeRequest grantRequest) {
        RangerPolicyResource ret;

        if (resourceName instanceof List) {
            List<String> resourceValues = (List<String>) resourceName;

            ret = new RangerPolicyResource(resourceValues, false, grantRequest.getIsRecursive());
        } else {
            ret = new RangerPolicyResource((String) resourceName);

            ret.setIsRecursive(grantRequest.getIsRecursive());
        }

        return ret;
    }

    /**
     * Returns {@link RangerPolicy} for non-empty serviceName, policyName and zoneName null otherwise.
     *
     * @param serviceName
     * @param policyName
     * @param zoneName
     * @return
     */
    public RangerPolicy getPolicyByName(String serviceName, String policyName, String zoneName) {
        LOG.debug("==> ServiceREST.getPolicyByName({}, {}, {})", serviceName, policyName, zoneName);

        RangerPolicy ret = null;

        if (StringUtils.isNotBlank(serviceName) && StringUtils.isNotBlank(policyName)) {
            XXPolicy dbPolicy = daoManager.getXXPolicy().findPolicy(policyName, serviceName, zoneName);

            if (dbPolicy != null) {
                ret = policyService.getPopulatedViewObject(dbPolicy);
            }

            if (ret != null) {
                ensureAdminAndAuditAccess(ret);
            }
        }

        LOG.debug("<== ServiceREST.getPolicyByName({}, {}, {}) : {}", serviceName, policyName, zoneName, (ret != null ? ret : "ret is null"));

        return ret;
    }

    void ensureAdminAccess(RangerPolicy policy) {
        blockIfGdsService(policy.getService());

        boolean isAdmin    = bizUtil.isAdmin();
        boolean isKeyAdmin = bizUtil.isKeyAdmin();
        String  userName   = bizUtil.getCurrentUserLoginId();
        boolean isSvcAdmin = isAdmin || svcStore.isServiceAdminUser(policy.getService(), userName);

        if (!isAdmin && !isKeyAdmin && !isSvcAdmin) {
            Set<String> userGroups = userMgr.getGroupsForUser(userName);
            boolean     isAllowed;

            //for zone policy create /update / delete
            if (!StringUtils.isEmpty(policy.getZoneName()) && serviceMgr.isZoneAdmin(policy.getZoneName())) {
                isAllowed = true;
            } else {
                isAllowed = hasAdminAccess(policy, userName, userGroups);
            }

            if (!isAllowed) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User '" + userName + "' does not have delegated-admin privilege on given resources", true);
            }
        } else {
            XXService    xService    = daoManager.getXXService().findByName(policy.getService());
            XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

            if (isAdmin) {
                if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
                    throw restErrorUtil.createRESTException("KMS Policies/Services/Service-Defs are not accessible for user '" + userName + "'.", MessageEnums.OPER_NO_PERMISSION);
                }
            } else if (isKeyAdmin) {
                if (!EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
                    throw restErrorUtil.createRESTException("Only KMS Policies/Services/Service-Defs are accessible for user '" + userName + "'.", MessageEnums.OPER_NO_PERMISSION);
                }
            }
        }
    }

    void ensureAdminAndAuditAccess(RangerPolicy policy) {
        ensureAdminAndAuditAccess(policy, new HashMap<>());
    }

    void ensureAdminAccessForPolicies(Set<RangerPolicy> policies, XXService xxService, String serviceName) {
        LOG.debug("==> ServiceREST.ensureAdminAccessForPolicies({})", serviceName);

        boolean isAdmin    = bizUtil.isAdmin();
        boolean isKeyAdmin = bizUtil.isKeyAdmin();
        String  userName   = bizUtil.getCurrentUserLoginId();

        XXServiceDef        xServiceDef = daoManager.getXXServiceDef().getById(xxService.getType());
        Set<String>         userGroups  = userMgr.getGroupsForUser(userName);
        RangerPolicyAdmin   policyAdmin = getPolicyAdminForDelegatedAdmin(serviceName);
        Set<String>         roles       = policyAdmin.getRolesFromUserAndGroups(userName, userGroups);
        Map<String, Object> evalContext = new HashMap<>();
        boolean             isKmsService = EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname());

        RangerAccessRequestUtil.setCurrentUserInContext(evalContext, userName);

        Map<String, Boolean> serviceToIsAdminUserMap = new HashMap<>();
        Map<String, Boolean> zoneToIsAdminMap        = new HashMap<>();

        policies.forEach(policy -> {
            boolean isServiceAdminUser = serviceToIsAdminUserMap.computeIfAbsent(policy.getService(), svcName -> svcStore.isServiceAdminUser(svcName, userName));
            boolean isZoneAdmin        = !StringUtils.isEmpty(policy.getZoneName()) && zoneToIsAdminMap.computeIfAbsent(policy.getZoneName(), serviceMgr::isZoneAdmin);
            boolean isSvcAdmin          = isAdmin || isServiceAdminUser || isZoneAdmin;

            if (!isAdmin && !isKeyAdmin && !isSvcAdmin) {
                boolean isAllowed = policyAdmin.isDelegatedAdminAccessAllowedForModify(policy, userName, userGroups, roles, evalContext);

                if (!isAllowed) {
                    throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User '" + userName + "' does not have delegated-admin privilege for policy id=" + policy.getId(), true);
                }
            } else {
                if ((isAdmin && isKmsService) || (isKeyAdmin && !isKmsService)) {
                    throw restErrorUtil.createRESTException(xServiceDef.getName() + " policies are not accessible for user '" + userName + "'.", MessageEnums.OPER_NO_PERMISSION);
                }
            }
        });

        LOG.debug("<== ServiceREST.ensureAdminAccessForPolicies({})", serviceName);
    }

    void ensureAdminAndAuditAccess(RangerPolicy policy, Map<String, String> mapServiceTypeAndImplClass) {
        boolean isAdmin         = bizUtil.isAdmin();
        boolean isKeyAdmin      = bizUtil.isKeyAdmin();
        String  userName        = bizUtil.getCurrentUserLoginId();
        boolean isAuditAdmin    = bizUtil.isAuditAdmin();
        boolean isAuditKeyAdmin = bizUtil.isAuditKeyAdmin();
        boolean isSvcAdmin      = isAdmin || svcStore.isServiceAdminUser(policy.getService(), userName) || (!StringUtils.isEmpty(policy.getZoneName()) && (serviceMgr.isZoneAdmin(policy.getZoneName()) || serviceMgr.isZoneAuditor(policy.getZoneName())));

        if (!isAdmin && !isKeyAdmin && !isSvcAdmin && !isAuditAdmin && !isAuditKeyAdmin) {
            boolean           isAllowed   = false;
            Set<String>       userGroups  = userMgr.getGroupsForUser(userName);
            RangerPolicyAdmin policyAdmin = getPolicyAdminForDelegatedAdmin(policy.getService());

            if (policyAdmin != null) {
                Map<String, Object> evalContext = new HashMap<>();

                RangerAccessRequestUtil.setCurrentUserInContext(evalContext, userName);

                Set<String> roles = policyAdmin.getRolesFromUserAndGroups(userName, userGroups);

                isAllowed = policyAdmin.isDelegatedAdminAccessAllowedForRead(policy, userName, userGroups, roles, evalContext);
            }

            if (!isAllowed) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User '" + userName + "' does not have delegated-admin privilege on given resources", true);
            }
        } else {
            if (StringUtils.isBlank(policy.getServiceType())) {
                XXService    xService    = daoManager.getXXService().findByName(policy.getService());
                XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

                mapServiceTypeAndImplClass.put(xServiceDef.getName(), xServiceDef.getImplclassname());
                policy.setServiceType(xServiceDef.getName());
            } else if (!mapServiceTypeAndImplClass.containsKey(policy.getServiceType())) {
                XXService    xService    = daoManager.getXXService().findByName(policy.getService());
                XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

                mapServiceTypeAndImplClass.put(xServiceDef.getName(), xServiceDef.getImplclassname());
            }

            String serviceDefImplClass = mapServiceTypeAndImplClass.get(policy.getServiceType());

            if (isAdmin || isAuditAdmin) {
                if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(serviceDefImplClass)) {
                    throw restErrorUtil.createRESTException("KMS Policies/Services/Service-Defs are not accessible for user '" + userName + "'.", MessageEnums.OPER_NO_PERMISSION);
                }
            } else if (isKeyAdmin || isAuditKeyAdmin) {
                if (!EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(serviceDefImplClass)) {
                    throw restErrorUtil.createRESTException("Only KMS Policies/Services/Service-Defs are accessible for user '" + userName + "'.", MessageEnums.OPER_NO_PERMISSION);
                }
            }
        }
    }

    boolean isServiceAdmin(String serviceName) {
        boolean ret = bizUtil.isAdmin();

        if (!ret && StringUtils.isNotEmpty(serviceName)) {
            ret = svcStore.isServiceAdminUser(serviceName, bizUtil.getCurrentUserLoginId());
        }

        return ret;
    }

    private String validateResourcePoliciesRequest(String serviceDefName, String serviceName, HttpServletRequest request, List<RangerService> services, Map<String, Object> resource) {
        LOG.debug("==> ServiceREST.validatePoliciesForResourceRequest(service-type={}, service-name={})", serviceDefName, serviceName);

        final String ret;

        if (MapUtils.isNotEmpty(request.getParameterMap())) {
            for (Entry<String, String[]> e : request.getParameterMap().entrySet()) {
                String   name   = e.getKey();
                String[] values = e.getValue();

                if (!StringUtils.isEmpty(name) && !ArrayUtils.isEmpty(values) && name.startsWith(SearchFilter.RESOURCE_PREFIX)) {
                    resource.put(name.substring(SearchFilter.RESOURCE_PREFIX.length()), values[0]);
                }
            }
        }

        if (MapUtils.isEmpty(resource)) {
            ret = "No resource specified";
        } else {
            RangerServiceDef serviceDef = null;

            try {
                serviceDef = svcStore.getServiceDefByName(serviceDefName);
            } catch (Exception e) {
                LOG.error("Invalid service-type:[{}]", serviceDefName, e);
            }

            if (serviceDef == null) {
                ret = "Invalid service-type:[" + serviceDefName + "]";
            } else {
                Set<String>                                   resourceDefNames    = resource.keySet();
                RangerServiceDefHelper                        serviceDefHelper    = new RangerServiceDefHelper(serviceDef);
                Set<List<RangerServiceDef.RangerResourceDef>> resourceHierarchies = serviceDefHelper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS, resourceDefNames);

                if (CollectionUtils.isEmpty(resourceHierarchies)) {
                    ret = "Invalid resource specified: resource-names:" + resourceDefNames + " are not part of any valid resource hierarchy for service-type:[" + serviceDefName + "]";
                } else {
                    if (StringUtils.isNotBlank(serviceName)) {
                        RangerService service = null;

                        try {
                            service = svcStore.getServiceByName(serviceName);
                        } catch (Exception e) {
                            LOG.error("Invalid service-name:[{}]", serviceName);
                        }

                        if (service == null || !StringUtils.equals(service.getType(), serviceDefName)) {
                            ret = "Invalid service-name:[" + serviceName + "] or service-type:[" + serviceDefName + "]";
                        } else {
                            services.add(service);
                            ret = StringUtils.EMPTY;
                        }
                    } else {
                        SearchFilter filter = new SearchFilter();

                        filter.setParam(SearchFilter.SERVICE_TYPE, serviceDefName);

                        List<RangerService> serviceList = null;

                        try {
                            serviceList = svcStore.getServices(filter);
                        } catch (Exception e) {
                            LOG.error("Cannot find service of service-type:[{}]", serviceDefName);
                        }

                        if (CollectionUtils.isEmpty(serviceList) || serviceList.size() != 1) {
                            ret = "Either 0 or more than 1 services found for service-type :[" + serviceDefName + "]";
                        } else {
                            services.add(serviceList.get(0));

                            ret = StringUtils.EMPTY;
                        }
                    }
                }
            }
        }

        LOG.debug("<== ServiceREST.validatePoliciesForResourceRequest(service-type={}, service-name={}) : {}", serviceDefName, serviceName, ret);

        return ret;
    }

    private void requestParamsValidation(SearchFilter filter) {
        boolean fetchAllZonePolicies = Boolean.parseBoolean(filter.getParam(SearchFilter.FETCH_ZONE_UNZONE_POLICIES));
        String  zoneName             = filter.getParam(SearchFilter.ZONE_NAME);

        if (fetchAllZonePolicies && StringUtils.isNotEmpty(zoneName)) {
            throw restErrorUtil.createRESTException("Invalid parameter: " + SearchFilter.ZONE_NAME + " can not be provided, along with " + SearchFilter.FETCH_ZONE_UNZONE_POLICIES + "=true");
        }
    }

    private int createPolicesBasedOnPolicyMap(HttpServletRequest request, Map<String, RangerPolicy> policiesMap, List<String> serviceNameList, boolean updateIfExists, int totalPolicyCreate) {
        boolean mergeIfExists  = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_MERGE_IF_EXISTS)));
        boolean deleteIfExists = "true".equalsIgnoreCase(StringUtils.trimToEmpty(request.getParameter(PARAM_DELETE_IF_EXISTS)));

        if (!CollectionUtils.sizeIsEmpty(policiesMap.entrySet())) {
            for (Entry<String, RangerPolicy> entry : policiesMap.entrySet()) {
                RangerPolicy policy = entry.getValue();

                if (policy != null) {
                    if (!CollectionUtils.isEmpty(serviceNameList)) {
                        for (String service : serviceNameList) {
                            if (StringUtils.isNotEmpty(service.trim()) && StringUtils.isNotEmpty(policy.getService().trim())) {
                                if (policy.getService().trim().equalsIgnoreCase(service.trim())) {
                                    if (updateIfExists || mergeIfExists || deleteIfExists) {
                                        request.setAttribute(PARAM_SERVICE_NAME, policy.getService());
                                        request.setAttribute(PARAM_POLICY_NAME, policy.getName());
                                        request.setAttribute(PARAM_ZONE_NAME, policy.getZoneName());

                                        if (mergeIfExists && !ServiceRESTUtil.containsRangerCondition(policy)) {
                                            String       user = request.getRemoteUser();
                                            RangerPolicy existingPolicy;

                                            try {
                                                existingPolicy = getExactMatchPolicyForResource(policy, StringUtils.isNotBlank(user) ? user : "admin");
                                            } catch (Exception e) {
                                                existingPolicy = null;
                                            }

                                            if (existingPolicy == null) {
                                                createPolicy(policy, request);
                                            } else {
                                                ServiceRESTUtil.mergeExactMatchPolicyForResource(existingPolicy, policy);

                                                updatePolicy(existingPolicy, null);
                                            }
                                        } else {
                                            createPolicy(policy, request);
                                        }
                                    } else {
                                        createPolicy(policy, request);
                                    }

                                    totalPolicyCreate = totalPolicyCreate + 1;

                                    LOG.debug("Policy {} created successfully.", policy.getName());

                                    break;
                                }
                            } else {
                                LOG.error("Service Name or Policy Name is not provided!!");

                                throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
                            }
                        }
                    } else {
                        if (updateIfExists || mergeIfExists || deleteIfExists) {
                            request.setAttribute(PARAM_SERVICE_NAME, policy.getService());
                            request.setAttribute(PARAM_POLICY_NAME, policy.getName());
                            request.setAttribute(PARAM_ZONE_NAME, policy.getZoneName());

                            if (mergeIfExists && !ServiceRESTUtil.containsRangerCondition(policy)) {
                                String       user = request.getRemoteUser();
                                RangerPolicy existingPolicy;

                                try {
                                    existingPolicy = getExactMatchPolicyForResource(policy, StringUtils.isNotBlank(user) ? user : "admin");
                                } catch (Exception e) {
                                    existingPolicy = null;
                                }

                                if (existingPolicy == null) {
                                    createPolicy(policy, request);
                                } else {
                                    ServiceRESTUtil.mergeExactMatchPolicyForResource(existingPolicy, policy);

                                    updatePolicy(existingPolicy, null);
                                }
                            } else {
                                createPolicy(policy, request);
                            }
                        } else {
                            createPolicy(policy, request);
                        }

                        totalPolicyCreate = totalPolicyCreate + 1;

                        LOG.debug("Policy {} created successfully.", policy.getName());
                    }
                }

                if (totalPolicyCreate % RangerBizUtil.POLICY_BATCH_SIZE == 0) {
                    bizUtil.bulkModeOnlyFlushAndClear();
                }
            }

            bizUtil.bulkModeOnlyFlushAndClear();

            LOG.debug("Total Policy Created From Json file : {}", totalPolicyCreate);
        }

        return totalPolicyCreate;
    }

    private List<RangerPolicy> getPoliciesFromProvidedJson(RangerExportPolicyList rangerExportPolicyList) {
        List<RangerPolicy> policies;

        if (rangerExportPolicyList != null && !CollectionUtils.sizeIsEmpty(rangerExportPolicyList.getPolicies())) {
            policies = rangerExportPolicyList.getPolicies();
        } else {
            LOG.error("Provided json file does not contain any policy!!");

            throw restErrorUtil.createRESTException("Provided json file does not contain any policy!!");
        }

        return policies;
    }

    private RangerExportPolicyList processPolicyInputJsonForMetaData(InputStream uploadedInputStream, RangerExportPolicyList rangerExportPolicyList) throws Exception {
        String policiesString = IOUtils.toString(uploadedInputStream).trim();

        if (StringUtils.isNotEmpty(policiesString)) {
            rangerExportPolicyList = JsonUtilsV2.jsonToObj(policiesString, RangerExportPolicyList.class);
        } else {
            LOG.error("Provided json file is empty!!");

            throw restErrorUtil.createRESTException("Provided json file is empty!!");
        }

        return rangerExportPolicyList;
    }

    private void getServiceNameList(HttpServletRequest request, List<String> serviceNameList) {
        SearchFilter filter          = searchUtil.getSearchFilter(request, policyService.sortFields);
        String       serviceType     = null;
        List<String> serviceTypeList = null;

        if (StringUtils.isNotEmpty(request.getParameter(PARAM_SERVICE_TYPE))) {
            serviceType = request.getParameter(PARAM_SERVICE_TYPE);
        }

        if (StringUtils.isNotEmpty(serviceType)) {
            serviceTypeList = new ArrayList<>(Arrays.asList(serviceType.split(",")));
        }

        List<RangerService> rangerServiceList;
        List<RangerService> rangerServiceLists = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(serviceTypeList)) {
            for (String s : serviceTypeList) {
                filter.removeParam(PARAM_SERVICE_TYPE);
                filter.setParam(PARAM_SERVICE_TYPE, s.trim());

                rangerServiceList = getServices(filter);

                rangerServiceLists.addAll(rangerServiceList);
            }
        }

        if (!CollectionUtils.sizeIsEmpty(rangerServiceLists)) {
            for (RangerService rService : rangerServiceLists) {
                if (StringUtils.isNotEmpty(rService.getName())) {
                    serviceNameList.add(rService.getName());
                }
            }
        }
    }

    private boolean validateDestZoneServiceMapping(String destinationZoneName, RangerPolicy policyInJson, Map<String, String> servicesMappingMap) {
        boolean        isZoneServiceExistAtDestination = false;
        XXSecurityZone xdestZone                       = daoManager.getXXSecurityZoneDao().findByZoneName(destinationZoneName);

        if (xdestZone == null) {
            LOG.error("destination zone provided does not exist");
            throw restErrorUtil.createRESTException("destination zone provided does not exist");
        }

        // CHECK IF json policies service is there on destination and asscioated with
        // destination zone.

        String serviceNameToCheck = policyInJson.getService();

        if (StringUtils.isNotBlank(serviceNameToCheck) && servicesMappingMap.containsKey(serviceNameToCheck)) {
            serviceNameToCheck = servicesMappingMap.get(policyInJson.getService());
        }

        List<XXSecurityZoneRefService>    serviceZoneMapping    = daoManager.getXXSecurityZoneRefService().findByServiceNameAndZoneId(serviceNameToCheck, xdestZone.getId());
        List<XXSecurityZoneRefTagService> tagServiceZoneMapping = daoManager.getXXSecurityZoneRefTagService().findByTagServiceNameAndZoneId(serviceNameToCheck, xdestZone.getId());

        if (!CollectionUtils.isEmpty(serviceZoneMapping) || !CollectionUtils.isEmpty(tagServiceZoneMapping)) {
            isZoneServiceExistAtDestination = true;
        }

        return isZoneServiceExistAtDestination;
    }

    private String getDestinationZoneName(List<String> destinationZones, String zoneNameInJson) {
        String destinationZoneName;

        if (CollectionUtils.isNotEmpty(destinationZones)) {
            destinationZoneName = destinationZones.get(0);
        } else {
            destinationZoneName = zoneNameInJson;
        }

        return destinationZoneName;
    }

    private void processServiceMapping(Map<String, String> servicesMappingMap, List<String> sourceServices, List<String> destinationServices) {
        if (!CollectionUtils.sizeIsEmpty(servicesMappingMap)) {
            for (Entry<String, String> map : servicesMappingMap.entrySet()) {
                String sourceServiceName;
                String destinationServiceName;

                if (StringUtils.isNotEmpty(map.getKey().trim()) && StringUtils.isNotEmpty(map.getValue().trim())) {
                    sourceServiceName      = map.getKey().trim();
                    destinationServiceName = map.getValue().trim();
                } else {
                    LOG.error("Source service or destination service name is not provided!!");

                    throw restErrorUtil.createRESTException("Source service or destonation service name is not provided!!");
                }

                if (StringUtils.isNotEmpty(sourceServiceName) && StringUtils.isNotEmpty(destinationServiceName)) {
                    sourceServices.add(sourceServiceName);
                    destinationServices.add(destinationServiceName);
                }
            }
        }
    }

    private void processZoneMapping(Map<String, String> zoneMappingMap, List<String> sourceZones, List<String> destinationZones) {
        if (!CollectionUtils.sizeIsEmpty(zoneMappingMap)) {
            for (Entry<String, String> map : zoneMappingMap.entrySet()) {
                String sourceZoneName      = null;
                String destinationZoneName = null;

                if (StringUtils.isNotEmpty(map.getKey().trim()) || StringUtils.isNotEmpty(map.getValue().trim())) {
                    // zone to zone
                    // zone to unzone
                    // unzone to zone
                    sourceZoneName      = map.getKey().trim();
                    destinationZoneName = map.getValue().trim();

                    LOG.info("sourceZoneName = {} destinationZoneName = {}", sourceZoneName, destinationZoneName);
                } else if (StringUtils.isEmpty(map.getKey().trim()) && StringUtils.isEmpty(map.getValue().trim())) {
                    LOG.info("Unzone to unzone policies import");
                } else {
                    LOG.error("Source zone or destination zone name is not provided!!");

                    throw restErrorUtil.createRESTException("Source zone or destination zone name is not provided!!");
                }

                if (StringUtils.isNotEmpty(sourceZoneName) || StringUtils.isNotEmpty(destinationZoneName)) {
                    sourceZones.add(sourceZoneName);
                    destinationZones.add(destinationZoneName);
                }
            }
        }
    }

    private List<RangerPolicy> getAllFilteredPolicyList(SearchFilter filter, HttpServletRequest request, List<RangerPolicy> policyLists) {
        String       serviceNames                 = null;
        String       serviceType                  = null;
        List<String> serviceNameList              = null;
        List<String> serviceTypeList              = null;
        List<String> serviceNameInServiceTypeList = new ArrayList<>();
        boolean      isServiceExists;

        if (request.getParameter(PARAM_SERVICE_NAME) != null) {
            serviceNames = request.getParameter(PARAM_SERVICE_NAME);
        }

        if (StringUtils.isNotEmpty(serviceNames)) {
            serviceNameList = new ArrayList<>(Arrays.asList(serviceNames.split(",")));
        }

        if (request.getParameter(PARAM_SERVICE_TYPE) != null) {
            serviceType = request.getParameter(PARAM_SERVICE_TYPE);
        }

        if (StringUtils.isNotEmpty(serviceType)) {
            serviceTypeList = new ArrayList<>(Arrays.asList(serviceType.split(",")));
        }

        List<RangerPolicy> policyList;
        List<RangerPolicy> policyListByServiceName = new ArrayList<>();

        if (filter != null) {
            filter.setStartIndex(0);
            filter.setMaxRows(Integer.MAX_VALUE);

            if (!CollectionUtils.isEmpty(serviceTypeList)) {
                for (String s : serviceTypeList) {
                    filter.removeParam(PARAM_SERVICE_TYPE);

                    if (request.getParameter(PARAM_SERVICE_NAME) != null) {
                        filter.removeParam(PARAM_SERVICE_NAME);
                    }

                    filter.setParam(PARAM_SERVICE_TYPE, s.trim());

                    policyList = getPolicies(filter);

                    policyLists.addAll(policyList);
                }
                if (!CollectionUtils.sizeIsEmpty(policyLists)) {
                    for (RangerPolicy rangerPolicy : policyLists) {
                        if (StringUtils.isNotEmpty(rangerPolicy.getService())) {
                            serviceNameInServiceTypeList.add(rangerPolicy.getService());
                        }
                    }
                }
            }
            if (!CollectionUtils.isEmpty(serviceNameList) && !CollectionUtils.isEmpty(serviceTypeList)) {
                isServiceExists = serviceNameInServiceTypeList.containsAll(serviceNameList);

                if (isServiceExists) {
                    for (String s : serviceNameList) {
                        filter.removeParam(PARAM_SERVICE_NAME);
                        filter.removeParam(PARAM_SERVICE_TYPE);
                        filter.setParam(PARAM_SERVICE_NAME, s.trim());

                        policyList = getPolicies(filter);

                        policyListByServiceName.addAll(policyList);
                    }

                    policyLists = policyListByServiceName;
                } else {
                    policyLists = new ArrayList<>();
                }
            } else if (CollectionUtils.isEmpty(serviceNameList) && CollectionUtils.isEmpty(serviceTypeList)) {
                policyLists = getPolicies(filter);
            }
            if (!CollectionUtils.isEmpty(serviceNameList) && CollectionUtils.isEmpty(serviceTypeList)) {
                for (String s : serviceNameList) {
                    filter.removeParam(PARAM_SERVICE_NAME);
                    filter.setParam(PARAM_SERVICE_NAME, s.trim());

                    policyList = getPolicies(filter);

                    policyLists.addAll(policyList);
                }
            }
        }

        if (StringUtils.isNotEmpty(request.getParameter("resourceMatch")) && "full".equalsIgnoreCase(request.getParameter("resourceMatch"))) {
            policyLists = serviceUtil.getMatchingPoliciesForResource(request, policyLists);
        }

        Map<Long, RangerPolicy> orderedPolicies = new TreeMap<>();

        if (!CollectionUtils.isEmpty(policyLists)) {
            for (RangerPolicy policy : policyLists) {
                if (policy != null) {
                    //set createTime & updateTime Time as null since exported policies dont need this
                    policy.setCreateTime(null);
                    policy.setUpdateTime(null);

                    orderedPolicies.put(policy.getId(), policy);
                }
            }
            if (!orderedPolicies.isEmpty()) {
                policyLists.clear();

                policyLists.addAll(orderedPolicies.values());
            }
        }

        return policyLists;
    }

    private void deletePoliciesProvidedInServiceMap(List<String> sourceServices, List<String> destinationServices, String zoneName) throws Exception {
        int totalDeletedPolicies = 0;

        if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)) {
            RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);

            for (int i = 0; i < sourceServices.size(); i++) {
                if (!destinationServices.get(i).isEmpty()) {
                    SearchFilter filter = new SearchFilter();

                    filter.setParam(SearchFilter.ZONE_NAME, zoneName);
                    filter.setParam(SearchFilter.SERVICE_NAME, destinationServices.get(i));

                    RangerService          service         = getServiceByName(destinationServices.get(i));
                    final RangerPolicyList servicePolicies = getServicePolicies(destinationServices.get(i), filter);

                    if (servicePolicies != null) {
                        List<RangerPolicy> rangerPolicyList = servicePolicies.getPolicies();

                        if (CollectionUtils.isNotEmpty(rangerPolicyList)) {
                            for (RangerPolicy rangerPolicy : rangerPolicyList) {
                                if (rangerPolicy != null) {
                                    validator.validate(rangerPolicy.getId(), Action.DELETE);

                                    ensureAdminAccess(rangerPolicy);

                                    bizUtil.blockAuditorRoleUser();
                                    svcStore.deletePolicy(rangerPolicy, service);

                                    totalDeletedPolicies = totalDeletedPolicies + 1;

                                    if (totalDeletedPolicies % RangerBizUtil.POLICY_BATCH_SIZE == 0) {
                                        bizUtil.bulkModeOnlyFlushAndClear();
                                    }

                                    LOG.debug("Policy {} deleted successfully.", rangerPolicy.getName());
                                    LOG.debug("TotalDeletedPolicies: {}", totalDeletedPolicies);
                                }
                            }

                            bizUtil.bulkModeOnlyFlushAndClear();
                        }
                    }
                }
            }
        }

        LOG.debug("Total Deleted Policy : {}", totalDeletedPolicies);
    }

    private void deletePoliciesForResource(List<String> sourceServices, List<String> destinationServices, HttpServletRequest request, List<RangerPolicy> exportPolicies, String zoneName) throws Exception {
        int totalDeletedPolicies = 0;
        if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)) {
            Set<String> exportedPolicyNames = new HashSet<>();

            if (CollectionUtils.isNotEmpty(exportPolicies)) {
                for (RangerPolicy rangerPolicy : exportPolicies) {
                    if (rangerPolicy != null) {
                        exportedPolicyNames.add(rangerPolicy.getName());
                    }
                }
            }

            for (int i = 0; i < sourceServices.size(); i++) {
                if (!destinationServices.get(i).isEmpty()) {
                    SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

                    filter.setParam("zoneName", zoneName);

                    RangerPolicyList servicePolicies = getServicePolicies(destinationServices.get(i), filter);
                    RangerService    service         = getServiceByName(destinationServices.get(i));

                    if (servicePolicies != null) {
                        List<RangerPolicy> rangerPolicyList = servicePolicies.getPolicies();

                        if (CollectionUtils.isNotEmpty(rangerPolicyList)) {
                            List<RangerPolicy> policiesToBeDeleted = new ArrayList<>();

                            for (RangerPolicy rangerPolicy : rangerPolicyList) {
                                if (rangerPolicy != null) {
                                    Map<String, RangerPolicyResource> rangerPolicyResourceMap = rangerPolicy.getResources();

                                    if (rangerPolicyResourceMap != null) {
                                        RangerPolicyResource rangerPolicyResource = null;

                                        if (rangerPolicyResourceMap.containsKey("path")) {
                                            rangerPolicyResource = rangerPolicyResourceMap.get("path");
                                        } else if (rangerPolicyResourceMap.containsKey("database")) {
                                            rangerPolicyResource = rangerPolicyResourceMap.get("database");
                                        }

                                        if (rangerPolicyResource != null) {
                                            if (CollectionUtils.isNotEmpty(rangerPolicyResource.getValues()) && rangerPolicyResource.getValues().size() > 1) {
                                                continue;
                                            }
                                        }
                                    }

                                    if (rangerPolicy.getId() != null) {
                                        if (!exportedPolicyNames.contains(rangerPolicy.getName())) {
                                            policiesToBeDeleted.add(rangerPolicy);
                                        }
                                    }
                                }
                            }

                            if (CollectionUtils.isNotEmpty(policiesToBeDeleted)) {
                                for (RangerPolicy rangerPolicy : policiesToBeDeleted) {
                                    svcStore.deletePolicy(rangerPolicy, service);

                                    LOG.debug("Policy {} deleted successfully.", rangerPolicy.getName());

                                    totalDeletedPolicies = totalDeletedPolicies + 1;

                                    if (totalDeletedPolicies % RangerBizUtil.POLICY_BATCH_SIZE == 0) {
                                        bizUtil.bulkModeOnlyFlushAndClear();
                                    }
                                }

                                bizUtil.bulkModeOnlyFlushAndClear();
                            }
                        }
                    }
                }
            }
        }
    }

    private RangerPolicyList getServicePolicies(String serviceName, SearchFilter filter) {
        RangerPerfTracer perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceName=" + serviceName + ")");
            }

            // get all policies from the store; pick the page to return after applying filter
            int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
            int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

            if (filter != null) {
                filter.setStartIndex(0);
                filter.setMaxRows(Integer.MAX_VALUE);
            }

            List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceName, filter);

            if (filter != null) {
                filter.setStartIndex(savedStartIndex);
                filter.setMaxRows(savedMaxRows);
            }

            servicePolicies = applyAdminAccessFilter(servicePolicies);

            return toRangerPolicyList(servicePolicies, filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getServicePolicies({}) failed", serviceName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    private void createPolicyDownloadAudit(String serviceName, Long lastKnownVersion, String pluginId, int httpRespCode, String clusterName, String zoneName, HttpServletRequest request) {
        try {
            String ipAddress = request.getHeader("X-FORWARDED-FOR");

            if (ipAddress == null) {
                ipAddress = request.getRemoteAddr();
            }

            XXPolicyExportAudit policyExportAudit = new XXPolicyExportAudit();

            policyExportAudit.setRepositoryName(serviceName);
            policyExportAudit.setAgentId(pluginId);
            policyExportAudit.setClientIP(ipAddress);
            policyExportAudit.setRequestedEpoch(lastKnownVersion);
            policyExportAudit.setHttpRetCode(httpRespCode);
            policyExportAudit.setClusterName(clusterName);
            policyExportAudit.setZoneName(zoneName);

            assetMgr.createPolicyAudit(policyExportAudit);
        } catch (Exception excp) {
            LOG.error("error while creating policy download audit", excp);
        }
    }

    private RangerPolicy getExactMatchPolicyForResource(String serviceName, RangerAccessResource resource, String zoneName, String user) throws Exception {
        LOG.debug("==> ServiceREST.getExactMatchPolicyForResource({}, {}, {})", resource, zoneName, user);

        RangerPolicy       ret         = null;
        RangerPolicyAdmin  policyAdmin = getPolicyAdmin(serviceName);
        List<RangerPolicy> policies    = policyAdmin != null ? policyAdmin.getExactMatchPolicies(resource, zoneName, null) : null;

        if (CollectionUtils.isNotEmpty(policies)) {
            // at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
            ret = svcStore.getPolicy(policies.get(0).getId());
        }

        LOG.debug("<== ServiceREST.getExactMatchPolicyForResource({}, {}, {}): {}", resource, zoneName, user, ret);

        return ret;
    }

    private RangerPolicy getExactMatchPolicyForResource(RangerPolicy policy, String user) throws Exception {
        LOG.debug("==> ServiceREST.getExactMatchPolicyForResource({}, {})", policy, user);

        RangerPolicy       ret         = null;
        RangerPolicyAdmin  policyAdmin = getPolicyAdmin(policy.getService());
        List<RangerPolicy> policies    = policyAdmin != null ? policyAdmin.getExactMatchPolicies(policy, null) : null;

        if (CollectionUtils.isNotEmpty(policies)) {
            // at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
            if (policies.size() == 1) {
                ret = svcStore.getPolicy(policies.get(0).getId());
            } else {
                if (StringUtils.isNotEmpty(policy.getZoneName())) {
                    for (RangerPolicy existingPolicy : policies) {
                        if (StringUtils.equals(policy.getZoneName(), existingPolicy.getZoneName())) {
                            ret = svcStore.getPolicy(existingPolicy.getId());
                            break;
                        }
                    }
                }
            }
        }

        LOG.debug("<== ServiceREST.getExactMatchPolicyForResource({}, {}): {}", policy, user, ret);

        return ret;
    }

    private List<RangerPolicy> applyAdminAccessFilter(List<RangerPolicy> policies) {
        List<RangerPolicy> ret  = new ArrayList<>();
        RangerPerfTracer   perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.applyAdminAccessFilter(policyCount=" + (policies == null ? 0 : policies.size()) + ")");
        }

        if (CollectionUtils.isNotEmpty(policies)) {
            boolean     isAdmin         = bizUtil.isAdmin();
            boolean     isKeyAdmin      = bizUtil.isKeyAdmin();
            String      userName        = bizUtil.getCurrentUserLoginId();
            boolean     isAuditAdmin    = bizUtil.isAuditAdmin();
            boolean     isAuditKeyAdmin = bizUtil.isAuditKeyAdmin();
            Set<String> userGroups      = null;

            Map<String, List<RangerPolicy>> servicePoliciesMap = new HashMap<>();
            Map<String, Object>             evalContext        = new HashMap<>();

            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, userName);

            for (RangerPolicy policy : policies) {
                String             serviceName = policy.getService();
                List<RangerPolicy> policyList  = servicePoliciesMap.computeIfAbsent(serviceName, k -> new ArrayList<>());

                policyList.add(policy);
            }

            for (Entry<String, List<RangerPolicy>> entry : servicePoliciesMap.entrySet()) {
                String             serviceName  = entry.getKey();
                List<RangerPolicy> listToFilter = entry.getValue();

                if (CollectionUtils.isNotEmpty(listToFilter)) {
                    boolean isServiceAdminUser = svcStore.isServiceAdminUser(serviceName, userName);

                    if (isServiceAdminUser) {
                        ret.addAll(listToFilter);
                        continue;
                    } else if (isAdmin || isKeyAdmin || isAuditAdmin || isAuditKeyAdmin) {
                        XXService xService     = daoManager.getXXService().findByName(serviceName);
                        Long      serviceDefId = xService.getType();
                        boolean   isKmsService = serviceDefId.equals(EmbeddedServiceDefsUtil.instance().getKmsServiceDefId());

                        if (isAdmin) {
                            if (!isKmsService) {
                                ret.addAll(listToFilter);
                            }
                        } else if (isAuditAdmin) {
                            if (!isKmsService) {
                                ret.addAll(listToFilter);
                            }
                        } else if (isAuditKeyAdmin) {
                            if (isKmsService) {
                                ret.addAll(listToFilter);
                            }
                        } else if (isKeyAdmin) {
                            if (isKmsService) {
                                ret.addAll(listToFilter);
                            }
                        }

                        continue;
                    }

                    RangerPolicyAdmin policyAdmin = getPolicyAdminForDelegatedAdmin(serviceName);

                    if (policyAdmin != null) {
                        if (userGroups == null) {
                            userGroups = daoManager.getXXGroupUser().findGroupNamesByUserName(userName);
                        }

                        Set<String> roles = policyAdmin.getRolesFromUserAndGroups(userName, userGroups);

                        for (RangerPolicy policy : listToFilter) {
                            if ((policyAdmin.isDelegatedAdminAccessAllowedForRead(policy, userName, userGroups, roles, evalContext)) || (!StringUtils.isEmpty(policy.getZoneName()) && (serviceMgr.isZoneAdmin(policy.getZoneName()) || serviceMgr.isZoneAuditor(policy.getZoneName())))) {
                                ret.add(policy);
                            }
                        }
                    }
                }
            }
        }

        RangerPerfTracer.log(perf);

        return ret;
    }

    private RangerPolicyEngineOptions getDelegatedAdminPolicyEngineOptions() {
        RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

        final String propertyPrefix = "ranger.admin";

        opts.configureDelegateAdmin(config, propertyPrefix);

        return opts;
    }

    private RangerPolicyEngineOptions getPolicySearchRangerAdminPolicyEngineOptions() {
        RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

        final String propertyPrefix = "ranger.admin";

        opts.configureRangerAdminForPolicySearch(config, propertyPrefix);

        return opts;
    }

    private RangerPolicyEngineOptions getDefaultRangerAdminPolicyEngineOptions() {
        RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

        final String propertyPrefix = "ranger.admin";

        opts.configureDefaultRangerAdmin(config, propertyPrefix);

        return opts;
    }

    private boolean hasAdminAccess(RangerPolicy policy, String userName, Set<String> userGroups) {
        boolean           isAllowed   = false;
        RangerPolicyAdmin policyAdmin = getPolicyAdminForDelegatedAdmin(policy.getService());

        if (policyAdmin != null) {
            Map<String, Object> evalContext = new HashMap<>();

            RangerAccessRequestUtil.setCurrentUserInContext(evalContext, userName);

            Set<String> roles = policyAdmin.getRolesFromUserAndGroups(userName, userGroups);

            isAllowed = policyAdmin.isDelegatedAdminAccessAllowedForModify(policy, userName, userGroups, roles, evalContext);
        }

        return isAllowed;
    }

    private boolean hasAdminAccess(String serviceName, String zoneName, String userName, Set<String> userGroups, RangerAccessResource resource, Set<String> accessTypes) {
        boolean isAllowed = false;

        RangerPolicyAdmin policyAdmin = getPolicyAdminForDelegatedAdmin(serviceName);

        if (policyAdmin != null) {
            isAllowed = CollectionUtils.isNotEmpty(accessTypes) && policyAdmin.isDelegatedAdminAccessAllowed(resource, zoneName, userName, userGroups, accessTypes);
        }

        return isAllowed;
    }

    private RangerPolicyAdmin getPolicyAdminForSearch(String serviceName) {
        return RangerPolicyAdminCacheForEngineOptions.getInstance().getServicePoliciesAdmin(serviceName, svcStore, zoneStore, roleDBStore, policySearchAdminOptions);
    }

    private RangerPolicyAdmin getPolicyAdmin(String serviceName) {
        return RangerPolicyAdminCacheForEngineOptions.getInstance().getServicePoliciesAdmin(serviceName, svcStore, zoneStore, roleDBStore, defaultAdminOptions);
    }

    private HashMap<String, Object> getCSRFPropertiesMap(HttpServletRequest request) {
        HashMap<String, Object> map = new HashMap<>();

        map.put(isCSRF_ENABLED, PropertiesUtil.getBooleanProperty(isCSRF_ENABLED, true));
        map.put(CUSTOM_HEADER_PARAM, PropertiesUtil.getProperty(CUSTOM_HEADER_PARAM, RangerCSRFPreventionFilter.HEADER_DEFAULT));
        map.put(BROWSER_USER_AGENT_PARAM, PropertiesUtil.getProperty(BROWSER_USER_AGENT_PARAM, RangerCSRFPreventionFilter.BROWSER_USER_AGENTS_DEFAULT));
        map.put(CUSTOM_METHODS_TO_IGNORE_PARAM, PropertiesUtil.getProperty(CUSTOM_METHODS_TO_IGNORE_PARAM, RangerCSRFPreventionFilter.METHODS_TO_IGNORE_DEFAULT));
        map.put(RangerCSRFPreventionFilter.CSRF_TOKEN, getCSRFToken(request));

        return map;
    }

    private static String getCSRFToken(HttpServletRequest request) {
        String salt = (String) request.getSession().getAttribute(RangerCSRFPreventionFilter.CSRF_TOKEN);

        if (StringUtils.isEmpty(salt)) {
            final int tokenLength = PropertiesUtil.getIntProperty(CSRF_TOKEN_LENGTH, 20);

            salt = RandomStringUtils.random(tokenLength, 0, 0, true, true, null, new SecureRandom());

            request.getSession().setAttribute(RangerCSRFPreventionFilter.CSRF_TOKEN, salt);
        }

        return salt;
    }

    private RangerPolicyList toRangerPolicyList(List<RangerPolicy> policyList, SearchFilter filter) {
        RangerPolicyList ret = new RangerPolicyList();

        if (CollectionUtils.isNotEmpty(policyList)) {
            int    totalCount = policyList.size();
            int    startIndex = filter.getStartIndex();
            int    pageSize   = filter.getMaxRows();
            int    toIndex    = Math.min(startIndex + pageSize, totalCount);
            String sortType   = filter.getSortType();
            String sortBy     = filter.getSortBy();

            if (StringUtils.isNotEmpty(sortBy) && StringUtils.isNotEmpty(sortType)) {
                // By default policyList is sorted by policyId in asc order, So handling only desc case.
                if (SearchFilter.POLICY_ID.equalsIgnoreCase(sortBy)) {
                    if (SORT_ORDER.DESC.name().equalsIgnoreCase(sortType)) {
                        policyList.sort(this.getPolicyComparator(sortBy, sortType));
                    }
                } else if (SearchFilter.POLICY_NAME.equalsIgnoreCase(sortBy)) {
                    if (SORT_ORDER.ASC.name().equalsIgnoreCase(sortType)) {
                        policyList.sort(this.getPolicyComparator(sortBy, sortType));
                    } else if (SORT_ORDER.DESC.name().equalsIgnoreCase(sortType)) {
                        policyList.sort(this.getPolicyComparator(sortBy, sortType));
                    } else {
                        LOG.info("Invalid or Unsupported sortType : {}", sortType);
                    }
                } else {
                    LOG.info("Invalid or Unsupported sortBy property : {}", sortBy);
                }
            }

            List<RangerPolicy> retList = new ArrayList<>();

            for (int i = startIndex; i < toIndex; i++) {
                retList.add(policyList.get(i));
            }

            ret.setPolicies(retList);
            ret.setPageSize(pageSize);
            ret.setResultSize(retList.size());
            ret.setStartIndex(startIndex);
            ret.setTotalCount(totalCount);
            ret.setSortBy(sortBy);
            ret.setSortType(sortType);
        }

        return ret;
    }

    private Comparator<RangerPolicy> getPolicyComparator(String sortBy, String sortType) {
        return (RangerPolicy me, RangerPolicy other) -> {
            int ret = 0;

            if (SearchFilter.POLICY_ID.equalsIgnoreCase(sortBy)) {
                ret = Long.compare(other.getId(), me.getId());
            } else if (SearchFilter.POLICY_NAME.equalsIgnoreCase(sortBy)) {
                if (SORT_ORDER.ASC.name().equalsIgnoreCase(sortType)) {
                    ret = me.getName().compareTo(other.getName());
                } else if (SORT_ORDER.DESC.name().equalsIgnoreCase(sortType)) {
                    ret = other.getName().compareTo(me.getName());
                }
            }

            return ret;
        };
    }

    private void validateGrantRevokeRequest(GrantRevokeRequest request, final boolean hasAdminPrivilege, final String loggedInUser) {
        if (request != null) {
            validateUsersGroupsAndRoles(request.getUsers(), request.getGroups(), request.getRoles());
            validateGrantor(request.getGrantor());
            validateGrantees(request.getUsers());
            validateGroups(request.getGroups());
            validateRoles(request.getRoles());

            if (!hasAdminPrivilege) {
                if (!StringUtils.equals(request.getGrantor(), loggedInUser) || StringUtils.isNotBlank(request.getOwnerUser())) {
                    throw restErrorUtil.createGrantRevokeRESTException("Invalid grant/revoke request - contains grantor or userOwner specification");
                }

                request.setGrantorGroups(userMgr.getGroupsForUser(request.getGrantor()));
            }
        }
    }

    private void validateUsersGroupsAndRoles(Set<String> users, Set<String> groups, Set<String> roles) {
        if (CollectionUtils.isEmpty(users) && CollectionUtils.isEmpty(groups) && CollectionUtils.isEmpty(roles)) {
            throw restErrorUtil.createGrantRevokeRESTException("Grantee users/groups/roles list is empty");
        }
    }

    private void validateGrantor(String grantor) {
        if (grantor != null) {
            try {
                VXUser vxUser = xUserService.getXUserByUserName(grantor);

                if (vxUser == null) {
                    throw restErrorUtil.createGrantRevokeRESTException("Grantor user " + grantor + " doesn't exist");
                }
            } catch (Exception e) {
                throw restErrorUtil.createGrantRevokeRESTException("Grantor user " + grantor + " doesn't exist");
            }
        }
    }

    private void validateGrantees(Set<String> grantees) {
        for (String userName : grantees) {
            try {
                VXUser vxUser = xUserService.getXUserByUserName(userName);

                if (vxUser == null) {
                    throw restErrorUtil.createGrantRevokeRESTException("Grantee user " + userName + " doesn't exist");
                }
            } catch (Exception e) {
                throw restErrorUtil.createGrantRevokeRESTException("Grantee user " + userName + " doesn't exist");
            }
        }
    }

    private void validateGroups(Set<String> groups) {
        for (String groupName : groups) {
            try {
                VXGroup vxGroup = userMgr.getGroupByGroupName(groupName);

                if (vxGroup == null) {
                    throw restErrorUtil.createGrantRevokeRESTException("Grantee group " + groupName + " doesn't exist");
                }
            } catch (Exception e) {
                throw restErrorUtil.createGrantRevokeRESTException("Grantee group " + groupName + " doesn't exist");
            }
        }
    }

    private void validateRoles(Set<String> roles) {
        XXRoleDao roleDao = daoManager.getXXRole();

        for (String role : roles) {
            try {
                XXRole xxRole = roleDao.findByRoleName(role);

                if (xxRole == null) {
                    throw restErrorUtil.createGrantRevokeRESTException("Grantee role " + role + " doesn't exist");
                }
            } catch (Exception e) {
                throw restErrorUtil.createGrantRevokeRESTException("Grantee role " + role + " doesn't exist");
            }
        }
    }

    private Map<String, Object> getOptions(HttpServletRequest request) {
        Map<String, Object> ret = null;

        if (request != null) {
            String isForceRenameOption = request.getParameter(ServiceStore.OPTION_FORCE_RENAME);

            if (StringUtils.isNotBlank(isForceRenameOption)) {
                ret = new HashMap<>();

                ret.put(ServiceStore.OPTION_FORCE_RENAME, Boolean.valueOf(isForceRenameOption));
            }
        }

        return ret;
    }

    private RangerService hideCriticalServiceDetailsForRoleUser(RangerService rangerService) {
        rangerService.setConfigs(null);
        rangerService.setDescription(null);
        rangerService.setCreatedBy(null);
        rangerService.setUpdatedBy(null);
        rangerService.setCreateTime(null);
        rangerService.setUpdateTime(null);
        rangerService.setPolicyVersion(null);
        rangerService.setPolicyUpdateTime(null);
        rangerService.setTagVersion(null);
        rangerService.setTagUpdateTime(null);
        rangerService.setVersion(null);

        return rangerService;
    }

    private void createOrGetLinkedServices(RangerService resourceService) {
        LOG.debug("==> createOrGetLinkedServices(resourceService={})", resourceService.getName());

        Runnable createAndLinkTagServiceTask = () -> {
            final LinkedServiceCreator creator = new LinkedServiceCreator(resourceService.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);

            creator.doCreateAndLinkService();
        };

        rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(createAndLinkTagServiceTask);

        LOG.debug("<== createOrGetLinkedServices(resourceService={})", resourceService.getName());
    }

    private void deleteExactMatchPolicyForResource(List<RangerPolicy> policies, String user, String zoneName) throws Exception {
        if (CollectionUtils.isNotEmpty(policies)) {
            long totalDeletedPolicies = 0;

            for (RangerPolicy rangerPolicy : policies) {
                RangerPolicy existingPolicy;

                try {
                    if (zoneName != null) {
                        rangerPolicy.setZoneName(zoneName);
                    }

                    existingPolicy = getExactMatchPolicyForResource(rangerPolicy, StringUtils.isNotBlank(user) ? user : "admin");
                } catch (Exception e) {
                    existingPolicy = null;
                }

                if (existingPolicy != null) {
                    svcStore.deletePolicy(existingPolicy, null);

                    totalDeletedPolicies = totalDeletedPolicies + 1;

                    if (totalDeletedPolicies % RangerBizUtil.POLICY_BATCH_SIZE == 0) {
                        bizUtil.bulkModeOnlyFlushAndClear();
                    }

                    LOG.debug("Policy {} deleted successfully.", rangerPolicy.getName());
                }
            }

            bizUtil.bulkModeOnlyFlushAndClear();
        }
    }

    private String getRangerAdminZoneName(String serviceName, GrantRevokeRequest grantRevokeRequest) {
        String ret = grantRevokeRequest.getZoneName();

        if (StringUtils.isEmpty(ret)) {
            RangerPolicyAdmin policyAdmin = getPolicyAdmin(serviceName);

            if (policyAdmin != null) {
                ret = policyAdmin.getUniquelyMatchedZoneName(grantRevokeRequest);
            }
        }

        return ret;
    }

    private List<RangerPolicy> getBulkPolicies(String serviceName, HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getBulkPolicies({})", serviceName);

        List<RangerPolicy> ret;
        RangerPerfTracer   perf = null;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getBulkPolicies()");
            }

            SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

            filter.setStartIndex(0);
            filter.setMaxRows(Integer.MAX_VALUE);
            filter.setParam(SearchFilter.SERVICE_NAME, serviceName);

            ret = svcStore.getPolicies(filter);

            ret = applyAdminAccessFilter(ret);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getBulkPolicies() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.getBulkPolicies({}): count={}", serviceName, ret.size());

        return ret;
    }

    private void ensureAdminAccessForServicePolicies(String serviceName, Set<RangerPolicy> policies) {
        LOG.debug("==> ServiceREST.ensureAdminAccessForServicePolicies({})", serviceName);

        if (!policies.isEmpty()) {
            XXService xxService = daoManager.getXXService().findByName(serviceName);

            if (xxService == null) {
                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, serviceName + ": service does not exist", true);
            }

            ensureAdminAccessForPolicies(policies, xxService, serviceName);
        }

        LOG.debug("<== ServiceREST.ensureAdminAccessForServicePolicies({})", serviceName);
    }

    private RangerPolicy createPolicyUnconditionally(RangerPolicy policy) throws Exception {
        LOG.debug("==> ServiceREST.createPolicyUnconditionally({})", policy);

        RangerPolicy ret;

        if (StringUtils.isBlank(policy.getName())) {
            String guid = policy.getGuid();

            if (StringUtils.isBlank(guid)) {
                guid = guidUtil.genGUID();

                policy.setGuid(guid);

                LOG.debug("No GUID supplied on the policy!  Ok, setting GUID to [{}].", guid);
            }

            String name = policy.getService() + "-" + guid;

            policy.setName(name);

            LOG.debug("Policy did not have its name set!  Ok, setting name to [{}]", name);
        } else if (isPolicyNameLengthValidationEnabled) {
            if (policy.getName().length() > maxPolicyNameLength) {
                throw restErrorUtil.createRESTException("Policy name should not be longer than " + maxPolicyNameLength + " characters", MessageEnums.INPUT_DATA_OUT_OF_BOUND, null, "policy name", policy.getName());
            }
        }

        RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);

        validator.validate(policy, Action.CREATE, bizUtil.isAdmin() || isServiceAdmin(policy.getService()) || isZoneAdmin(policy.getZoneName()));

        ensureAdminAccess(policy);

        bizUtil.blockAuditorRoleUser();

        ret = svcStore.createPolicy(policy);

        LOG.debug("<== ServiceREST.createPolicyUnconditionally({})", ret);

        return ret;
    }

    private RangerPolicy getPolicyMatchByName(RangerPolicy policy, HttpServletRequest request) {
        LOG.debug("==> ServiceREST.getPolicyMatchByName({})", policy);

        RangerPolicy existingPolicy = null;
        String       serviceName    = request.getParameter(PARAM_SERVICE_NAME);

        if (serviceName == null) {
            serviceName = (String) request.getAttribute(PARAM_SERVICE_NAME);
        }

        if (StringUtils.isNotEmpty(serviceName)) {
            policy.setService(serviceName);
        }

        String policyName = request.getParameter(PARAM_POLICY_NAME);

        if (policyName == null) {
            policyName = (String) request.getAttribute(PARAM_POLICY_NAME);
        }

        if (StringUtils.isNotEmpty(policyName)) {
            policy.setName(StringUtils.trim(policyName));
        }

        if (StringUtils.isNotEmpty(serviceName) && StringUtils.isNotEmpty(policyName)) {
            String zoneName = request.getParameter(PARAM_ZONE_NAME);

            if (StringUtils.isBlank(zoneName)) {
                zoneName = (String) request.getAttribute(PARAM_ZONE_NAME);
            }

            if (StringUtils.isNotBlank(zoneName)) {
                policy.setZoneName(StringUtils.trim(zoneName));
            }

            existingPolicy = getPolicyByName(policy.getService(), policy.getName(), policy.getZoneName());
        }

        LOG.debug("<== ServiceREST.getPolicyMatchByName({})", existingPolicy);

        return existingPolicy;
    }

    private String deleteServiceById(Long id) {
        LOG.debug("==> ServiceREST.deleteServiceById({})", id);

        RangerContextHolder.getOrCreateOpContext().setBulkModeContext(true);

        RangerPerfTracer perf               = null;
        String           deletedServiceName;

        try {
            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteService(serviceId=" + id + ")");
            }

            RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);

            validator.validate(id, Action.DELETE);

            UserSessionBase session = ContextUtil.getCurrentUserSession();

            if (session != null) {
                XXService service = daoManager.getXXService().getById(id);

                if (service != null) {
                    //if logged-in user is not the service creator then check admin priv.
                    if (!session.getUserId().equals(service.getAddedByUserId())) {
                        bizUtil.hasAdminPermissions("Services");
                    }

                    EmbeddedServiceDefsUtil embeddedServiceDefsUtil = EmbeddedServiceDefsUtil.instance();

                    if (service.getType().equals(embeddedServiceDefsUtil.getTagServiceDefId())) {
                        List<XXService> referringServices = daoManager.getXXService().findByTagServiceId(id);

                        if (!CollectionUtils.isEmpty(referringServices)) {
                            Set<String> referringServiceNames = new HashSet<>();

                            for (XXService xXService : referringServices) {
                                referringServiceNames.add(xXService.getName());

                                if (referringServiceNames.size() >= 10) {
                                    break;
                                }
                            }

                            if (referringServices.size() <= 10) {
                                throw restErrorUtil.createRESTException("Tag service '" + service.getName() + "' is being referenced by " + referringServices.size() + " services: " + referringServiceNames, MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
                            } else {
                                throw restErrorUtil.createRESTException("Tag service '" + service.getName() + "' is being referenced by " + referringServices.size() + " services: " + referringServiceNames + " and more..", MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
                            }
                        }
                    }

                    XXServiceDef xxServiceDef = daoManager.getXXServiceDef().getById(service.getType());

                    if (!session.getUserId().equals(service.getAddedByUserId())) {
                        bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
                        bizUtil.blockAuditorRoleUser();
                    }

                    tagStore.deleteAllTagObjectsForService(service.getName());

                    deletedServiceName = service.getName();

                    svcStore.deleteService(id);
                } else {
                    LOG.error("Cannot retrieve service:[{}] for deletion", id);

                    throw restErrorUtil.createRESTException("Data Not Found for given Id", MessageEnums.DATA_NOT_FOUND, id, null, "readResource : No Object found with given id.");
                }
            } else {
                LOG.error("Cannot retrieve user session.");

                throw new Exception("deleteService(" + id + ") failed");
            }
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("deleteService({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        } finally {
            RangerPerfTracer.log(perf);
        }

        LOG.debug("<== ServiceREST.deleteServiceById() - deletedServiceName={}", deletedServiceName);

        return deletedServiceName;
    }

    private boolean isZoneAdmin(String zoneName) {
        boolean ret = bizUtil.isAdmin();

        if (!ret && StringUtils.isNotEmpty(zoneName)) {
            ret = serviceMgr.isZoneAdmin(zoneName);
        }

        return ret;
    }

    private final class LinkedServiceCreator {
        static final char SEP = '_';

        final String  resourceServiceName;
        final String  linkedServiceType;
        final String  linkedServiceName;
        final boolean isAutoCreate;
        final boolean isAutoLink;

        LinkedServiceCreator(@Nonnull String resourceServiceName, @Nonnull String linkedServiceType) {
            this.resourceServiceName = resourceServiceName;
            this.linkedServiceType   = linkedServiceType;
            this.linkedServiceName   = computeLinkedServiceName();
            this.isAutoCreate        = config.getBoolean("ranger." + linkedServiceType + "service.auto.create", true);
            this.isAutoLink          = config.getBoolean("ranger." + linkedServiceType + "service.auto.link", true);
        }

        @Override
        public String toString() {
            return "{resourceServiceName=" + resourceServiceName + ", linkedServiceType=" + linkedServiceType + ", isAutoCreate=" + isAutoCreate + ", isAutoLink=" + isAutoLink + "}";
        }

        void doCreateAndLinkService() {
            LOG.debug("==> doCreateAndLinkService()");

            RangerService resourceService = null;

            try {
                resourceService = svcStore.getServiceByName(resourceServiceName);

                LOG.info("Successfully retrieved resource-service:[{}]", resourceService.getName());
            } catch (Exception e) {
                LOG.error("Resource-service:[{}] cannot be retrieved", resourceServiceName);
            }

            if (resourceService != null) {
                try {
                    RangerService linkedService = svcStore.getServiceByName(linkedServiceName);

                    if (linkedService == null && isAutoCreate) {
                        linkedService = new RangerService();

                        linkedService.setName(linkedServiceName);
                        linkedService.setDisplayName(linkedServiceName); //set DEFAULT display name
                        linkedService.setType(linkedServiceType);

                        LOG.info("creating service [{}]", linkedServiceName);

                        svcStore.createService(linkedService);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                if (isAutoLink) {
                    doLinkService();
                }
            } else {
                LOG.info("Resource service :[{}] not found! Returning without linking {} service!!", resourceServiceName, linkedServiceType);
            }

            LOG.debug("<== doCreateAndLinkService()");
        }

        private String computeLinkedServiceName() {
            String ret = config.get("ranger." + linkedServiceType + "service.auto.name");

            if (StringUtils.isBlank(ret)) {
                final int lastIndexOfSep = StringUtils.lastIndexOf(resourceServiceName, SEP);

                ret = (lastIndexOfSep != -1) ? resourceServiceName.substring(0, lastIndexOfSep) + SEP + linkedServiceType : linkedServiceType;
            }

            return ret;
        }

        private void doLinkService() {
            LOG.debug("==> doLinkTagService()");

            try {
                RangerService resourceService = svcStore.getServiceByName(resourceServiceName);

                LOG.info("Successfully retrieved resource-service:[{}]", resourceService.getName());

                RangerService linkedService = svcStore.getServiceByName(linkedServiceName);

                if (linkedService == null) {
                    LOG.error("Failed to link service[{}] with service [{}]: {} not found", resourceServiceName, linkedServiceName, linkedServiceName);
                } else if (EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(linkedServiceType)) {
                    LOG.info("Successfully retrieved service:[{}]", linkedService.getName());

                    if (!StringUtils.equals(linkedService.getName(), resourceService.getTagService())) {
                        resourceService.setTagService(linkedService.getName());

                        LOG.info("Linking resource-service[{}] with tag-service [{}]", resourceService.getName(), linkedService.getName());

                        RangerService service = svcStore.updateService(resourceService, null);

                        LOG.info("Updated resource-service:[{}]", service.getName());
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to link service[{}] with service [{}]", resourceServiceName, linkedServiceName);
            }
            LOG.debug("<== doLinkTagService()");
        }
    }
}
