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

package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;

import javax.annotation.PostConstruct;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerFactory;
import org.apache.ranger.common.RangerServicePoliciesCache;
import org.apache.ranger.common.RangerVersionInfo;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXAccessTypeDefGrantsDao;
import org.apache.ranger.db.XXContextEnricherDefDao;
import org.apache.ranger.db.XXDataMaskTypeDefDao;
import org.apache.ranger.db.XXEnumDefDao;
import org.apache.ranger.db.XXEnumElementDefDao;
import org.apache.ranger.db.XXPolicyConditionDefDao;
import org.apache.ranger.db.XXPolicyItemAccessDao;
import org.apache.ranger.db.XXPolicyItemConditionDao;
import org.apache.ranger.db.XXPolicyItemDao;
import org.apache.ranger.db.XXPolicyItemDataMaskInfoDao;
import org.apache.ranger.db.XXPolicyItemGroupPermDao;
import org.apache.ranger.db.XXPolicyItemRowFilterInfoDao;
import org.apache.ranger.db.XXPolicyItemUserPermDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.db.XXPolicyResourceMapDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceConfigDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceVersionInfoDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXAccessTypeDefGrants;
import org.apache.ranger.entity.XXContextEnricherDef;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.entity.XXDataMaskTypeDef;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXEnumElementDef;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyItemDataMaskInfo;
import org.apache.ranger.entity.XXPolicyItemGroupPerm;
import org.apache.ranger.entity.XXPolicyItemRowFilterInfo;
import org.apache.ranger.entity.XXPolicyItemUserPerm;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerRowFilterDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyItemEvaluator;
import org.apache.ranger.plugin.store.AbstractServiceStore;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServicePredicateUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.rest.ServiceREST;
import org.apache.ranger.rest.TagREST;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerPolicyWithAssignedIdService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceDefWithAssignedIdService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.RangerServiceWithAssignedIdService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerExportPolicyList;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.codehaus.jettison.json.JSONException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Component
public class ServiceDBStore extends AbstractServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceDBStore.class);

	private static final String POLICY_ALLOW_EXCLUDE = "Policy Allow:Exclude";
	//private static final String POLICY_ALLOW_INCLUDE = "Policy Allow:Include";
	private static final String POLICY_DENY_EXCLUDE = "Policy Deny:Exclude";
	private static final String POLICY_DENY_INCLUDE = "Policy Deny:Include";
	
	private static String LOCAL_HOSTNAME = "unknown";
	private static final String HOSTNAME = "Host name";
	private static final String USER_NAME = "Exported by";
	private static final String RANGER_VERSION = "Ranger apache version";
	private static final String TIMESTAMP = "Export time";

	private static final String AMBARI_SERVICE_CHECK_USER = "ambari.service.check.user";

        public static final String CRYPT_ALGO = PropertiesUtil.getProperty("ranger.password.encryption.algorithm", PasswordUtils.DEFAULT_CRYPT_ALGO);
        public static final String ENCRYPT_KEY = PropertiesUtil.getProperty("ranger.password.encryption.key", PasswordUtils.DEFAULT_ENCRYPT_KEY);
        public static final String SALT = PropertiesUtil.getProperty("ranger.password.salt", PasswordUtils.DEFAULT_SALT);
        public static final Integer ITERATION_COUNT = PropertiesUtil.getIntProperty("ranger.password.iteration.count", PasswordUtils.DEFAULT_ITERATION_COUNT);

    static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOCAL_HOSTNAME = "unknown";
		}
	}

	@Autowired
	RangerServiceDefService serviceDefService;

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	RangerServiceService svcService;
	
	@Autowired
	StringUtil stringUtil;
	
	@Autowired
	RangerAuditFields rangerAuditFields;
	
	@Autowired
	RangerPolicyService policyService;
	
	@Autowired
	XUserService xUserService;
	
	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	XGroupService xGroupService;

	@Autowired
	RangerDataHistService dataHistService;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    RangerPolicyWithAssignedIdService assignedIdPolicyService;

    @Autowired
    RangerServiceWithAssignedIdService svcServiceWithAssignedId;

    @Autowired
    RangerServiceDefWithAssignedIdService svcDefServiceWithAssignedId;

    @Autowired
    RangerFactory factory;
    
    @Autowired
    JSONUtil jsonUtil;

	@Autowired
	ServiceMgr serviceMgr;

	private static volatile boolean legacyServiceDefsInitDone = false;
	private Boolean populateExistingBaseFields = false;
	
	public static final String HIDDEN_PASSWORD_STR = "*****";
	public static final String CONFIG_KEY_PASSWORD = "password";
	public static final String ACCESS_TYPE_DECRYPT_EEK    = "decrypteek";
	public static final String ACCESS_TYPE_GENERATE_EEK   = "generateeek";
	public static final String ACCESS_TYPE_GET_METADATA   = "getmetadata";

	private ServicePredicateUtil predicateUtil = null;


	@Override
	public void init() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.init()");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.init()");
		}
	}

	@PostConstruct
	public void initStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.initStore()");
		}

		if(! legacyServiceDefsInitDone) {
			synchronized(ServiceDBStore.class) {
				if(!legacyServiceDefsInitDone) {

					if (! RangerConfiguration.getInstance().addAdminResources()) {
						LOG.error("Could not add ranger-admin resources to RangerConfiguration.");
					}

					TransactionTemplate txTemplate = new TransactionTemplate(txManager);

					final ServiceDBStore dbStore = this;
					predicateUtil = new ServicePredicateUtil(dbStore);

					try {
						txTemplate.execute(new TransactionCallback<Object>() {
							@Override
							public Object doInTransaction(TransactionStatus status) {
								EmbeddedServiceDefsUtil.instance().init(dbStore);
								getServiceUpgraded();
								createGenericUsers();
								return null;
							}
						});
					} catch (Throwable ex) {
						LOG.fatal("ServiceDBStore.initStore(): Failed to update DB: " + ex);
					}

					legacyServiceDefsInitDone = true;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.initStore()");
		}
	}

	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.createServiceDef(" + serviceDef + ")");
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(
				serviceDef.getName());
		if (xServiceDef != null) {
			throw restErrorUtil.createRESTException("service-def with name: "
					+ serviceDef.getName() + " already exists",
					MessageEnums.ERROR_DUPLICATE_OBJECT);
		}
		
		List<RangerServiceConfigDef> configs = serviceDef.getConfigs();
		List<RangerResourceDef> resources = serviceDef.getResources();
		List<RangerAccessTypeDef> accessTypes = serviceDef.getAccessTypes();
		List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions();
		List<RangerContextEnricherDef> contextEnrichers = serviceDef.getContextEnrichers();
		List<RangerEnumDef> enums = serviceDef.getEnums();
		RangerDataMaskDef           dataMaskDef          = serviceDef.getDataMaskDef();
		RangerRowFilterDef          rowFilterDef         = serviceDef.getRowFilterDef();
		List<RangerDataMaskTypeDef> dataMaskTypes        = dataMaskDef == null || dataMaskDef.getMaskTypes() == null ? new ArrayList<RangerDataMaskTypeDef>() : dataMaskDef.getMaskTypes();
		List<RangerAccessTypeDef>   dataMaskAccessTypes  = dataMaskDef == null || dataMaskDef.getAccessTypes() == null ? new ArrayList<RangerAccessTypeDef>() : dataMaskDef.getAccessTypes();
		List<RangerResourceDef>     dataMaskResources    = dataMaskDef == null || dataMaskDef.getResources() == null ? new ArrayList<RangerResourceDef>() : dataMaskDef.getResources();
		List<RangerAccessTypeDef>   rowFilterAccessTypes = rowFilterDef == null || rowFilterDef.getAccessTypes() == null ? new ArrayList<RangerAccessTypeDef>() : rowFilterDef.getAccessTypes();
		List<RangerResourceDef>     rowFilterResources   = rowFilterDef == null || rowFilterDef.getResources() == null ? new ArrayList<RangerResourceDef>() : rowFilterDef.getResources();

		RangerServiceDefHelper defHelper = new RangerServiceDefHelper(serviceDef, false);
		defHelper.patchServiceDefWithDefaultValues();

		// While creating, value of version should be 1.
		serviceDef.setVersion(Long.valueOf(1));
		
		if (populateExistingBaseFields) {
			svcDefServiceWithAssignedId.setPopulateExistingBaseFields(true);
			daoMgr.getXXServiceDef().setIdentityInsert(true);

			svcDefServiceWithAssignedId.create(serviceDef);

			svcDefServiceWithAssignedId.setPopulateExistingBaseFields(false);
			daoMgr.getXXServiceDef().updateSequence();
			daoMgr.getXXServiceDef().setIdentityInsert(false);
		} else {
			// following fields will be auto populated
			serviceDef.setId(null);
			serviceDef.setCreateTime(null);
			serviceDef.setUpdateTime(null);
			serviceDef = serviceDefService.create(serviceDef);
		}
		Long serviceDefId = serviceDef.getId();
		XXServiceDef createdSvcDef = daoMgr.getXXServiceDef().getById(serviceDefId);
		
		XXServiceConfigDefDao xxServiceConfigDao = daoMgr.getXXServiceConfigDef();
		for(int i = 0; i < configs.size(); i++) {
			RangerServiceConfigDef config = configs.get(i);

			XXServiceConfigDef xConfig = new XXServiceConfigDef();
			xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xConfig.setOrder(i);
			xConfig = xxServiceConfigDao.create(xConfig);
		}
		
		XXResourceDefDao xxResDefDao = daoMgr.getXXResourceDef();
		for(int i = 0; i < resources.size(); i++) {
			RangerResourceDef resource = resources.get(i);

			XXResourceDef parent = xxResDefDao.findByNameAndServiceDefId(resource.getParent(), serviceDefId);
			Long parentId = (parent != null) ? parent.getId() : null;
			
			XXResourceDef xResource = new XXResourceDef();
			xResource = serviceDefService.populateRangerResourceDefToXX(resource, xResource, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xResource.setOrder(i);
			xResource.setParent(parentId);
			xResource = xxResDefDao.create(xResource);
		}
		
		XXAccessTypeDefDao xxATDDao = daoMgr.getXXAccessTypeDef();
		for(int i = 0; i < accessTypes.size(); i++) {
			RangerAccessTypeDef accessType = accessTypes.get(i);

			XXAccessTypeDef xAccessType = new XXAccessTypeDef();
			xAccessType = serviceDefService.populateRangerAccessTypeDefToXX(accessType, xAccessType, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xAccessType.setOrder(i);
			xAccessType = xxATDDao.create(xAccessType);
			
			Collection<String> impliedGrants = accessType.getImpliedGrants();
			XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
			for(String impliedGrant : impliedGrants) {
				XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
				xImpliedGrant.setAtdId(xAccessType.getId());
				xImpliedGrant.setImpliedGrant(impliedGrant);
				xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
			}
		}
		
		XXPolicyConditionDefDao xxPolCondDao = daoMgr.getXXPolicyConditionDef();
		for (int i = 0; i < policyConditions.size(); i++) {
			RangerPolicyConditionDef policyCondition = policyConditions.get(i);

			XXPolicyConditionDef xPolicyCondition = new XXPolicyConditionDef();
			xPolicyCondition = serviceDefService
					.populateRangerPolicyConditionDefToXX(policyCondition,
							xPolicyCondition, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xPolicyCondition.setOrder(i);
			xPolicyCondition = xxPolCondDao.create(xPolicyCondition);
		}
		
		XXContextEnricherDefDao xxContextEnricherDao = daoMgr.getXXContextEnricherDef();
		for (int i = 0; i < contextEnrichers.size(); i++) {
			RangerContextEnricherDef contextEnricher = contextEnrichers.get(i);

			XXContextEnricherDef xContextEnricher = new XXContextEnricherDef();
			xContextEnricher = serviceDefService
					.populateRangerContextEnricherDefToXX(contextEnricher,
							xContextEnricher, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xContextEnricher.setOrder(i);
			xContextEnricher = xxContextEnricherDao.create(xContextEnricher);
		}
		
		XXEnumDefDao xxEnumDefDao = daoMgr.getXXEnumDef();
		for(RangerEnumDef vEnum : enums) {
			XXEnumDef xEnum = new XXEnumDef();
			xEnum = serviceDefService.populateRangerEnumDefToXX(vEnum, xEnum, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xEnum = xxEnumDefDao.create(xEnum);
			
			List<RangerEnumElementDef> elements = vEnum.getElements();
			XXEnumElementDefDao xxEnumEleDefDao = daoMgr.getXXEnumElementDef();
			for(int i = 0; i < elements.size(); i++) {
				RangerEnumElementDef element = elements.get(i);

				XXEnumElementDef xElement = new XXEnumElementDef();
				xElement = serviceDefService.populateRangerEnumElementDefToXX(element, xElement, xEnum, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xElement.setOrder(i);
				xElement = xxEnumEleDefDao.create(xElement);
			}
		}

		XXDataMaskTypeDefDao xxDataMaskDefDao = daoMgr.getXXDataMaskTypeDef();
		for (int i = 0; i < dataMaskTypes.size(); i++) {
			RangerDataMaskTypeDef dataMask = dataMaskTypes.get(i);

			XXDataMaskTypeDef xDataMaskDef = new XXDataMaskTypeDef();
			xDataMaskDef = serviceDefService.populateRangerDataMaskDefToXX(dataMask, xDataMaskDef, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xDataMaskDef.setOrder(i);
			xDataMaskDef = xxDataMaskDefDao.create(xDataMaskDef);
		}

		List<XXAccessTypeDef> xxAccessTypeDefs = xxATDDao.findByServiceDefId(createdSvcDef.getId());

		for(RangerAccessTypeDef accessType : dataMaskAccessTypes) {
			if(! isAccessTypeInList(accessType.getName(), xxAccessTypeDefs)) {
				throw restErrorUtil.createRESTException("accessType with name: "
								+ accessType.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(RangerAccessTypeDef accessType : rowFilterAccessTypes) {
			if(! isAccessTypeInList(accessType.getName(), xxAccessTypeDefs)) {
				throw restErrorUtil.createRESTException("accessType with name: "
						+ accessType.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(XXAccessTypeDef xxAccessTypeDef : xxAccessTypeDefs) {
			String dataMaskOptions  = null;
			String rowFilterOptions = null;

			for(RangerAccessTypeDef accessTypeDef : dataMaskAccessTypes) {
				if(StringUtils.equals(accessTypeDef.getName(), xxAccessTypeDef.getName())) {
					dataMaskOptions = svcDefServiceWithAssignedId.objectToJson(accessTypeDef);
					break;
				}
			}

			for(RangerAccessTypeDef accessTypeDef : rowFilterAccessTypes) {
				if(StringUtils.equals(accessTypeDef.getName(), xxAccessTypeDef.getName())) {
					rowFilterOptions = svcDefServiceWithAssignedId.objectToJson(accessTypeDef);
					break;
				}
			}

			if(!StringUtils.equals(dataMaskOptions, xxAccessTypeDef.getDataMaskOptions()) ||
			   !StringUtils.equals(rowFilterOptions, xxAccessTypeDef.getRowFilterOptions())) {
				xxAccessTypeDef.setDataMaskOptions(dataMaskOptions);
				xxAccessTypeDef.setRowFilterOptions(rowFilterOptions);

				xxATDDao.update(xxAccessTypeDef);
			}
		}

		List<XXResourceDef> xxResourceDefs = xxResDefDao.findByServiceDefId(createdSvcDef.getId());

		for(RangerResourceDef resource : dataMaskResources) {
			if(! isResourceInList(resource.getName(), xxResourceDefs)) {
				throw restErrorUtil.createRESTException("resource with name: "
						+ resource.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(RangerResourceDef resource : rowFilterResources) {
			if(! isResourceInList(resource.getName(), xxResourceDefs)) {
				throw restErrorUtil.createRESTException("resource with name: "
						+ resource.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(XXResourceDef xxResourceDef : xxResourceDefs) {
			String dataMaskOptions  = null;
			String rowFilterOptions = null;

			for(RangerResourceDef resource : dataMaskResources) {
				if(StringUtils.equals(resource.getName(), xxResourceDef.getName())) {
					dataMaskOptions = svcDefServiceWithAssignedId.objectToJson(resource);
					break;
				}
			}

			for(RangerResourceDef resource : rowFilterResources) {
				if(StringUtils.equals(resource.getName(), xxResourceDef.getName())) {
					rowFilterOptions = svcDefServiceWithAssignedId.objectToJson(resource);
					break;
				}
			}

			if(!StringUtils.equals(dataMaskOptions, xxResourceDef.getDataMaskOptions()) ||
			   !StringUtils.equals(rowFilterOptions, xxResourceDef.getRowFilterOptions())) {
				xxResourceDef.setDataMaskOptions(dataMaskOptions);
				xxResourceDef.setRowFilterOptions(rowFilterOptions);

				xxResDefDao.update(xxResourceDef);
			}
		}

		RangerServiceDef createdServiceDef = serviceDefService.getPopulatedViewObject(createdSvcDef);
		dataHistService.createObjectDataHistory(createdServiceDef, RangerDataHistService.ACTION_CREATE);

		postCreate(createdServiceDef);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.createServiceDef(" + serviceDef + "): " + createdServiceDef);
		}

		return createdServiceDef;
	}

	@Override
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updateServiceDef(" + serviceDef + ")");
		}

		Long serviceDefId = serviceDef.getId();

		XXServiceDef existing = daoMgr.getXXServiceDef().getById(serviceDefId);
		if (existing == null) {
			throw restErrorUtil.createRESTException("no service-def exists with ID=" + serviceDef.getId(),
					MessageEnums.DATA_NOT_FOUND);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(serviceDef.getName(), existingName);

		if (renamed) {
			XXServiceDef renamedSVCDef = daoMgr.getXXServiceDef().findByName(serviceDef.getName());

			if (renamedSVCDef != null) {
				throw restErrorUtil.createRESTException(
						"another service-def already exists with name '" + serviceDef.getName() + "'. ID="
								+ renamedSVCDef.getId(), MessageEnums.DATA_NOT_UPDATABLE);
			}
		}

		List<RangerServiceConfigDef> configs 			= serviceDef.getConfigs() != null 			? serviceDef.getConfigs()   		  : new ArrayList<RangerServiceConfigDef>();
		List<RangerResourceDef> resources 				= serviceDef.getResources() != null  		? serviceDef.getResources() 		  : new ArrayList<RangerResourceDef>();
		List<RangerAccessTypeDef> accessTypes 			= serviceDef.getAccessTypes() != null 		? serviceDef.getAccessTypes() 	  	  : new ArrayList<RangerAccessTypeDef>();
		List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions() != null 	? serviceDef.getPolicyConditions() 	  : new ArrayList<RangerPolicyConditionDef>();
		List<RangerContextEnricherDef> contextEnrichers = serviceDef.getContextEnrichers() != null 	? serviceDef.getContextEnrichers() 	  : new ArrayList<RangerContextEnricherDef>();
		List<RangerEnumDef> enums 						= serviceDef.getEnums() != null 			? serviceDef.getEnums() 			  : new ArrayList<RangerEnumDef>();
		RangerDataMaskDef dataMaskDef                   = serviceDef.getDataMaskDef();
		RangerRowFilterDef rowFilterDef                 = serviceDef.getRowFilterDef();

		RangerServiceDefHelper defHelper = new RangerServiceDefHelper(serviceDef, false);
		defHelper.patchServiceDefWithDefaultValues();

		serviceDef.setCreateTime(existing.getCreateTime());
		serviceDef.setGuid(existing.getGuid());
		serviceDef.setVersion(existing.getVersion());

		serviceDef = serviceDefService.update(serviceDef);
		XXServiceDef createdSvcDef = daoMgr.getXXServiceDef().getById(serviceDefId);

		updateChildObjectsOfServiceDef(createdSvcDef, configs, resources, accessTypes, policyConditions, contextEnrichers, enums, dataMaskDef, rowFilterDef);

		RangerServiceDef updatedSvcDef = getServiceDef(serviceDefId);
		dataHistService.createObjectDataHistory(updatedSvcDef, RangerDataHistService.ACTION_UPDATE);

		postUpdate(updatedSvcDef);


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.updateServiceDef(" + serviceDef + "): " + serviceDef);
		}

		return updatedSvcDef;
	}

	private void updateChildObjectsOfServiceDef(XXServiceDef createdSvcDef, List<RangerServiceConfigDef> configs,
			List<RangerResourceDef> resources, List<RangerAccessTypeDef> accessTypes,
			List<RangerPolicyConditionDef> policyConditions, List<RangerContextEnricherDef> contextEnrichers,
			List<RangerEnumDef> enums, RangerDataMaskDef dataMaskDef, RangerRowFilterDef rowFilterDef) {

		Long serviceDefId = createdSvcDef.getId();

		List<XXServiceConfigDef> xxConfigs = daoMgr.getXXServiceConfigDef().findByServiceDefId(serviceDefId);
		List<XXResourceDef> xxResources = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		List<XXAccessTypeDef> xxAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		List<XXPolicyConditionDef> xxPolicyConditions = daoMgr.getXXPolicyConditionDef().findByServiceDefId(
				serviceDefId);
		List<XXContextEnricherDef> xxContextEnrichers = daoMgr.getXXContextEnricherDef().findByServiceDefId(
				serviceDefId);
		List<XXEnumDef> xxEnums = daoMgr.getXXEnumDef().findByServiceDefId(serviceDefId);

		XXServiceConfigDefDao xxServiceConfigDao = daoMgr.getXXServiceConfigDef();
		for (int i = 0; i < configs.size(); i++) {
			RangerServiceConfigDef config = configs.get(i);
			boolean found = false;
			for (XXServiceConfigDef xConfig : xxConfigs) {
				if (config.getItemId() != null && config.getItemId().equals(xConfig.getItemId())) {
					found = true;
					xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xConfig.setOrder(i);
					xConfig = xxServiceConfigDao.update(xConfig);
					config = serviceDefService.populateXXToRangerServiceConfigDef(xConfig);
					break;
				}
			}
			if (!found) {
				XXServiceConfigDef xConfig = new XXServiceConfigDef();
				xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xConfig.setOrder(i);
				xConfig = xxServiceConfigDao.create(xConfig);
				config = serviceDefService.populateXXToRangerServiceConfigDef(xConfig);
			}
		}
		for (XXServiceConfigDef xConfig : xxConfigs) {
			boolean found = false;
			for (RangerServiceConfigDef config : configs) {
				if (xConfig.getItemId() != null && xConfig.getItemId().equals(config.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				xxServiceConfigDao.remove(xConfig);
			}
		}

		XXResourceDefDao xxResDefDao = daoMgr.getXXResourceDef();
		for (RangerResourceDef resource : resources) {
			boolean found = false;
			for (XXResourceDef xRes : xxResources) {
				if (resource.getItemId() != null && resource.getItemId().equals(xRes.getItemId())) {
					found = true;
					xRes = serviceDefService.populateRangerResourceDefToXX(resource, xRes, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xxResDefDao.update(xRes);
					resource = serviceDefService.populateXXToRangerResourceDef(xRes);
					break;
				}
			}
			if (!found) {
				XXResourceDef parent = xxResDefDao.findByNameAndServiceDefId(resource.getParent(), serviceDefId);
				Long parentId = (parent != null) ? parent.getId() : null;

				XXResourceDef xResource = new XXResourceDef();
				xResource = serviceDefService.populateRangerResourceDefToXX(resource, xResource, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xResource.setParent(parentId);
				xResource = xxResDefDao.create(xResource);
			}
		}
		for (XXResourceDef xRes : xxResources) {
			boolean found = false;
			for (RangerResourceDef resource : resources) {
				if (xRes.getItemId() != null && xRes.getItemId().equals(resource.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXPolicyResource> policyResList = daoMgr.getXXPolicyResource().findByResDefId(xRes.getId());
				if (!stringUtil.isEmpty(policyResList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this resource: "
							+ xRes.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				deleteXXResourceDef(xRes);
			}
		}

		XXAccessTypeDefDao xxATDDao = daoMgr.getXXAccessTypeDef();
		for(int i = 0; i < accessTypes.size(); i++) {
			RangerAccessTypeDef access = accessTypes.get(i);
			boolean found = false;
			for (XXAccessTypeDef xAccess : xxAccessTypes) {
				if (access.getItemId() != null && access.getItemId().equals(xAccess.getItemId())) {
					found = true;
					xAccess = serviceDefService.populateRangerAccessTypeDefToXX(access, xAccess, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xAccess.setOrder(i);
					xAccess = xxATDDao.update(xAccess);

					Collection<String> impliedGrants = access.getImpliedGrants();
					XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
					List<String> xxImpliedGrants = xxATDGrantDao.findImpliedGrantsByATDId(xAccess.getId());
					for (String impliedGrant : impliedGrants) {
						boolean foundGrant = false;
						for (String xImpliedGrant : xxImpliedGrants) {
							if (StringUtils.equalsIgnoreCase(impliedGrant, xImpliedGrant)) {
								foundGrant = true;
								break;
							}
						}
						if (!foundGrant) {
							XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
							xImpliedGrant.setAtdId(xAccess.getId());
							xImpliedGrant.setImpliedGrant(impliedGrant);
							xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
						}
					}
					for (String xImpliedGrant : xxImpliedGrants) {
						boolean foundGrant = false;
						for (String impliedGrant : impliedGrants) {
							if (StringUtils.equalsIgnoreCase(xImpliedGrant, impliedGrant)) {
								foundGrant = true;
								break;
							}
						}
						if (!foundGrant) {
							XXAccessTypeDefGrants xATDGrant = xxATDGrantDao.findByNameAndATDId(xAccess.getId(),
									xImpliedGrant);
							xxATDGrantDao.remove(xATDGrant);

						}
					}
					access = serviceDefService.populateXXToRangerAccessTypeDef(xAccess);
					break;
				}
			}
			if (!found) {
				XXAccessTypeDef xAccessType = new XXAccessTypeDef();
				xAccessType = serviceDefService.populateRangerAccessTypeDefToXX(access, xAccessType, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xAccessType.setOrder(i);
				xAccessType = xxATDDao.create(xAccessType);

				Collection<String> impliedGrants = access.getImpliedGrants();
				XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
				for (String impliedGrant : impliedGrants) {
					XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
					xImpliedGrant.setAtdId(xAccessType.getId());
					xImpliedGrant.setImpliedGrant(impliedGrant);
					xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
				}
				access = serviceDefService.populateXXToRangerAccessTypeDef(xAccessType);
			}
		}

		for (XXAccessTypeDef xAccess : xxAccessTypes) {
			boolean found = false;
			for (RangerAccessTypeDef access : accessTypes) {
				if (xAccess.getItemId() != null && xAccess.getItemId().equals(access.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXPolicyItemAccess> polItemAccessList = daoMgr.getXXPolicyItemAccess().findByType(xAccess.getId());
				if(!stringUtil.isEmpty(polItemAccessList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this access-type: "
							+ xAccess.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				deleteXXAccessTypeDef(xAccess);
			}
		}

		XXPolicyConditionDefDao xxPolCondDao = daoMgr.getXXPolicyConditionDef();
		for (int i = 0; i < policyConditions.size(); i++) {
			RangerPolicyConditionDef condition = policyConditions.get(i);
			boolean found = false;
			for (XXPolicyConditionDef xCondition : xxPolicyConditions) {
				if (condition.getItemId() != null && condition.getItemId().equals(xCondition.getItemId())) {
					found = true;
					xCondition = serviceDefService.populateRangerPolicyConditionDefToXX(condition, xCondition,
							createdSvcDef, RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xCondition.setOrder(i);
					xCondition = xxPolCondDao.update(xCondition);
					condition = serviceDefService.populateXXToRangerPolicyConditionDef(xCondition);
					break;
				}
			}
			if (!found) {
				XXPolicyConditionDef xCondition = new XXPolicyConditionDef();
				xCondition = serviceDefService.populateRangerPolicyConditionDefToXX(condition, xCondition,
						createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xCondition.setOrder(i);
				xCondition = xxPolCondDao.create(xCondition);
				condition = serviceDefService.populateXXToRangerPolicyConditionDef(xCondition);
			}
		}
		for(XXPolicyConditionDef xCondition : xxPolicyConditions) {
			boolean found = false;
			for(RangerPolicyConditionDef condition : policyConditions) {
				if(xCondition.getItemId() != null && xCondition.getItemId().equals(condition.getItemId())) {
					found = true;
					break;
				}
			}
			if(!found) {
				List<XXPolicyItemCondition> policyItemCondList = daoMgr.getXXPolicyItemCondition()
						.findByPolicyConditionDefId(xCondition.getId());
				if(!stringUtil.isEmpty(policyItemCondList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this policy-condition: "
							+ xCondition.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				for(XXPolicyItemCondition policyItemCond : policyItemCondList) {
					daoMgr.getXXPolicyItemCondition().remove(policyItemCond);
				}
				xxPolCondDao.remove(xCondition);
			}
		}

		XXContextEnricherDefDao xxContextEnricherDao = daoMgr.getXXContextEnricherDef();
		for (int i = 0; i < contextEnrichers.size(); i++) {
			RangerContextEnricherDef context = contextEnrichers.get(i);
			boolean found = false;
			for (XXContextEnricherDef xContext : xxContextEnrichers) {
				if (context.getItemId() != null && context.getItemId().equals(xContext.getItemId())) {
					found = true;
					xContext = serviceDefService.populateRangerContextEnricherDefToXX(context, xContext, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xContext.setOrder(i);
					xContext = xxContextEnricherDao.update(xContext);
					context = serviceDefService.populateXXToRangerContextEnricherDef(xContext);
					break;
				}
			}
			if (!found) {
				XXContextEnricherDef xContext = new XXContextEnricherDef();
				xContext = serviceDefService.populateRangerContextEnricherDefToXX(context, xContext, createdSvcDef,
						RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
				xContext.setOrder(i);
				xContext = xxContextEnricherDao.create(xContext);
				context = serviceDefService.populateXXToRangerContextEnricherDef(xContext);
			}
		}
		for (XXContextEnricherDef xContext : xxContextEnrichers) {
			boolean found = false;
			for (RangerContextEnricherDef context : contextEnrichers) {
				if (xContext.getItemId() != null && xContext.getItemId().equals(context.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				daoMgr.getXXContextEnricherDef().remove(xContext);
			}
		}

		XXEnumDefDao xxEnumDefDao = daoMgr.getXXEnumDef();
		for (RangerEnumDef enumDef : enums) {
			boolean found = false;
			for (XXEnumDef xEnumDef : xxEnums) {
				if (enumDef.getItemId() != null && enumDef.getItemId().equals(xEnumDef.getItemId())) {
					found = true;
					xEnumDef = serviceDefService.populateRangerEnumDefToXX(enumDef, xEnumDef, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xEnumDef = xxEnumDefDao.update(xEnumDef);

					XXEnumElementDefDao xEnumEleDao = daoMgr.getXXEnumElementDef();
					List<XXEnumElementDef> xxEnumEleDefs = xEnumEleDao.findByEnumDefId(xEnumDef.getId());
					List<RangerEnumElementDef> enumEleDefs = enumDef.getElements();

					for (int i = 0; i < enumEleDefs.size(); i++) {
						RangerEnumElementDef eleDef = enumEleDefs.get(i);
						boolean foundEle = false;
						for (XXEnumElementDef xEleDef : xxEnumEleDefs) {
							if (eleDef.getItemId() != null && eleDef.getItemId().equals(xEleDef.getItemId())) {
								foundEle = true;
								xEleDef = serviceDefService.populateRangerEnumElementDefToXX(eleDef, xEleDef, xEnumDef,
										RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
								xEleDef.setOrder(i);
								xEleDef = xEnumEleDao.update(xEleDef);
								break;
							}
						}
						if (!foundEle) {
							XXEnumElementDef xElement = new XXEnumElementDef();
							xElement = serviceDefService.populateRangerEnumElementDefToXX(eleDef, xElement, xEnumDef,
									RangerServiceDefService.OPERATION_CREATE_CONTEXT);
							xElement.setOrder(i);
							xElement = xEnumEleDao.create(xElement);
						}
					}
					for (XXEnumElementDef xxEleDef : xxEnumEleDefs) {
						boolean foundEle = false;
						for (RangerEnumElementDef enumEle : enumEleDefs) {
							if (xxEleDef.getItemId() != null && xxEleDef.getItemId().equals(enumEle.getItemId())) {
								foundEle = true;
								break;
							}
						}
						if (!foundEle) {
							xEnumEleDao.remove(xxEleDef);
						}
					}
					enumDef = serviceDefService.populateXXToRangerEnumDef(xEnumDef);
					break;
				}
			}
			if (!found) {
				XXEnumDef xEnum = new XXEnumDef();
				xEnum = serviceDefService.populateRangerEnumDefToXX(enumDef, xEnum, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xEnum = xxEnumDefDao.create(xEnum);

				List<RangerEnumElementDef> elements = enumDef.getElements();
				XXEnumElementDefDao xxEnumEleDefDao = daoMgr.getXXEnumElementDef();
				for (RangerEnumElementDef element : elements) {
					XXEnumElementDef xElement = new XXEnumElementDef();
					xElement = serviceDefService.populateRangerEnumElementDefToXX(element, xElement, xEnum,
							RangerServiceDefService.OPERATION_CREATE_CONTEXT);
					xElement = xxEnumEleDefDao.create(xElement);
				}
				enumDef = serviceDefService.populateXXToRangerEnumDef(xEnum);
			}
		}
		for (XXEnumDef xEnumDef : xxEnums) {
			boolean found = false;
			for (RangerEnumDef enumDef : enums) {
				if (xEnumDef.getItemId() != null && xEnumDef.getItemId().equals(enumDef.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXEnumElementDef> enumEleDefList = daoMgr.getXXEnumElementDef().findByEnumDefId(xEnumDef.getId());
				for (XXEnumElementDef eleDef : enumEleDefList) {
					daoMgr.getXXEnumElementDef().remove(eleDef);
				}
				xxEnumDefDao.remove(xEnumDef);
			}
		}

		List<RangerDataMaskTypeDef> dataMasks            = dataMaskDef == null || dataMaskDef.getMaskTypes() == null ? new ArrayList<RangerDataMaskTypeDef>() : dataMaskDef.getMaskTypes();
		List<RangerAccessTypeDef>   dataMaskAccessTypes  = dataMaskDef == null || dataMaskDef.getAccessTypes() == null ? new ArrayList<RangerAccessTypeDef>() : dataMaskDef.getAccessTypes();
		List<RangerResourceDef>     dataMaskResources    = dataMaskDef == null || dataMaskDef.getResources() == null ? new ArrayList<RangerResourceDef>() : dataMaskDef.getResources();
		List<RangerAccessTypeDef>   rowFilterAccessTypes = rowFilterDef == null || rowFilterDef.getAccessTypes() == null ? new ArrayList<RangerAccessTypeDef>() : rowFilterDef.getAccessTypes();
		List<RangerResourceDef>     rowFilterResources   = rowFilterDef == null || rowFilterDef.getResources() == null ? new ArrayList<RangerResourceDef>() : rowFilterDef.getResources();
		XXDataMaskTypeDefDao        dataMaskTypeDao      = daoMgr.getXXDataMaskTypeDef();
		List<XXDataMaskTypeDef>     xxDataMaskTypes      = dataMaskTypeDao.findByServiceDefId(serviceDefId);
		List<XXAccessTypeDef>       xxAccessTypeDefs     = xxATDDao.findByServiceDefId(serviceDefId);
		List<XXResourceDef>         xxResourceDefs       = xxResDefDao.findByServiceDefId(serviceDefId);

		// create or update dataMasks
		for(int i = 0; i < dataMasks.size(); i++) {
			RangerDataMaskTypeDef dataMask = dataMasks.get(i);
			boolean found = false;
			for (XXDataMaskTypeDef xxDataMask : xxDataMaskTypes) {
				if (xxDataMask.getItemId() != null && xxDataMask.getItemId().equals(dataMask.getItemId())) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Updating existing dataMask with itemId=" + dataMask.getItemId());
					}

					found = true;
					xxDataMask = serviceDefService.populateRangerDataMaskDefToXX(dataMask, xxDataMask, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xxDataMask.setOrder(i);
					xxDataMask = dataMaskTypeDao.update(xxDataMask);
					dataMask = serviceDefService.populateXXToRangerDataMaskTypeDef(xxDataMask);
					break;
				}
			}

			if (!found) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Creating dataMask with itemId=" + dataMask.getItemId() + "");
				}

				XXDataMaskTypeDef xxDataMask = new XXDataMaskTypeDef();
				xxDataMask = serviceDefService.populateRangerDataMaskDefToXX(dataMask, xxDataMask, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xxDataMask.setOrder(i);
				xxDataMask = dataMaskTypeDao.create(xxDataMask);
			}
		}

		// remove dataMasks
		for (XXDataMaskTypeDef xxDataMask : xxDataMaskTypes) {
			boolean found = false;
			for (RangerDataMaskTypeDef dataMask : dataMasks) {
				if (xxDataMask.getItemId() != null && xxDataMask.getItemId().equals(dataMask.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Deleting dataMask with itemId=" + xxDataMask.getItemId());
				}

				dataMaskTypeDao.remove(xxDataMask);
			}
		}

		for(RangerAccessTypeDef accessType : dataMaskAccessTypes) {
			if(! isAccessTypeInList(accessType.getName(), xxAccessTypeDefs)) {
				throw restErrorUtil.createRESTException("accessType with name: "
						+ accessType.getName() + " does not exist", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(RangerAccessTypeDef accessType : rowFilterAccessTypes) {
			if(! isAccessTypeInList(accessType.getName(), xxAccessTypeDefs)) {
				throw restErrorUtil.createRESTException("accessType with name: "
						+ accessType.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(XXAccessTypeDef xxAccessTypeDef : xxAccessTypeDefs) {
			String dataMaskOptions = null;
			String rowFilterOptions = null;

			for(RangerAccessTypeDef accessTypeDef : dataMaskAccessTypes) {
				if(StringUtils.equals(accessTypeDef.getName(), xxAccessTypeDef.getName())) {
					dataMaskOptions = svcDefServiceWithAssignedId.objectToJson(accessTypeDef);
					break;
				}
			}

			for(RangerAccessTypeDef accessTypeDef : rowFilterAccessTypes) {
				if(StringUtils.equals(accessTypeDef.getName(), xxAccessTypeDef.getName())) {
					rowFilterOptions = svcDefServiceWithAssignedId.objectToJson(accessTypeDef);
					break;
				}
			}

			if(!StringUtils.equals(dataMaskOptions, xxAccessTypeDef.getDataMaskOptions()) ||
			   !StringUtils.equals(rowFilterOptions, xxAccessTypeDef.getRowFilterOptions())) {
				xxAccessTypeDef.setDataMaskOptions(dataMaskOptions);
				xxAccessTypeDef.setRowFilterOptions(rowFilterOptions);
				xxATDDao.update(xxAccessTypeDef);
			}
		}

		for(RangerResourceDef resource : dataMaskResources) {
			if(! isResourceInList(resource.getName(), xxResourceDefs)) {
				throw restErrorUtil.createRESTException("resource with name: "
						+ resource.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(RangerResourceDef resource : rowFilterResources) {
			if(! isResourceInList(resource.getName(), xxResourceDefs)) {
				throw restErrorUtil.createRESTException("resource with name: "
						+ resource.getName() + " does not exists", MessageEnums.DATA_NOT_FOUND);
			}
		}

		for(XXResourceDef xxResourceDef : xxResourceDefs) {
			String dataMaskOptions  = null;
			String rowFilterOptions = null;

			for(RangerResourceDef resource : dataMaskResources) {
				if(StringUtils.equals(resource.getName(), xxResourceDef.getName())) {
					dataMaskOptions = svcDefServiceWithAssignedId.objectToJson(resource);
					break;
				}
			}

			for(RangerResourceDef resource : rowFilterResources) {
				if(StringUtils.equals(resource.getName(), xxResourceDef.getName())) {
					rowFilterOptions = svcDefServiceWithAssignedId.objectToJson(resource);
					break;
				}
			}

			if(!StringUtils.equals(dataMaskOptions, xxResourceDef.getDataMaskOptions()) ||
			   !StringUtils.equals(rowFilterOptions, xxResourceDef.getRowFilterOptions())) {
				xxResourceDef.setDataMaskOptions(dataMaskOptions);
				xxResourceDef.setRowFilterOptions(rowFilterOptions);
				xxResDefDao.update(xxResourceDef);
			}
		}
	}

	public void deleteServiceDef(Long serviceDefId, Boolean forceDelete) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deleteServiceDef(" + serviceDefId + ", " + forceDelete + ")");
		}

		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			throw restErrorUtil.createRESTException(
					"UserSession cannot be null, only Admin can update service-def",
					MessageEnums.OPER_NO_PERMISSION);
		}

		if (!session.isKeyAdmin() && !session.isUserAdmin()) {
			throw restErrorUtil.createRESTException(
					"User is not allowed to update service-def, only Admin can update service-def",
					MessageEnums.OPER_NO_PERMISSION);
		}

		RangerServiceDef serviceDef = getServiceDef(serviceDefId);
		if(serviceDef == null) {
			throw restErrorUtil.createRESTException("No Service Definiton found for Id: " + serviceDefId,
					MessageEnums.DATA_NOT_FOUND);
		}
		
		List<XXService> serviceList = daoMgr.getXXService().findByServiceDefId(serviceDefId);
		if (!forceDelete) {
			if(CollectionUtils.isNotEmpty(serviceList)) {
				throw restErrorUtil.createRESTException(
						"Services exists under given service definition, can't delete Service-Def: "
								+ serviceDef.getName(), MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}

		XXDataMaskTypeDefDao dataMaskDao = daoMgr.getXXDataMaskTypeDef();
		List<XXDataMaskTypeDef> dataMaskDefs = dataMaskDao.findByServiceDefId(serviceDefId);
		for(XXDataMaskTypeDef dataMaskDef : dataMaskDefs) {
			dataMaskDao.remove(dataMaskDef);
		}

		List<XXAccessTypeDef> accTypeDefs = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		for(XXAccessTypeDef accessType : accTypeDefs) {
			deleteXXAccessTypeDef(accessType);
		}
		
		XXContextEnricherDefDao xContextEnricherDao = daoMgr.getXXContextEnricherDef();
		List<XXContextEnricherDef> contextEnrichers = xContextEnricherDao.findByServiceDefId(serviceDefId);
		for(XXContextEnricherDef context : contextEnrichers) {
			xContextEnricherDao.remove(context);
		}
		
		XXEnumDefDao enumDefDao = daoMgr.getXXEnumDef();
		List<XXEnumDef> enumDefList = enumDefDao.findByServiceDefId(serviceDefId);
		for (XXEnumDef enumDef : enumDefList) {
			List<XXEnumElementDef> enumEleDefList = daoMgr.getXXEnumElementDef().findByEnumDefId(enumDef.getId());
			for (XXEnumElementDef eleDef : enumEleDefList) {
				daoMgr.getXXEnumElementDef().remove(eleDef);
			}
			enumDefDao.remove(enumDef);
		}
		
		XXPolicyConditionDefDao policyCondDao = daoMgr.getXXPolicyConditionDef();
		List<XXPolicyConditionDef> policyCondList = policyCondDao.findByServiceDefId(serviceDefId);
		
		for (XXPolicyConditionDef policyCond : policyCondList) {
			List<XXPolicyItemCondition> policyItemCondList = daoMgr.getXXPolicyItemCondition().findByPolicyConditionDefId(policyCond.getId());
			for (XXPolicyItemCondition policyItemCond : policyItemCondList) {
				daoMgr.getXXPolicyItemCondition().remove(policyItemCond);
			}
			policyCondDao.remove(policyCond);
		}
		
		List<XXResourceDef> resDefList = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		for(XXResourceDef resDef : resDefList) {
			deleteXXResourceDef(resDef);
		}
		
		XXServiceConfigDefDao configDefDao = daoMgr.getXXServiceConfigDef();
		List<XXServiceConfigDef> configDefList = configDefDao.findByServiceDefId(serviceDefId);
		for(XXServiceConfigDef configDef : configDefList) {
			configDefDao.remove(configDef);
		}
		
		if(CollectionUtils.isNotEmpty(serviceList)) {
			for(XXService service : serviceList) {
				deleteService(service.getId());
			}
		}
		
		Long version = serviceDef.getVersion();
		if(version == null) {
			version = Long.valueOf(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = Long.valueOf(version.longValue() + 1);
		}
		serviceDef.setVersion(version);
		
		serviceDefService.delete(serviceDef);
		LOG.info("ServiceDefinition has been deleted successfully. Service-Def Name: " + serviceDef.getName());
		
		dataHistService.createObjectDataHistory(serviceDef, RangerDataHistService.ACTION_DELETE);

		postDelete(serviceDef);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.deleteServiceDef(" + serviceDefId + ", " + forceDelete + ")");
		}
	}

	public void deleteXXAccessTypeDef(XXAccessTypeDef xAccess) {
		List<XXAccessTypeDefGrants> atdGrantsList = daoMgr.getXXAccessTypeDefGrants().findByATDId(xAccess.getId());

		for (XXAccessTypeDefGrants atdGrant : atdGrantsList) {
			daoMgr.getXXAccessTypeDefGrants().remove(atdGrant);
		}

		List<XXPolicyItemAccess> policyItemAccessList = daoMgr.getXXPolicyItemAccess().findByType(xAccess.getId());
		for (XXPolicyItemAccess policyItemAccess : policyItemAccessList) {
			daoMgr.getXXPolicyItemAccess().remove(policyItemAccess);
		}
		daoMgr.getXXAccessTypeDef().remove(xAccess);
	}

	public void deleteXXResourceDef(XXResourceDef xRes) {

		List<XXResourceDef> xChildObjs = daoMgr.getXXResourceDef().findByParentResId(xRes.getId());
		for(XXResourceDef childRes : xChildObjs) {
			deleteXXResourceDef(childRes);
		}

		List<XXPolicyResource> xxResources = daoMgr.getXXPolicyResource().findByResDefId(xRes.getId());
		for (XXPolicyResource xPolRes : xxResources) {
			deleteXXPolicyResource(xPolRes);
		}

		daoMgr.getXXResourceDef().remove(xRes);
	}

	public void deleteXXPolicyResource(XXPolicyResource xPolRes) {
		List<XXPolicyResourceMap> polResMapList = daoMgr.getXXPolicyResourceMap().findByPolicyResId(xPolRes.getId());
		XXPolicyResourceMapDao polResMapDao = daoMgr.getXXPolicyResourceMap();
		for (XXPolicyResourceMap xxPolResMap : polResMapList) {
			polResMapDao.remove(xxPolResMap);
		}
		daoMgr.getXXPolicyResource().remove(xPolRes);
	}

	@Override
	public RangerServiceDef getServiceDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret = serviceDefService.read(id);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getServiceDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret = null;

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(name);

		if(xServiceDef != null) {
			ret = serviceDefService.getPopulatedViewObject(xServiceDef);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("== ServiceDBStore.getServiceDefByName(" + name + "): " + ret);
		}

		return  ret;
	}

	@Override
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + ")");
		}

		RangerServiceDefList svcDefList = serviceDefService.searchRangerServiceDefs(filter);

		predicateUtil.applyFilter(svcDefList.getServiceDefs(), filter);

		List<RangerServiceDef> ret = svcDefList.getServiceDefs();

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + "): " + ret);
		}

		return ret;
	}

	@Override

	public PList<RangerServiceDef> getPaginatedServiceDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServiceDefs(" + filter + ")");
		}

		RangerServiceDefList svcDefList = serviceDefService.searchRangerServiceDefs(filter);

		predicateUtil.applyFilter(svcDefList.getServiceDefs(), filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServiceDefs(" + filter + ")");
		}

		return new PList<RangerServiceDef>(svcDefList.getServiceDefs(), svcDefList.getStartIndex(), svcDefList.getPageSize(), svcDefList.getTotalCount(),
				svcDefList.getResultSize(), svcDefList.getSortType(), svcDefList.getSortBy());

	}

	@Override
	public RangerService createService(RangerService service) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.createService(" + service + ")");
		}

		if (service == null) {
			throw restErrorUtil.createRESTException("Service object cannot be null.",
					MessageEnums.ERROR_CREATING_OBJECT);
		}

		boolean createDefaultPolicy = true;
		Map<String, String> configs = service.getConfigs();
		Map<String, String> validConfigs = validateRequiredConfigParams(service, configs);
		if (validConfigs == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConfigParams cannot be null, ServiceDBStore.createService(" + service + ")");
			}
			throw restErrorUtil.createRESTException("ConfigParams cannot be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		// While creating, value of version should be 1.
		service.setVersion(Long.valueOf(1));
		service.setTagVersion(Long.valueOf(1));

		if (populateExistingBaseFields) {
			svcServiceWithAssignedId.setPopulateExistingBaseFields(true);
			daoMgr.getXXService().setIdentityInsert(true);

			service = svcServiceWithAssignedId.create(service);

			daoMgr.getXXService().setIdentityInsert(false);
			daoMgr.getXXService().updateSequence();
			svcServiceWithAssignedId.setPopulateExistingBaseFields(false);
			createDefaultPolicy = false;
		} else {
			service = svcService.create(service);
		}
		XXService xCreatedService = daoMgr.getXXService().getById(service.getId());
		VXUser vXUser = null;

		XXServiceConfigMapDao xConfMapDao = daoMgr.getXXServiceConfigMap();
		for (Entry<String, String> configMap : validConfigs.entrySet()) {
			String configKey = configMap.getKey();
			String configValue = configMap.getValue();

			if (StringUtils.equalsIgnoreCase(configKey, "username")) {
				String userName = stringUtil.getValidUserName(configValue);
				XXUser xxUser = daoMgr.getXXUser().findByUserName(userName);
				if (xxUser != null) {
					vXUser = xUserService.populateViewBean(xxUser);
				} else {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isUserAdmin() && !usb.isSpnegoEnabled()) {
						throw restErrorUtil.createRESTException("User does not exist with given username: ["
								+ userName + "] please use existing user", MessageEnums.OPER_NO_PERMISSION);
					}
					vXUser = xUserMgr.createServiceConfigUser(userName);
				}
			}

			if (StringUtils.equalsIgnoreCase(configKey, CONFIG_KEY_PASSWORD)) {
                                String cryptConfigString = CRYPT_ALGO + "," +  ENCRYPT_KEY + "," + SALT + "," + ITERATION_COUNT + "," + configValue;
                                String encryptedPwd = PasswordUtils.encryptPassword(cryptConfigString);
                                encryptedPwd = CRYPT_ALGO + "," +  ENCRYPT_KEY + "," + SALT + "," + ITERATION_COUNT + "," + encryptedPwd;
				String decryptedPwd = PasswordUtils.decryptPassword(encryptedPwd);
				if (StringUtils.equals(decryptedPwd, configValue)) {
					configValue = encryptedPwd;
				}
			}

			XXServiceConfigMap xConfMap = new XXServiceConfigMap();
			xConfMap = rangerAuditFields.populateAuditFields(xConfMap, xCreatedService);
			xConfMap.setServiceId(xCreatedService.getId());
			xConfMap.setConfigkey(configKey);
			xConfMap.setConfigvalue(configValue);
			xConfMapDao.create(xConfMap);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("vXUser:[" + vXUser + "]");
		}
		RangerService createdService = svcService.getPopulatedViewObject(xCreatedService);

		if (createdService == null) {
			throw restErrorUtil.createRESTException("Could not create service - Internal error ", MessageEnums.ERROR_CREATING_OBJECT);
		}

		dataHistService.createObjectDataHistory(createdService, RangerDataHistService.ACTION_CREATE);

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(createdService,
				RangerServiceService.OPERATION_CREATE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);

		if (createDefaultPolicy) {
			createDefaultPolicies(createdService);
		}

		return createdService;

	}

	@Override
	public RangerService updateService(RangerService service, Map<String, Object> options) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updateService()");
		}

		XXService existing = daoMgr.getXXService().getById(service.getId());

		if(existing == null) {
			throw restErrorUtil.createRESTException(
					"no service exists with ID=" + service.getId(),
					MessageEnums.DATA_NOT_FOUND);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(service.getName(), existingName);

		if(renamed) {
            XXService newNameService = daoMgr.getXXService().findByName(service.getName());

            if (newNameService != null) {
                throw restErrorUtil.createRESTException("another service already exists with name '"
                        + service.getName() + "'. ID=" + newNameService.getId(), MessageEnums.DATA_NOT_UPDATABLE);
            }

            long countOfTaggedResources = daoMgr.getXXServiceResource().countTaggedResourcesInServiceId(existing.getId());

            Boolean isForceRename =  options != null && options.get(ServiceStore.OPTION_FORCE_RENAME) != null ? (Boolean) options.get(ServiceStore.OPTION_FORCE_RENAME) : Boolean.FALSE;

            if (countOfTaggedResources != 0L) {
                if (isForceRename) {
                    LOG.warn("Forcing the renaming of service from " + existingName + " to " + service.getName() + " although it is associated with " + countOfTaggedResources
                    + " service-resources!");
                } else {
                    throw restErrorUtil.createRESTException("Service " + existingName + " cannot be renamed, as it has associated service-resources", MessageEnums.DATA_NOT_UPDATABLE);
                }
            }
        }

		Map<String, String> configs = service.getConfigs();
		Map<String, String> validConfigs = validateRequiredConfigParams(service, configs);
		if (validConfigs == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConfigParams cannot be null, ServiceDBStore.createService(" + service + ")");
			}
			throw restErrorUtil.createRESTException("ConfigParams cannot be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		boolean hasTagServiceValueChanged = false;
		Long    existingTagServiceId      = existing.getTagService();
		String  newTagServiceName         = service.getTagService(); // null for old clients; empty string to remove existing association
		Long    newTagServiceId           = null;

		if(newTagServiceName == null) { // old client; don't update existing tagService
			if(existingTagServiceId != null) {
				newTagServiceName = getServiceName(existingTagServiceId);

				service.setTagService(newTagServiceName);

				LOG.info("ServiceDBStore.updateService(id=" + service.getId() + "; name=" + service.getName() + "): tagService is null; using existing tagService '" + newTagServiceName + "'");
			}
		}

		if (StringUtils.isNotBlank(newTagServiceName)) {
			RangerService tmp = getServiceByName(newTagServiceName);

			if (tmp == null || !EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(tmp.getType())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("ServiceDBStore.updateService() - " + newTagServiceName + " does not refer to a valid tag service.(" + service + ")");
				}
				throw restErrorUtil.createRESTException("Invalid tag service name " + newTagServiceName, MessageEnums.ERROR_CREATING_OBJECT);

			} else {
				newTagServiceId = tmp.getId();
			}
		}

		if (existingTagServiceId == null) {
			if (newTagServiceId != null) {
				hasTagServiceValueChanged = true;
			}
		} else if (!existingTagServiceId.equals(newTagServiceId)) {
			hasTagServiceValueChanged = true;
		}

		boolean hasIsEnabledChanged = !existing.getIsenabled().equals(service.getIsEnabled());

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(service, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

		if(populateExistingBaseFields) {
			svcServiceWithAssignedId.setPopulateExistingBaseFields(true);
			service = svcServiceWithAssignedId.update(service);
			svcServiceWithAssignedId.setPopulateExistingBaseFields(false);
		} else {
			service.setCreateTime(existing.getCreateTime());
			service.setGuid(existing.getGuid());
			service.setVersion(existing.getVersion());
			service = svcService.update(service);

			if (hasTagServiceValueChanged || hasIsEnabledChanged) {
				updatePolicyVersion(service, false);
			}
		}

		XXService xUpdService = daoMgr.getXXService().getById(service.getId());

		String oldPassword = null;

		List<XXServiceConfigMap> dbConfigMaps = daoMgr.getXXServiceConfigMap().findByServiceId(service.getId());
		for(XXServiceConfigMap dbConfigMap : dbConfigMaps) {
			if(StringUtils.equalsIgnoreCase(dbConfigMap.getConfigkey(), CONFIG_KEY_PASSWORD)) {
				oldPassword = dbConfigMap.getConfigvalue();
			}
			daoMgr.getXXServiceConfigMap().remove(dbConfigMap);
		}

		VXUser vXUser = null;
		XXServiceConfigMapDao xConfMapDao = daoMgr.getXXServiceConfigMap();
		for (Entry<String, String> configMap : validConfigs.entrySet()) {
			String configKey = configMap.getKey();
			String configValue = configMap.getValue();

			if(StringUtils.equalsIgnoreCase(configKey, "username")) {
				String userName = stringUtil.getValidUserName(configValue);
				XXUser xxUser = daoMgr.getXXUser().findByUserName(userName);
				if (xxUser != null) {
					vXUser = xUserService.populateViewBean(xxUser);
				} else {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isUserAdmin()) {
						throw restErrorUtil.createRESTException("User does not exist with given username: ["
								+ userName + "] please use existing user", MessageEnums.OPER_NO_PERMISSION);
					}
					vXUser = xUserMgr.createServiceConfigUser(userName);
				}
			}

			if (StringUtils.equalsIgnoreCase(configKey, CONFIG_KEY_PASSWORD)) {
				if (StringUtils.equalsIgnoreCase(configValue, HIDDEN_PASSWORD_STR)) {
                                        String[] crypt_algo_array = null;
                                        if (configValue.contains(",")) {
                                                crypt_algo_array = configValue.split(",");
                                        }
                                        if (oldPassword != null && oldPassword.contains(",")) {
						String encryptKey = null;
						String salt = null;
						int iterationCount = 0;

                                                crypt_algo_array = oldPassword.split(",");
                                                String OLD_CRYPT_ALGO = crypt_algo_array[0];
                                                encryptKey = crypt_algo_array[1];
                                                salt = crypt_algo_array[2];
                                                iterationCount = Integer.parseInt(crypt_algo_array[3]);

                                                if (!OLD_CRYPT_ALGO.equalsIgnoreCase(CRYPT_ALGO)) {
                                                        String decryptedPwd = PasswordUtils.decryptPassword(oldPassword);
                                                        String paddingString = CRYPT_ALGO + "," +  encryptKey + "," + salt + "," + iterationCount;
                                                        String encryptedPwd = PasswordUtils.encryptPassword(paddingString + "," + decryptedPwd);
                                                        String newDecryptedPwd = PasswordUtils.decryptPassword(paddingString + "," + encryptedPwd);
                                                        if (StringUtils.equals(newDecryptedPwd, decryptedPwd)) {
                                                                configValue = paddingString + "," + encryptedPwd;
                                                        }
                                                } else {
                                                        configValue = oldPassword;
                                                }
                                        } else {
                                                configValue = oldPassword;
                                        }
				} else {
                                        String paddingString = CRYPT_ALGO + "," +  ENCRYPT_KEY + "," + SALT + "," + ITERATION_COUNT;
                                        String encryptedPwd = PasswordUtils.encryptPassword(paddingString + "," +configValue);
                                        String decryptedPwd = PasswordUtils.decryptPassword(paddingString + "," +encryptedPwd);

					if (StringUtils.equals(decryptedPwd, configValue)) {
                                                configValue = paddingString + "," + encryptedPwd;
					}
				}
			}
			XXServiceConfigMap xConfMap = new XXServiceConfigMap();
			xConfMap = (XXServiceConfigMap) rangerAuditFields.populateAuditFields(xConfMap, xUpdService);
			xConfMap.setServiceId(service.getId());
			xConfMap.setConfigkey(configKey);
			xConfMap.setConfigvalue(configValue);
			xConfMapDao.create(xConfMap);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("vXUser:[" + vXUser + "]");
		}
		RangerService updService = svcService.getPopulatedViewObject(xUpdService);
		dataHistService.createObjectDataHistory(updService, RangerDataHistService.ACTION_UPDATE);
		bizUtil.createTrxLog(trxLogList);

		return updService;
	}

	@Override
	public void deleteService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deleteService(" + id + ")");
		}

		RangerService service = getService(id);

		if(service == null) {
			throw new Exception("no service exists with ID=" + id);
		}

		List<XXPolicy> policies = daoMgr.getXXPolicy().findByServiceId(service.getId());
		RangerPolicy rangerPolicy =null;
		for(XXPolicy policy : policies) {
			LOG.info("Deleting Policy, policyName: " + policy.getName());
			rangerPolicy = getPolicy(policy.getId());
			deletePolicy(rangerPolicy);
		}

		XXServiceConfigMapDao configDao = daoMgr.getXXServiceConfigMap();
		List<XXServiceConfigMap> configs = configDao.findByServiceId(service.getId());
		for (XXServiceConfigMap configMap : configs) {
			configDao.remove(configMap);
		}

		Long version = service.getVersion();
		if(version == null) {
			version = Long.valueOf(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = Long.valueOf(version.longValue() + 1);
		}
		service.setVersion(version);

		svcService.delete(service);

		dataHistService.createObjectDataHistory(service, RangerDataHistService.ACTION_DELETE);

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(service, RangerServiceService.OPERATION_DELETE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);
	}

	@Override
	public List<RangerPolicy> getPoliciesByResourceSignature(String serviceName, String policySignature, Boolean isPolicyEnabled) throws Exception {

		List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByResourceSignatureByPolicyStatus(serviceName, policySignature, isPolicyEnabled);
		List<RangerPolicy> policies = new ArrayList<RangerPolicy>(xxPolicies.size());
		for (XXPolicy xxPolicy : xxPolicies) {
			RangerPolicy policy = policyService.getPopulatedViewObject(xxPolicy);
			policies.add(policy);
		}
		
		return policies;
	}

	@Override
	public RangerService getService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getService()");
		}

		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			throw restErrorUtil.createRESTException("UserSession cannot be null.",
					MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
		}

		XXService xService = daoMgr.getXXService().getById(id);

		// TODO: As of now we are allowing SYS_ADMIN to read all the
		// services including KMS

		if (!bizUtil.hasAccess(xService, null)) {
			throw restErrorUtil.createRESTException("Logged in user is not allowed to read service, id: " + id,
					MessageEnums.OPER_NO_PERMISSION);
		}

		return svcService.getPopulatedViewObject(xService);
	}

	@Override
	public RangerService getServiceByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceByName()");
		}
		XXService xService = daoMgr.getXXService().findByName(name);

		// TODO: As of now we are allowing SYS_ADMIN to read all the
		// services including KMS

		if (ContextUtil.getCurrentUserSession() != null) {
			if (xService == null) {
				return null;
			}
			if (!bizUtil.hasAccess(xService, null)) {
				throw restErrorUtil.createRESTException("Logged in user is not allowed to read service, name: " + name,
						MessageEnums.OPER_NO_PERMISSION);
			}
		}

		return xService == null ? null : svcService.getPopulatedViewObject(xService);
	}

	public RangerService getServiceByNameForDP(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceByNameForDP()");
		}
		XXService xService = daoMgr.getXXService().findByName(name);
		if (ContextUtil.getCurrentUserSession() != null) {
			if (xService == null) {
				return null;
			}
		}
		return xService == null ? null : svcService.getPopulatedViewObject(xService);
	}

	@Override
	public List<RangerService> getServices(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServices()");
		}

		RangerServiceList serviceList = svcService.searchRangerServices(filter);
		predicateUtil.applyFilter(serviceList.getServices(), filter);
		List<RangerService> ret = serviceList.getServices();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServices()");
		}

		return ret;
	}

	public PList<RangerService> getPaginatedServices(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServices()");
		}

		RangerServiceList serviceList = svcService.searchRangerServices(filter);
		if (StringUtils.isEmpty(filter.getParam("serviceNamePartial"))){

		predicateUtil.applyFilter(serviceList.getServices(), filter);

		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServices()");
		}

		return new PList<RangerService>(serviceList.getServices(), serviceList.getStartIndex(), serviceList.getPageSize(), serviceList.getTotalCount(),
				serviceList.getResultSize(), serviceList.getSortType(), serviceList.getSortBy());

	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {

		RangerService service = getServiceByName(policy.getService());

		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(service.getType());

		if(xServiceDef == null) {
			throw new Exception("service-def does not exist - name=" + service.getType());
		}

		XXPolicy existing = daoMgr.getXXPolicy().findByNameAndServiceId(policy.getName(), service.getId());

		if(existing != null) {
			throw new Exception("policy already exists: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName() + ". ID=" + existing.getId());
		}

		Map<String, RangerPolicyResource> resources = policy.getResources();
		List<RangerPolicyItem> policyItems     = policy.getPolicyItems();
		List<RangerPolicyItem> denyPolicyItems = policy.getDenyPolicyItems();
		List<RangerPolicyItem> allowExceptions = policy.getAllowExceptions();
		List<RangerPolicyItem> denyExceptions  = policy.getDenyExceptions();
		List<RangerDataMaskPolicyItem> dataMaskItems  = policy.getDataMaskPolicyItems();
		List<RangerRowFilterPolicyItem> rowFilterItems = policy.getRowFilterPolicyItems();

		policy.setVersion(Long.valueOf(1));
		updatePolicySignature(policy);

		if(populateExistingBaseFields) {
			assignedIdPolicyService.setPopulateExistingBaseFields(true);
			daoMgr.getXXPolicy().setIdentityInsert(true);

			policy = assignedIdPolicyService.create(policy);

			daoMgr.getXXPolicy().setIdentityInsert(false);
			daoMgr.getXXPolicy().updateSequence();
			assignedIdPolicyService.setPopulateExistingBaseFields(false);
		} else {
			policy = policyService.create(policy);
		}

		XXPolicy xCreatedPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		createNewResourcesForPolicy(policy, xCreatedPolicy, resources);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, policyItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, denyPolicyItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, allowExceptions, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, denyExceptions, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS);
		createNewDataMaskPolicyItemsForPolicy(policy, xCreatedPolicy, dataMaskItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DATAMASK);
		createNewRowFilterPolicyItemsForPolicy(policy, xCreatedPolicy, rowFilterItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ROWFILTER);
		handlePolicyUpdate(service, true);
		RangerPolicy createdPolicy = policyService.getPopulatedViewObject(xCreatedPolicy);
		dataHistService.createObjectDataHistory(createdPolicy, RangerDataHistService.ACTION_CREATE);

		List<XXTrxLog> trxLogList = policyService.getTransactionLog(createdPolicy, RangerPolicyService.OPERATION_CREATE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);

		return createdPolicy;
	}

	private boolean validatePolicyItems(List<? extends RangerPolicyItem> policyItems) {

		boolean isPolicyItemValid = true;

		if (CollectionUtils.isNotEmpty(policyItems)) {
			for (RangerPolicyItem policyItem : policyItems) {
				if (policyItem == null) {
					isPolicyItemValid = false;
					break;
				}

				if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
					isPolicyItemValid = false;
					break;
				}

				if (policyItem.getUsers() != null && (policyItem.getUsers().contains(null) || policyItem.getUsers().contains(""))) {
					isPolicyItemValid = false;
					break;
				}

				if (policyItem.getGroups() != null && (policyItem.getGroups().contains(null) || policyItem.getGroups().contains(""))) {
					isPolicyItemValid = false;
					break;
				}

				if (CollectionUtils.isEmpty(policyItem.getAccesses()) || policyItem.getAccesses().contains(null) || policyItem.getAccesses().contains("")) {
					isPolicyItemValid = false;
					break;
				}
			}
		}

		return isPolicyItemValid;
	}

	@Override
	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updatePolicy(" + policy + ")");
		}

		XXPolicy xxExisting = daoMgr.getXXPolicy().getById(policy.getId());
		RangerPolicy existing = policyService.getPopulatedViewObject(xxExisting);

		if(existing == null) {
			throw new Exception("no policy exists with ID=" + policy.getId());
		}

		RangerService service = getServiceByName(policy.getService());

		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(service.getType());

		if(xServiceDef == null) {
			throw new Exception("service-def does not exist - name=" + service.getType());
		}

		if(! StringUtils.equalsIgnoreCase(existing.getService(), policy.getService())) {
			throw new Exception("policy id=" + policy.getId() + " already exists in service " + existing.getService() + ". It can not be moved to service " + policy.getService());
		}
		boolean renamed = !StringUtils.equalsIgnoreCase(policy.getName(), existing.getName());

		if(renamed) {
			XXPolicy newNamePolicy = daoMgr.getXXPolicy().findByNameAndServiceId(policy.getName(), service.getId());

			if(newNamePolicy != null) {
				throw new Exception("another policy already exists with name '" + policy.getName() + "'. ID=" + newNamePolicy.getId());
			}
		}
		Map<String, RangerPolicyResource> newResources = policy.getResources();
		List<RangerPolicyItem> policyItems     = policy.getPolicyItems();
		List<RangerPolicyItem> denyPolicyItems = policy.getDenyPolicyItems();
		List<RangerPolicyItem> allowExceptions = policy.getAllowExceptions();
		List<RangerPolicyItem> denyExceptions  = policy.getDenyExceptions();
		List<RangerDataMaskPolicyItem> dataMaskPolicyItems = policy.getDataMaskPolicyItems();
		List<RangerRowFilterPolicyItem> rowFilterItems = policy.getRowFilterPolicyItems();

		policy.setCreateTime(xxExisting.getCreateTime());
		policy.setGuid(xxExisting.getGuid());
		policy.setVersion(xxExisting.getVersion());

		List<XXTrxLog> trxLogList = policyService.getTransactionLog(policy, xxExisting, RangerPolicyService.OPERATION_UPDATE_CONTEXT);

		updatePolicySignature(policy);

		boolean isTagVersionUpdateNeeded = false;
		if (EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(service.getType())) {
			isTagVersionUpdateNeeded = existing.getIsEnabled() ? !policy.getIsEnabled() : policy.getIsEnabled();
			isTagVersionUpdateNeeded = isTagVersionUpdateNeeded || !StringUtils.equals(existing.getResourceSignature(), policy.getResourceSignature());
		}
		policy = policyService.update(policy);
		XXPolicy newUpdPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		deleteExistingPolicyResources(policy);
		deleteExistingPolicyItems(policy);
		
		createNewResourcesForPolicy(policy, newUpdPolicy, newResources);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, policyItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, denyPolicyItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, allowExceptions, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, denyExceptions, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS);
		createNewDataMaskPolicyItemsForPolicy(policy, newUpdPolicy, dataMaskPolicyItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DATAMASK);
		createNewRowFilterPolicyItemsForPolicy(policy, newUpdPolicy, rowFilterItems, xServiceDef, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ROWFILTER);

		handlePolicyUpdate(service, isTagVersionUpdateNeeded);
		RangerPolicy updPolicy = policyService.getPopulatedViewObject(newUpdPolicy);
		dataHistService.createObjectDataHistory(updPolicy, RangerDataHistService.ACTION_UPDATE);
		
		bizUtil.createTrxLog(trxLogList);
		
		return updPolicy;
	}

	@Override
	public void deletePolicy(Long policyId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deletePolicy(" + policyId + ")");
		}

		RangerPolicy policy = getPolicy(policyId);

		if(policy == null) {
			throw new Exception("no policy exists with ID=" + policyId);
		}

		String policyName = policy.getName();
		RangerService service = getServiceByName(policy.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name='" + policy.getService());
		}
		
		Long version = policy.getVersion();
		if(version == null) {
			version = Long.valueOf(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = Long.valueOf(version.longValue() + 1);
		}
		
		policy.setVersion(version);
		
		List<XXTrxLog> trxLogList = policyService.getTransactionLog(policy, RangerPolicyService.OPERATION_DELETE_CONTEXT);
		
		deleteExistingPolicyItems(policy);
		deleteExistingPolicyResources(policy);
		
		policyService.delete(policy);
		handlePolicyUpdate(service, true);
		
		dataHistService.createObjectDataHistory(policy, RangerDataHistService.ACTION_DELETE);
		
		bizUtil.createTrxLog(trxLogList);
		
		LOG.info("Policy Deleted Successfully. PolicyName : " + policyName);
	}

	public void deletePolicy(RangerPolicy policy) throws Exception {
		if(policy == null) {
			return;
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deletePolicy(" + policy.getId() + ")");
		}
		RangerService service = getServiceByName(policy.getService());
		if(service == null) {
			throw new Exception("service does not exist - name='" + policy.getService());
		}
		Long version = policy.getVersion();
		if(version == null) {
			version = Long.valueOf(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = Long.valueOf(version.longValue() + 1);
		}
		policy.setVersion(version);
		List<XXTrxLog> trxLogList = policyService.getTransactionLog(policy, RangerPolicyService.OPERATION_DELETE_CONTEXT);
		deleteExistingPolicyItemsNative(policy);
		deleteExistingPolicyResourcesNative(policy);
		daoMgr.getXXPolicy().deletePolicyIDReference("id",policy.getId());
		handlePolicyUpdate(service, true);
		dataHistService.createObjectDataHistory(policy, RangerDataHistService.ACTION_DELETE);
		bizUtil.createTrxLog(trxLogList);
	}

	@Override
	public RangerPolicy getPolicy(Long id) throws Exception {
		return policyService.read(id);
	}

	@Override
	public List<RangerPolicy> getPolicies(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPolicies()");
		}
		RangerPolicyList policyList = searchRangerPolicies(filter);
		List<RangerPolicy> ret = policyList.getPolicies();
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPolicies()");
		}
		return ret;
	}

	public void getPoliciesInExcel(List<RangerPolicy> policies, HttpServletResponse response) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPoliciesInExcel()");
		}
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
		String excelFileName = "Ranger_Policies_"+timeStamp+".xls";
		writeExcel(policies, excelFileName, response);
	}

	public void getPoliciesInCSV(List<RangerPolicy> policies,
			HttpServletResponse response) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPoliciesInCSV()");
		}
		ServletOutputStream out = null;
		String CSVFileName = null;
		try {
			String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
			CSVFileName = "Ranger_Policies_" + timeStamp + ".csv";
			out = response.getOutputStream();
			StringBuilder sb = writeCSV(policies, CSVFileName, response);
			IOUtils.write(sb.toString(), out, "UTF-8");
		} catch (Exception e) {
			LOG.error("Error while generating report file " + CSVFileName, e);
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.flush();
					out.close();
				}
			} catch (Exception ex) {
			}
		}
	}
	
	public void getPoliciesInJson(List<RangerPolicy> policies,
			HttpServletResponse response) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPoliciesInJson()");
		}
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
		String jsonFileName = "Ranger_Policies_" + timeStamp + ".json";
		writeJson(policies, jsonFileName, response);
	}

	public PList<RangerPolicy> getPaginatedPolicies(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedPolicies(+ " + filter + ")");
		}

		RangerPolicyList policyList = searchRangerPolicies(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("before filter: count=" + policyList.getListSize());
		}
		predicateUtil.applyFilter(policyList.getPolicies(), filter);
		if (LOG.isDebugEnabled()) {
			LOG.debug("after filter: count=" + policyList.getListSize());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedPolicies(" + filter + "): count=" + policyList.getListSize());
		}


		return new PList<RangerPolicy>(policyList.getPolicies(), policyList.getStartIndex(), policyList.getPageSize(), policyList.getTotalCount(),
				policyList.getResultSize(), policyList.getSortType(), policyList.getSortBy());

	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceId + ")");
		}

		XXService service = daoMgr.getXXService().getById(serviceId);

		if (service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		List<RangerPolicy> ret = getServicePolicies(service, filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceId + ") : policy-count=" + (ret == null ? 0 : ret.size()));
		}
		return ret;

	}

	public PList<RangerPolicy> getPaginatedServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServicePolicies(" + serviceId + ")");
		}

		XXService service = daoMgr.getXXService().getById(serviceId);

		if (service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		PList<RangerPolicy> ret = getPaginatedServicePolicies(service.getName(), filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServicePolicies(" + serviceId + ")");
		}
		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = null;

		XXService service = daoMgr.getXXService().findByName(serviceName);

		if (service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		ret = getServicePolicies(service, filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceName + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	private List<RangerPolicy> getServicePolicies(XXService service, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies()");
		}

		if (service == null) {
			throw new Exception("service does not exist");
		}

		List<RangerPolicy> ret = null;

		ServicePolicies servicePolicies = RangerServicePoliciesCache.getInstance().getServicePolicies(service.getName(), service.getId(), this);
		List<RangerPolicy> policies = servicePolicies != null ? servicePolicies.getPolicies() : null;

		if(policies != null && filter != null) {
			Map<String, String> filterResources = filter.getParamsWithPrefix(SearchFilter.RESOURCE_PREFIX, true);
			String resourceMatchScope = filter.getParam(SearchFilter.RESOURCE_MATCH_SCOPE);

			boolean useLegacyResourceSearch = true;

			if (MapUtils.isNotEmpty(filterResources) && resourceMatchScope != null) {
				useLegacyResourceSearch = false;
				for (Map.Entry<String, String> entry : filterResources.entrySet()) {
					filter.removeParam(SearchFilter.RESOURCE_PREFIX + entry.getKey());
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Using" + (useLegacyResourceSearch ? " old " : " new ") + "way of filtering service-policies");
			}

			ret = new ArrayList<RangerPolicy>(policies);
			predicateUtil.applyFilter(ret, filter);

			if (!useLegacyResourceSearch && CollectionUtils.isNotEmpty(ret)) {
				RangerPolicyResourceMatcher.MatchScope scope;

				if (StringUtils.equalsIgnoreCase(resourceMatchScope, "self")) {
					scope = RangerPolicyResourceMatcher.MatchScope.SELF;
				} else if (StringUtils.equalsIgnoreCase(resourceMatchScope, "ancestor")) {
					scope = RangerPolicyResourceMatcher.MatchScope.ANCESTOR;
				} else if (StringUtils.equalsIgnoreCase(resourceMatchScope, "self_or_ancestor")) {
					scope = RangerPolicyResourceMatcher.MatchScope.SELF_OR_ANCESTOR;
				} else {
					// DESCENDANT match will never happen
					scope = RangerPolicyResourceMatcher.MatchScope.SELF_OR_ANCESTOR;
				}

				RangerServiceDef serviceDef = servicePolicies.getServiceDef();

				switch (scope) {
					case SELF : {
						serviceDef = RangerServiceDefHelper.getServiceDefForPolicyFiltering(serviceDef);
						break;
					}
					case ANCESTOR : {
						Map<String, String> updatedFilterResources = RangerServiceDefHelper.getFilterResourcesForAncestorPolicyFiltering(serviceDef, filterResources);
						if (MapUtils.isNotEmpty(updatedFilterResources)) {
							for (Map.Entry<String, String> entry : updatedFilterResources.entrySet()) {
								filterResources.put(entry.getKey(), entry.getValue());
							}
							scope = RangerPolicyResourceMatcher.MatchScope.SELF_OR_ANCESTOR;
						}
						break;
					}
					default:
						break;
				}

				ret = applyResourceFilter(serviceDef, ret, filterResources, filter, scope);
			}
		} else {
			ret = policies;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	List<RangerPolicy> applyResourceFilter(RangerServiceDef serviceDef, List<RangerPolicy> policies, Map<String, String> filterResources, SearchFilter filter, RangerPolicyResourceMatcher.MatchScope scope) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.applyResourceFilter(policies-size=" + policies.size() + ", filterResources=" + filterResources + ", " + scope + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		List<RangerPolicyResourceMatcher> matchers = getMatchers(serviceDef, filterResources, filter);

		if (CollectionUtils.isNotEmpty(matchers)) {

			for (RangerPolicy policy : policies) {

				for (RangerPolicyResourceMatcher matcher : matchers) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Trying to match for policy:[" + policy + "] using RangerDefaultPolicyResourceMatcher:[" + matcher + "]");
					}

					if (matcher.isMatch(policy, scope, null)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("matched policy:[" + policy + "]");
						}
						ret.add(policy);
						break;
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.applyResourceFilter(policies-size=" + ret.size() + ", filterResources=" + filterResources + ", " + scope + ")");
		}

		return ret;
	}

	List<RangerPolicyResourceMatcher> getMatchers(RangerServiceDef serviceDef, Map<String, String> filterResources, SearchFilter filter) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getMatchers(filterResources=" + filterResources + ")");
		}

		List<RangerPolicyResourceMatcher> ret = new ArrayList<RangerPolicyResourceMatcher>();

		RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);

		String policyTypeStr = filter.getParam(SearchFilter.POLICY_TYPE);

		List<Integer> policyTypes = new ArrayList<>();

		if (StringUtils.isNotBlank(policyTypeStr)) {
			policyTypes.add(Integer.parseInt(policyTypeStr));
		} else {
			policyTypes.add(RangerPolicy.POLICY_TYPE_ACCESS);
			policyTypes.add(RangerPolicy.POLICY_TYPE_DATAMASK);
			policyTypes.add(RangerPolicy.POLICY_TYPE_ROWFILTER);
		}

		for (Integer policyType : policyTypes) {
			Set<List<RangerResourceDef>> validResourceHierarchies = serviceDefHelper.getResourceHierarchies(policyType, filterResources.keySet());

			if (LOG.isDebugEnabled()) {
				LOG.debug("Found " + validResourceHierarchies.size() + " valid resource hierarchies for key-set " + filterResources.keySet());
			}

			List<List<RangerResourceDef>> resourceHierarchies = new ArrayList<List<RangerResourceDef>>(validResourceHierarchies);

			for (List<RangerResourceDef> validResourceHierarchy : resourceHierarchies) {

				if (LOG.isDebugEnabled()) {
					LOG.debug("validResourceHierarchy:[" + validResourceHierarchy + "]");
				}

				Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();

				for (RangerResourceDef resourceDef : validResourceHierarchy) {
					policyResources.put(resourceDef.getName(), new RangerPolicyResource(filterResources.get(resourceDef.getName()), false, resourceDef.getRecursiveSupported()));
				}

				RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();
				matcher.setServiceDef(serviceDef);
				matcher.setPolicyResources(policyResources, policyType);
				matcher.init();

				ret.add(matcher);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Added matcher:[" + matcher + "]");
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getMatchers(filterResources=" + filterResources + ", " + ", count=" + ret.size() + ")");
		}

		return ret;
	}

	private List<RangerPolicy> getServicePoliciesFromDb(XXService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePoliciesFromDb(" + service.getName() + ")");
		}

		RangerPolicyRetriever policyRetriever = new RangerPolicyRetriever(daoMgr);

		List<RangerPolicy> ret = policyRetriever.getServicePolicies(service);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePoliciesFromDb(" + service.getName() + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	public PList<RangerPolicy> getPaginatedServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServicePolicies(" + serviceName + ")");
		}

		if (filter == null) {
			filter = new SearchFilter();
		}

		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);

		PList<RangerPolicy> ret = getPaginatedPolicies(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServicePolicies(" + serviceName + "): count="
					+ ((ret == null) ? 0 : ret.getListSize()));
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		XXService serviceDbObj = daoMgr.getXXService().findByName(serviceName);

		if (serviceDbObj == null) {
			throw new Exception("service does not exist. name=" + serviceName);
		}

		XXServiceVersionInfo serviceVersionInfoDbObj = daoMgr.getXXServiceVersionInfo().findByServiceName(serviceName);

		if (serviceVersionInfoDbObj == null) {
			LOG.warn("serviceVersionInfo does not exist. name=" + serviceName);
		}

		if (lastKnownVersion == null || serviceVersionInfoDbObj == null || serviceVersionInfoDbObj.getPolicyVersion() == null || !lastKnownVersion.equals(serviceVersionInfoDbObj.getPolicyVersion())) {
			ret = RangerServicePoliciesCache.getInstance().getServicePolicies(serviceName, serviceDbObj.getId(), this);
		}

		if (ret != null && lastKnownVersion != null && lastKnownVersion.equals(ret.getPolicyVersion())) {
			// ServicePolicies are not changed
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			RangerServicePoliciesCache.getInstance().dump();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	@Override
	public Long getServicePolicyVersion(String serviceName) {

		XXServiceVersionInfo serviceVersionInfoDbObj = daoMgr.getXXServiceVersionInfo().findByServiceName(serviceName);

		return serviceVersionInfoDbObj != null ? serviceVersionInfoDbObj.getPolicyVersion() : null;
	}

	@Override
	public ServicePolicies getServicePolicies(String serviceName) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceName  + ")");
		}

		ServicePolicies ret = null;

		XXService serviceDbObj = daoMgr.getXXService().findByName(serviceName);

		if (serviceDbObj == null) {
			throw new Exception("service does not exist. name=" + serviceName);
		}

		XXServiceVersionInfo serviceVersionInfoDbObj = daoMgr.getXXServiceVersionInfo().findByServiceName(serviceName);

		if (serviceVersionInfoDbObj == null) {
			LOG.warn("serviceVersionInfo does not exist. name=" + serviceName);
		}

		RangerServiceDef serviceDef = getServiceDef(serviceDbObj.getType());

		if (serviceDef == null) {
			throw new Exception("service-def does not exist. id=" + serviceDbObj.getType());
		}
		List<RangerPolicy> policies = null;
		ServicePolicies.TagPolicies tagPolicies = null;

		String auditMode = getAuditMode(serviceDef.getName(), serviceName);

		if (serviceDbObj.getIsenabled()) {
			if (serviceDbObj.getTagService() != null) {
				XXService tagServiceDbObj = daoMgr.getXXService().getById(serviceDbObj.getTagService());

				if (tagServiceDbObj != null && tagServiceDbObj.getIsenabled()) {
					RangerServiceDef tagServiceDef = getServiceDef(tagServiceDbObj.getType());

					if (tagServiceDef == null) {
						throw new Exception("service-def does not exist. id=" + tagServiceDbObj.getType());
					}

					XXServiceVersionInfo tagServiceVersionInfoDbObj = daoMgr.getXXServiceVersionInfo().findByServiceId(serviceDbObj.getTagService());

					if (tagServiceVersionInfoDbObj == null) {
						LOG.warn("serviceVersionInfo does not exist. name=" + tagServiceDbObj.getName());
					}
					tagPolicies = new ServicePolicies.TagPolicies();

					tagPolicies.setServiceId(tagServiceDbObj.getId());
					tagPolicies.setServiceName(tagServiceDbObj.getName());
					tagPolicies.setPolicyVersion(tagServiceVersionInfoDbObj == null ? null : tagServiceVersionInfoDbObj.getPolicyVersion());
					tagPolicies.setPolicyUpdateTime(tagServiceVersionInfoDbObj == null ? null : tagServiceVersionInfoDbObj.getPolicyUpdateTime());
					tagPolicies.setPolicies(getServicePoliciesFromDb(tagServiceDbObj));
					tagPolicies.setServiceDef(tagServiceDef);
					tagPolicies.setAuditMode(auditMode);
				}
			}

			policies = getServicePoliciesFromDb(serviceDbObj);

		} else {
			policies = new ArrayList<RangerPolicy>();
		}

		ret = new ServicePolicies();

		ret.setServiceId(serviceDbObj.getId());
		ret.setServiceName(serviceDbObj.getName());
		ret.setPolicyVersion(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getPolicyVersion());
		ret.setPolicyUpdateTime(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getPolicyUpdateTime());
		ret.setPolicies(policies);
		ret.setServiceDef(serviceDef);
		ret.setAuditMode(auditMode);
		ret.setTagPolicies(tagPolicies);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceName  + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	void createDefaultPolicies(RangerService createdService) throws Exception {

		RangerBaseService svc = serviceMgr.getRangerServiceByService(createdService, this);

		if (svc != null) {

			List<String> serviceCheckUsers = getServiceCheckUsers(createdService);

			List<RangerPolicy> defaultPolicies = svc.getDefaultRangerPolicies();

			if (CollectionUtils.isNotEmpty(defaultPolicies)) {

				createDefaultPolicyUsersAndGroups(defaultPolicies);

				for (RangerPolicy defaultPolicy : defaultPolicies) {
					if (CollectionUtils.isNotEmpty(serviceCheckUsers) && StringUtils.equalsIgnoreCase(defaultPolicy.getService(), createdService.getName())) {
						RangerPolicyItem defaultAllowPolicyItem = CollectionUtils.isNotEmpty(defaultPolicy.getPolicyItems()) ? defaultPolicy.getPolicyItems().get(0) : null;

						if (defaultAllowPolicyItem == null) {
							LOG.error("There is no allow-policy-item in the default-policy:[" + defaultPolicy + "]");
						} else {
							RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

							policyItem.setUsers(serviceCheckUsers);
							policyItem.setAccesses(defaultAllowPolicyItem.getAccesses());
							policyItem.setDelegateAdmin(true);

							defaultPolicy.getPolicyItems().add(policyItem);
						}
					}

					boolean isPolicyItemValid = validatePolicyItems(defaultPolicy.getPolicyItems())
							&& validatePolicyItems(defaultPolicy.getDenyPolicyItems())
							&& validatePolicyItems(defaultPolicy.getAllowExceptions())
							&& validatePolicyItems(defaultPolicy.getDenyExceptions())
							&& validatePolicyItems(defaultPolicy.getDataMaskPolicyItems())
							&& validatePolicyItems(defaultPolicy.getRowFilterPolicyItems());

					if (isPolicyItemValid) {
						createPolicy(defaultPolicy);
					} else {
						LOG.warn("Default policy won't be created,since policyItems not valid-either users/groups not present or access not present in policy.");
					}
				}
			}
		}
	}

	void createDefaultPolicyUsersAndGroups(List<RangerPolicy> defaultPolicies) {
		Set<String> defaultPolicyUsers = new HashSet<String>();
		Set<String> defaultPolicyGroups = new HashSet<String>();

		for (RangerPolicy defaultPolicy : defaultPolicies) {

			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getPolicyItems()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getAllowExceptions()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getDenyPolicyItems()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getDenyExceptions()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getDataMaskPolicyItems()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
			for (RangerPolicyItem defaultPolicyItem : defaultPolicy.getRowFilterPolicyItems()) {
				defaultPolicyUsers.addAll(defaultPolicyItem.getUsers());
				defaultPolicyGroups.addAll(defaultPolicyItem.getGroups());
			}
		}
		for (String policyUser : defaultPolicyUsers) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking policyUser:[" + policyUser + "] for existence");
			}
			if (StringUtils.isNotBlank(policyUser) && !StringUtils.equals(policyUser, RangerPolicyEngine.USER_CURRENT)
					&& !StringUtils.equals(policyUser, RangerPolicyEngine.RESOURCE_OWNER)) {
				XXUser xxUser = daoMgr.getXXUser().findByUserName(policyUser);
				if (xxUser == null) {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isKeyAdmin() && !usb.isUserAdmin() && !usb.isSpnegoEnabled()) {
						throw restErrorUtil.createRESTException("User does not exist with given username: ["
								+ policyUser + "] please use existing user", MessageEnums.OPER_NO_PERMISSION);
					}
					xUserMgr.createServiceConfigUser(policyUser);
				}
			}
		}
		for (String policyGroup : defaultPolicyGroups) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking policyGroup:[" + policyGroup + "] for existence");
			}
			if (StringUtils.isNotBlank(policyGroup)) {
				XXGroup xxGroup = daoMgr.getXXGroup().findByGroupName(policyGroup);
				if (xxGroup == null) {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isKeyAdmin() && !usb.isUserAdmin() && !usb.isSpnegoEnabled()) {
						throw restErrorUtil.createRESTException("Group does not exist with given groupname: ["
								+ policyGroup + "] please use existing group", MessageEnums.OPER_NO_PERMISSION);
					}
					VXGroup vXGroup = new VXGroup();
					vXGroup.setName(policyGroup);
					vXGroup.setDescription(policyGroup);
					vXGroup.setGroupSource(RangerCommonEnums.GROUP_INTERNAL);
					vXGroup.setIsVisible(RangerCommonEnums.IS_VISIBLE);
					xGroupService.createResource(vXGroup);
				}
			}
		}
	}

	List<String> getServiceCheckUsers(RangerService createdService) {
		List<String> ret = new ArrayList<String>();

		Map<String, String> serviceConfig = createdService.getConfigs();

		if (serviceConfig.containsKey(AMBARI_SERVICE_CHECK_USER)) {
			String userNames = serviceConfig.get(AMBARI_SERVICE_CHECK_USER);
			String[] userList = userNames.split(",");
			for (String userName : userList) {
				if (!StringUtils.isEmpty(userName)) {
					ret.add(userName);
				}
			}
		}

		return ret;
	}

	private Map<String, String> validateRequiredConfigParams(RangerService service, Map<String, String> configs) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.validateRequiredConfigParams()");
		}
		if(configs == null) {
			return null;
		}
		
		List<XXServiceConfigDef> svcConfDefList = daoMgr.getXXServiceConfigDef()
				.findByServiceDefName(service.getType());
		for(XXServiceConfigDef svcConfDef : svcConfDefList ) {
			String confField = configs.get(svcConfDef.getName());
			
			if(svcConfDef.getIsMandatory() && stringUtil.isEmpty(confField)) {
				throw restErrorUtil.createRESTException(
						"Please provide value of mandatory: "+ svcConfDef.getName(),
						MessageEnums.INVALID_INPUT_DATA);
			}
		}
		Map<String, String> validConfigs = new HashMap<String, String>();
		for(Entry<String, String> config : configs.entrySet()) {
			if(!stringUtil.isEmpty(config.getValue())) {
				validConfigs.put(config.getKey(), config.getValue());
			}
		}
		return validConfigs;
	}

	private void handlePolicyUpdate(RangerService service, boolean isTagVersionUpdateNeeded) throws Exception {
		updatePolicyVersion(service, isTagVersionUpdateNeeded);
	}

	private void updatePolicyVersion(RangerService service, boolean isTagVersionUpdateNeeded) throws Exception {
		if(service == null || service.getId() == null) {
			return;
		}

		boolean filterForServicePlugin = RangerConfiguration.getInstance().getBoolean(RangerTagDBRetriever.OPTION_RANGER_FILTER_TAGS_FOR_SERVICE_PLUGIN, false);

		XXServiceDao serviceDao = daoMgr.getXXService();

		XXService serviceDbObj = serviceDao.getById(service.getId());
		if(serviceDbObj == null) {
			LOG.warn("updatePolicyVersion(serviceId=" + service.getId() + "): service not found");

			return;
		}

		XXServiceVersionInfoDao serviceVersionInfoDao = daoMgr.getXXServiceVersionInfo();

		XXServiceVersionInfo serviceVersionInfoDbObj = serviceVersionInfoDao.findByServiceId(service.getId());

		if(serviceVersionInfoDbObj != null) {
			serviceVersionInfoDbObj.setPolicyVersion(getNextVersion(serviceVersionInfoDbObj.getPolicyVersion()));
			serviceVersionInfoDbObj.setPolicyUpdateTime(new Date());

			serviceVersionInfoDao.update(serviceVersionInfoDbObj);

		} else {
			LOG.warn("updatePolicyVersion(service=" + serviceDbObj.getName() + "): serviceVersionInfo not found, creating it..");

			serviceVersionInfoDbObj = new XXServiceVersionInfo();
			serviceVersionInfoDbObj.setServiceId(serviceDbObj.getId());
			serviceVersionInfoDbObj.setPolicyVersion(getNextVersion(serviceDbObj.getPolicyVersion()));
			serviceVersionInfoDbObj.setPolicyUpdateTime(new Date());
			serviceVersionInfoDbObj.setTagVersion(serviceDbObj.getTagVersion());
			serviceVersionInfoDbObj.setTagUpdateTime(serviceDbObj.getTagUpdateTime());

			serviceVersionInfoDao.create(serviceVersionInfoDbObj);
		}

		// if this is a tag service, update all services that refer to this tag service
		// so that next policy-download from plugins will get updated tag policies
		boolean isTagService = serviceDbObj.getType() == EmbeddedServiceDefsUtil.instance().getTagServiceDefId();
		if(isTagService) {
			List<XXService> referringServices = serviceDao.findByTagServiceId(serviceDbObj.getId());

			if(CollectionUtils.isNotEmpty(referringServices)) {
				for(XXService referringService : referringServices) {
					serviceVersionInfoDbObj = serviceVersionInfoDao.findByServiceId(referringService.getId());
					if (serviceVersionInfoDbObj != null) {

						serviceVersionInfoDbObj.setPolicyVersion(getNextVersion(serviceVersionInfoDbObj.getPolicyVersion()));
						serviceVersionInfoDbObj.setPolicyUpdateTime(new Date());

						if (filterForServicePlugin && isTagVersionUpdateNeeded) {
							serviceVersionInfoDbObj.setTagVersion(getNextVersion(serviceVersionInfoDbObj.getTagVersion()));
							serviceVersionInfoDbObj.setTagUpdateTime(new Date());
						}
						serviceVersionInfoDao.update(serviceVersionInfoDbObj);
					} else {
						LOG.warn("updatePolicyVersion(service=" + referringService.getName() + "): serviceVersionInfo not found, creating it..");
						serviceVersionInfoDbObj = new XXServiceVersionInfo();
						serviceVersionInfoDbObj.setServiceId(referringService.getId());
						serviceVersionInfoDbObj.setPolicyVersion(getNextVersion(referringService.getPolicyVersion()));
						serviceVersionInfoDbObj.setPolicyUpdateTime(new Date());
						if (filterForServicePlugin && isTagVersionUpdateNeeded) {
							serviceVersionInfoDbObj.setTagVersion(getNextVersion(referringService.getTagVersion()));
							serviceVersionInfoDbObj.setTagUpdateTime(new Date());
						} else {
							serviceVersionInfoDbObj.setTagVersion(referringService.getTagVersion());
							serviceVersionInfoDbObj.setTagUpdateTime(referringService.getTagUpdateTime());
						}
						serviceVersionInfoDao.create(serviceVersionInfoDbObj);
					}
				}
			}
		}
	}

	private XXPolicyItem createNewPolicyItemForPolicy(RangerPolicy policy, XXPolicy xPolicy, RangerPolicyItem policyItem, XXServiceDef xServiceDef, int itemOrder, int policyItemType) throws Exception {
		XXPolicyItem xPolicyItem = new XXPolicyItem();

		xPolicyItem = rangerAuditFields.populateAuditFields(xPolicyItem, xPolicy);

		xPolicyItem.setDelegateAdmin(policyItem.getDelegateAdmin());
		xPolicyItem.setItemType(policyItemType);
		xPolicyItem.setIsEnabled(Boolean.TRUE);
		xPolicyItem.setComments(null);
		xPolicyItem.setPolicyId(policy.getId());
		xPolicyItem.setOrder(itemOrder);
		xPolicyItem = daoMgr.getXXPolicyItem().create(xPolicyItem);

		List<RangerPolicyItemAccess> accesses = policyItem.getAccesses();
		for (int i = 0; i < accesses.size(); i++) {
			RangerPolicyItemAccess access = accesses.get(i);

			XXAccessTypeDef xAccTypeDef = daoMgr.getXXAccessTypeDef()
					.findByNameAndServiceId(access.getType(),
							xPolicy.getService());
			if (xAccTypeDef == null) {
				throw new Exception(access.getType() + ": is not a valid access-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
			}

			XXPolicyItemAccess xPolItemAcc = new XXPolicyItemAccess();

			xPolItemAcc = (XXPolicyItemAccess) rangerAuditFields.populateAuditFields(xPolItemAcc, xPolicyItem);
			xPolItemAcc.setIsAllowed(access.getIsAllowed());
			xPolItemAcc.setType(xAccTypeDef.getId());
			xPolItemAcc.setPolicyitemid(xPolicyItem.getId());
			xPolItemAcc.setOrder(i);

			daoMgr.getXXPolicyItemAccess().create(xPolItemAcc);
		}

		List<String> users = policyItem.getUsers();
		for(int i = 0; i < users.size(); i++) {
			String user = users.get(i);
			if (StringUtils.isBlank(user)) {
				continue;
			}
			XXUser xUser = daoMgr.getXXUser().findByUserName(user);
			if(xUser == null) {
				throw new Exception(user + ": user does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "' user='" + user +"'");
			}
			XXPolicyItemUserPerm xUserPerm = new XXPolicyItemUserPerm();
			xUserPerm = rangerAuditFields.populateAuditFields(xUserPerm, xPolicyItem);
			xUserPerm.setUserId(xUser.getId());
			xUserPerm.setPolicyItemId(xPolicyItem.getId());
			xUserPerm.setOrder(i);
			xUserPerm = daoMgr.getXXPolicyItemUserPerm().create(xUserPerm);
		}

		List<String> groups = policyItem.getGroups();
		for(int i = 0; i < groups.size(); i++) {
			String group = groups.get(i);
			if (StringUtils.isBlank(group)) {
				continue;
			}
			XXGroup xGrp = daoMgr.getXXGroup().findByGroupName(group);
			if(xGrp == null) {
				throw new Exception(group + ": group does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "' group='" + group + "'");
			}
			XXPolicyItemGroupPerm xGrpPerm = new XXPolicyItemGroupPerm();
			xGrpPerm = rangerAuditFields.populateAuditFields(xGrpPerm, xPolicyItem);
			xGrpPerm.setGroupId(xGrp.getId());
			xGrpPerm.setPolicyItemId(xPolicyItem.getId());
			xGrpPerm.setOrder(i);
			xGrpPerm = daoMgr.getXXPolicyItemGroupPerm().create(xGrpPerm);
		}

		List<RangerPolicyItemCondition> conditions = policyItem.getConditions();
		for(RangerPolicyItemCondition condition : conditions) {
			XXPolicyConditionDef xPolCond = daoMgr
					.getXXPolicyConditionDef().findByServiceDefIdAndName(
							xServiceDef.getId(), condition.getType());

			if(xPolCond == null) {
				throw new Exception(condition.getType() + ": is not a valid condition-type. policy='"+  xPolicy.getName() + "' service='"+ xPolicy.getService() + "'");
			}

			for(int i = 0; i < condition.getValues().size(); i++) {
				String value = condition.getValues().get(i);
				XXPolicyItemCondition xPolItemCond = new XXPolicyItemCondition();
				xPolItemCond = rangerAuditFields.populateAuditFields(xPolItemCond, xPolicyItem);
				xPolItemCond.setPolicyItemId(xPolicyItem.getId());
				xPolItemCond.setType(xPolCond.getId());
				xPolItemCond.setValue(value);
				xPolItemCond.setOrder(i);

				daoMgr.getXXPolicyItemCondition().create(xPolItemCond);
			}
		}

		return xPolicyItem;
	}

	private void createNewPolicyItemsForPolicy(RangerPolicy policy, XXPolicy xPolicy, List<RangerPolicyItem> policyItems, XXServiceDef xServiceDef, int policyItemType) throws Exception {
		if(CollectionUtils.isNotEmpty(policyItems)) {
			for (int itemOrder = 0; itemOrder < policyItems.size(); itemOrder++) {
				RangerPolicyItem policyItem = policyItems.get(itemOrder);
				createNewPolicyItemForPolicy(policy, xPolicy, policyItem, xServiceDef, itemOrder, policyItemType);
			}
		}
	}

	private void createNewDataMaskPolicyItemsForPolicy(RangerPolicy policy, XXPolicy xPolicy, List<RangerDataMaskPolicyItem> policyItems, XXServiceDef xServiceDef, int policyItemType) throws Exception {
		if(CollectionUtils.isNotEmpty(policyItems)) {
			for (int itemOrder = 0; itemOrder < policyItems.size(); itemOrder++) {
				RangerDataMaskPolicyItem policyItem = policyItems.get(itemOrder);

				XXPolicyItem xPolicyItem = createNewPolicyItemForPolicy(policy, xPolicy, policyItem, xServiceDef, itemOrder, policyItemType);

				RangerPolicyItemDataMaskInfo dataMaskInfo = policyItem.getDataMaskInfo();

				if(dataMaskInfo != null) {
					XXDataMaskTypeDef dataMaskDef = daoMgr.getXXDataMaskTypeDef().findByNameAndServiceId(dataMaskInfo.getDataMaskType(), xPolicy.getService());

					if(dataMaskDef == null) {
						throw new Exception(dataMaskInfo.getDataMaskType() + ": is not a valid datamask-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
					}

					XXPolicyItemDataMaskInfo xxDataMaskInfo = new XXPolicyItemDataMaskInfo();

					xxDataMaskInfo.setPolicyItemId(xPolicyItem.getId());
					xxDataMaskInfo.setType(dataMaskDef.getId());
					xxDataMaskInfo.setConditionExpr(dataMaskInfo.getConditionExpr());
					xxDataMaskInfo.setValueExpr(dataMaskInfo.getValueExpr());

					daoMgr.getXXPolicyItemDataMaskInfo().create(xxDataMaskInfo);
				}
			}
		}
	}

	private void createNewRowFilterPolicyItemsForPolicy(RangerPolicy policy, XXPolicy xPolicy, List<RangerRowFilterPolicyItem> policyItems, XXServiceDef xServiceDef, int policyItemType) throws Exception {
		if(CollectionUtils.isNotEmpty(policyItems)) {
			for (int itemOrder = 0; itemOrder < policyItems.size(); itemOrder++) {
				RangerRowFilterPolicyItem policyItem = policyItems.get(itemOrder);

				XXPolicyItem xPolicyItem = createNewPolicyItemForPolicy(policy, xPolicy, policyItem, xServiceDef, itemOrder, policyItemType);

				RangerPolicyItemRowFilterInfo dataMaskInfo = policyItem.getRowFilterInfo();

				if(dataMaskInfo != null) {
					XXPolicyItemRowFilterInfo xxRowFilterInfo = new XXPolicyItemRowFilterInfo();

					xxRowFilterInfo.setPolicyItemId(xPolicyItem.getId());
					xxRowFilterInfo.setFilterExpr(dataMaskInfo.getFilterExpr());

					xxRowFilterInfo = daoMgr.getXXPolicyItemRowFilterInfo().create(xxRowFilterInfo);
				}
			}
		}
	}

	private void createNewResourcesForPolicy(RangerPolicy policy, XXPolicy xPolicy, Map<String, RangerPolicyResource> resources) throws Exception {
		
		for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
			RangerPolicyResource policyRes = resource.getValue();

			XXResourceDef xResDef = daoMgr.getXXResourceDef()
					.findByNameAndPolicyId(resource.getKey(), policy.getId());
			if (xResDef == null) {
				throw new Exception(resource.getKey() + ": is not a valid resource-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
			}

			XXPolicyResource xPolRes = new XXPolicyResource();
			xPolRes = rangerAuditFields.populateAuditFields(xPolRes, xPolicy);

			xPolRes.setIsExcludes(policyRes.getIsExcludes());
			xPolRes.setIsRecursive(policyRes.getIsRecursive());
			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResDefId(xResDef.getId());
			xPolRes = daoMgr.getXXPolicyResource().create(xPolRes);

			List<String> values = policyRes.getValues();
			if (CollectionUtils.isNotEmpty(values)) {
				Set<String> uniqueValues = new LinkedHashSet<String>(values);
				int i = 0;
				if (CollectionUtils.isNotEmpty(uniqueValues)) {
					for (String uniqValue : uniqueValues) {
						if (!StringUtils.isEmpty(uniqValue)) {
							XXPolicyResourceMap xPolResMap = new XXPolicyResourceMap();
							xPolResMap = (XXPolicyResourceMap) rangerAuditFields.populateAuditFields(xPolResMap,
									xPolRes);
							xPolResMap.setResourceId(xPolRes.getId());
							xPolResMap.setValue(uniqValue);
							xPolResMap.setOrder(i);
							xPolResMap = daoMgr.getXXPolicyResourceMap().create(xPolResMap);
							i++;

						}
					}
				}
			}
		}
	}

	private Boolean deleteExistingPolicyItems(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}
		
		XXPolicyItemDao policyItemDao = daoMgr.getXXPolicyItem();
		List<XXPolicyItem> policyItems = policyItemDao.findByPolicyId(policy.getId());
		for(XXPolicyItem policyItem : policyItems) {
			Long polItemId = policyItem.getId();
			
			XXPolicyItemConditionDao polCondDao = daoMgr.getXXPolicyItemCondition();
			List<XXPolicyItemCondition> conditions = polCondDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemCondition condition : conditions) {
				polCondDao.remove(condition);
			}
			
			XXPolicyItemGroupPermDao grpPermDao = daoMgr.getXXPolicyItemGroupPerm();
			List<XXPolicyItemGroupPerm> groups = grpPermDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemGroupPerm group : groups) {
				grpPermDao.remove(group);
			}
			
			XXPolicyItemUserPermDao userPermDao = daoMgr.getXXPolicyItemUserPerm();
			List<XXPolicyItemUserPerm> users = userPermDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemUserPerm user : users) {
				userPermDao.remove(user);
			}
			
			XXPolicyItemAccessDao polItemAccDao = daoMgr.getXXPolicyItemAccess();
			List<XXPolicyItemAccess> accesses = polItemAccDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemAccess access : accesses) {
				polItemAccDao.remove(access);
			}

			XXPolicyItemDataMaskInfoDao polItemDataMaskInfoDao = daoMgr.getXXPolicyItemDataMaskInfo();
			List<XXPolicyItemDataMaskInfo> dataMaskInfos = polItemDataMaskInfoDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemDataMaskInfo dataMaskInfo : dataMaskInfos) {
				polItemDataMaskInfoDao.remove(dataMaskInfo);
			}

			XXPolicyItemRowFilterInfoDao polItemRowFilterInfoDao = daoMgr.getXXPolicyItemRowFilterInfo();
			List<XXPolicyItemRowFilterInfo> rowFilterInfos = polItemRowFilterInfoDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemRowFilterInfo rowFilterInfo : rowFilterInfos) {
				polItemRowFilterInfoDao.remove(rowFilterInfo);
			}

			policyItemDao.remove(policyItem);
		}
		return true;
	}

	private Boolean deleteExistingPolicyResources(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}
		
		List<XXPolicyResource> resources = daoMgr.getXXPolicyResource().findByPolicyId(policy.getId());
		
		XXPolicyResourceDao resDao = daoMgr.getXXPolicyResource();
		for(XXPolicyResource resource : resources) {
			List<XXPolicyResourceMap> resMapList = daoMgr.getXXPolicyResourceMap().findByPolicyResId(resource.getId());
			
			XXPolicyResourceMapDao resMapDao = daoMgr.getXXPolicyResourceMap();
			for(XXPolicyResourceMap resMap : resMapList) {
				resMapDao.remove(resMap);
			}
			resDao.remove(resource);
		}
		return true;
	}

	private Boolean deleteExistingPolicyItemsNative(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}
		XXPolicyItemDao policyItemDao = daoMgr.getXXPolicyItem();
		List<XXPolicyItem> policyItems = policyItemDao.findByPolicyId(policy.getId());
		for(XXPolicyItem policyItem : policyItems) {
			Long polItemId = policyItem.getId();
			daoMgr.getXXPolicyItemRowFilterInfo().deletePolicyIDReference("policy_item_id", polItemId);
			daoMgr.getXXPolicyItemDataMaskInfo().deletePolicyIDReference("policy_item_id", polItemId);
			daoMgr.getXXPolicyItemGroupPerm().deletePolicyIDReference("policy_item_id", polItemId);
			daoMgr.getXXPolicyItemUserPerm().deletePolicyIDReference("policy_item_id", polItemId);
			daoMgr.getXXPolicyItemCondition().deletePolicyIDReference("policy_item_id", polItemId);
			daoMgr.getXXPolicyItemAccess().deletePolicyIDReference("policy_item_id", polItemId);
		}
		daoMgr.getXXPolicyItem().deletePolicyIDReference("policy_id", policy.getId());
		return true;
	}

	private Boolean deleteExistingPolicyResourcesNative(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}
		List<XXPolicyResource> resources = daoMgr.getXXPolicyResource().findByPolicyId(policy.getId());
		for(XXPolicyResource resource : resources) {
			daoMgr.getXXPolicyResourceMap().deletePolicyIDReference("resource_id", resource.getId());
			daoMgr.getXXPolicyResource().deletePolicyIDReference("id", resource.getId());
		}
		return true;
	}

	@Override
	public Boolean getPopulateExistingBaseFields() {
		return populateExistingBaseFields;
	}

	@Override
	public void setPopulateExistingBaseFields(Boolean populateExistingBaseFields) {
		this.populateExistingBaseFields = populateExistingBaseFields;
	}

	public RangerPolicy getPolicyFromEventTime(String eventTime, Long policyId) {

		XXDataHist xDataHist = daoMgr.getXXDataHist().findObjByEventTimeClassTypeAndId(eventTime,
				AppConstants.CLASS_TYPE_RANGER_POLICY, policyId);

		if (xDataHist == null) {
			String errMsg = "No policy history found for given time: " + eventTime;
			LOG.error(errMsg);
			throw restErrorUtil.createRESTException(errMsg, MessageEnums.DATA_NOT_FOUND);
		}

		String content = xDataHist.getContent();
		RangerPolicy policy = (RangerPolicy) dataHistService.writeJsonToJavaObject(content, RangerPolicy.class);

		return policy;
	}

	public VXString getPolicyVersionList(Long policyId) {
		List<Integer> versionList = daoMgr.getXXDataHist().getVersionListOfObject(policyId,
				AppConstants.CLASS_TYPE_RANGER_POLICY);

		VXString vXString = new VXString();
		vXString.setValue(StringUtils.join(versionList, ","));

		return vXString;
	}

	public RangerPolicy getPolicyForVersionNumber(Long policyId, int versionNo) {
		XXDataHist xDataHist = daoMgr.getXXDataHist().findObjectByVersionNumber(policyId,
				AppConstants.CLASS_TYPE_RANGER_POLICY, versionNo);

		if (xDataHist == null) {
			throw restErrorUtil.createRESTException("No Policy found for given version.", MessageEnums.DATA_NOT_FOUND);
		}

		String content = xDataHist.getContent();
		RangerPolicy policy = (RangerPolicy) dataHistService.writeJsonToJavaObject(content, RangerPolicy.class);

		return policy;
	}

	void updatePolicySignature(RangerPolicy policy) {
		RangerPolicyResourceSignature policySignature = factory.createPolicyResourceSignature(policy);
		String signature = policySignature.getSignature();
		policy.setResourceSignature(signature);
		if (LOG.isDebugEnabled()) {
			String message = String.format("Setting signature on policy id=%d, name=%s to [%s]", policy.getId(), policy.getName(), signature);
			LOG.debug(message);
		}
	}

	// when a service-def is updated, the updated service-def should be made available to plugins
	//   this is achieved by incrementing policyVersion of all services of this service-def
	protected void updateServicesForServiceDefUpdate(RangerServiceDef serviceDef) throws Exception {
		if(serviceDef == null) {
			return;
		}

		boolean isTagServiceDef = StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);

		XXServiceDao serviceDao = daoMgr.getXXService();
		XXServiceVersionInfoDao serviceVersionInfoDao = daoMgr.getXXServiceVersionInfo();

		List<XXService> services = serviceDao.findByServiceDefId(serviceDef.getId());

		if(CollectionUtils.isNotEmpty(services)) {
			for(XXService service : services) {
				XXServiceVersionInfo serviceVersionInfo = serviceVersionInfoDao.findByServiceId(service.getId());
				if (serviceVersionInfo != null) {
					serviceVersionInfo.setPolicyVersion(getNextVersion(serviceVersionInfo.getPolicyVersion()));
					serviceVersionInfo.setPolicyUpdateTime(serviceDef.getUpdateTime());

					serviceVersionInfoDao.update(serviceVersionInfo);
				} else {
					LOG.warn("updateServicesForServiceDefUpdate(service=" + service.getName() + "): serviceVersionInfo not found, creating it..");
					serviceVersionInfo = new XXServiceVersionInfo();
					serviceVersionInfo.setServiceId(service.getId());
					serviceVersionInfo.setPolicyVersion(getNextVersion(service.getPolicyVersion()));
					serviceVersionInfo.setTagVersion(service.getTagVersion());
					serviceVersionInfo.setPolicyUpdateTime(new Date());
					serviceVersionInfo.setTagUpdateTime(service.getTagUpdateTime());

					serviceVersionInfoDao.create(serviceVersionInfo);
				}

				if(isTagServiceDef) {
					List<XXService> referrringServices = serviceDao.findByTagServiceId(service.getId());

					if(CollectionUtils.isNotEmpty(referrringServices)) {
						for(XXService referringService : referrringServices) {
							serviceVersionInfo = serviceVersionInfoDao.findByServiceId(referringService.getId());
							if (serviceVersionInfo != null) {
								serviceVersionInfo.setPolicyVersion(getNextVersion(serviceVersionInfo.getPolicyVersion()));
								serviceVersionInfo.setPolicyUpdateTime(serviceDef.getUpdateTime());

								serviceVersionInfoDao.update(serviceVersionInfo);
							} else {
								LOG.warn("updateServicesForServiceDefUpdate(service=" + referringService.getName() + "): serviceVersionInfo not found, creating it..");
								serviceVersionInfo = new XXServiceVersionInfo();
								serviceVersionInfo.setServiceId(referringService.getId());
								serviceVersionInfo.setPolicyVersion(getNextVersion(referringService.getPolicyVersion()));
								serviceVersionInfo.setTagVersion(referringService.getTagVersion());
								serviceVersionInfo.setPolicyUpdateTime(new Date());
								serviceVersionInfo.setTagUpdateTime(referringService.getTagUpdateTime());

								serviceVersionInfoDao.create(serviceVersionInfo);
							}
						}
					}
				}
			}
		}
	}


	private String getServiceName(Long serviceId) {
		String ret = null;

		if(serviceId != null) {
			XXService service = daoMgr.getXXService().getById(serviceId);

			if(service != null) {
				ret = service.getName();
			}
		}

		return ret;
	}

	private boolean isAccessTypeInList(String accessType, List<XXAccessTypeDef> xAccessTypeDefs) {
		for(XXAccessTypeDef xxAccessTypeDef : xAccessTypeDefs) {
			if(StringUtils.equals(xxAccessTypeDef.getName(), accessType)) {
				return true;
			}
		}

		return false;
	}

	private boolean isResourceInList(String resource, List<XXResourceDef> xResourceDefs) {
		for(XXResourceDef xResourceDef : xResourceDefs) {
			if(StringUtils.equals(xResourceDef.getName(), resource)) {
				return true;
			}
		}

		return false;
	}

	private void writeExcel(List<RangerPolicy> policies, String excelFileName, HttpServletResponse response)
			throws IOException {
		Workbook workbook = null;
		OutputStream outStream = null;
		try {
			workbook = new HSSFWorkbook();
			Sheet sheet = workbook.createSheet();
			createHeaderRow(sheet);
			int rowCount = 0;
			if (!CollectionUtils.isEmpty(policies)) {
				for (RangerPolicy policy : policies) {
					long serviceType = daoMgr.getXXService().findByName(policy.getService()).getType();
					List<RangerPolicyItem> policyItems = policy.getPolicyItems();
					List<RangerRowFilterPolicyItem> rowFilterPolicyItems = policy.getRowFilterPolicyItems();
					List<RangerDataMaskPolicyItem> dataMaskPolicyItems = policy.getDataMaskPolicyItems();
					List<RangerPolicyItem> allowExceptions = policy.getAllowExceptions();
					List<RangerPolicyItem> denyExceptions = policy.getDenyExceptions();
					List<RangerPolicyItem> denyPolicyItems = policy.getDenyPolicyItems();

					if (CollectionUtils.isNotEmpty(policyItems)) {
						for (RangerPolicyItem policyItem : policyItems) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, policyItem, null, null, row, null);
						}
					} else if (CollectionUtils.isNotEmpty(dataMaskPolicyItems)) {
						for (RangerDataMaskPolicyItem dataMaskPolicyItem : dataMaskPolicyItems) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, null, dataMaskPolicyItem, null, row, null);
						}
					} else if (CollectionUtils.isNotEmpty(rowFilterPolicyItems)) {
						for (RangerRowFilterPolicyItem rowFilterPolicyItem : rowFilterPolicyItems) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, null, null, rowFilterPolicyItem, row, null);
						}
					} else if (serviceType == 100) {
						if (CollectionUtils.isEmpty(policyItems)) {
							Row row = sheet.createRow(++rowCount);
							RangerPolicyItem policyItem = new RangerPolicyItem();
							writeBookForPolicyItems(policy, policyItem, null, null, row, null);
						}
					} else if (CollectionUtils.isEmpty(policyItems)) {
						Row row = sheet.createRow(++rowCount);
						RangerPolicyItem policyItem = new RangerPolicyItem();
						writeBookForPolicyItems(policy, policyItem, null, null, row, null);
					}
					if (CollectionUtils.isNotEmpty(allowExceptions)) {
						for (RangerPolicyItem policyItem : allowExceptions) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, policyItem, null, null, row, POLICY_ALLOW_EXCLUDE);
						}
					}
					if (CollectionUtils.isNotEmpty(denyExceptions)) {
						for (RangerPolicyItem policyItem : denyExceptions) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, policyItem, null, null, row, POLICY_DENY_EXCLUDE);
						}
					}
					if (CollectionUtils.isNotEmpty(denyPolicyItems)) {
						for (RangerPolicyItem policyItem : denyPolicyItems) {
							Row row = sheet.createRow(++rowCount);
							writeBookForPolicyItems(policy, policyItem, null, null, row, POLICY_DENY_INCLUDE);
						}
					}	
				}
			}
			ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
			workbook.write(outByteStream);
			byte[] outArray = outByteStream.toByteArray();
			response.setContentType("application/ms-excel");
			response.setContentLength(outArray.length);
			response.setHeader("Expires:", "0");
			response.setHeader("Content-Disposition", "attachment; filename=" + excelFileName);
			response.setStatus(HttpServletResponse.SC_OK);
			outStream = response.getOutputStream();
			outStream.write(outArray);
			outStream.flush();
		} catch (IOException ex) {
			LOG.error("Failed to create report file " + excelFileName, ex);
		} catch (Exception ex) {
			LOG.error("Error while generating report file " + excelFileName, ex);
		} finally {
			if (outStream != null) {
				outStream.close();
			}
			if (workbook != null) {
				workbook.close();
			}
		}
	}

	private StringBuilder writeCSV(List<RangerPolicy> policies, String cSVFileName, HttpServletResponse response) {
		response.setContentType("text/csv");
		final String COMMA_DELIMITER = "|";
		final String LINE_SEPARATOR = "\n";
		final String FILE_HEADER = "ID|Name|Resources|Groups|Users|Accesses|Service Type|Status";
		StringBuilder csvBuffer = new StringBuilder();
		csvBuffer.append(FILE_HEADER);
		csvBuffer.append(LINE_SEPARATOR);
		for (RangerPolicy policy : policies) {
			String policyStatus = "";
			String policyName = "";
			String ServiceType = "";
			Long serviceTypeId = null;
			List<String> groups = new ArrayList<String>();
			List<String> users = new ArrayList<String>();
			List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
			String groupNames = "";
			String userNames = "";
			String accessType = "";
			String resValue = "";
			String resourceKeyVal = "";
			StringBuffer resKeyVal = new StringBuffer();
			String resKey = "";
			policyName = policy.getName();
			policyName=policyName.replace("|", "");
			Long policyId = policy.getId();

			if (policy.getIsEnabled()) {
				policyStatus = "Enabled";
			} else {
				policyStatus = "Disabled";
			}
			XXService xxservice = daoMgr.getXXService().findByName(policy.getService());

			if (xxservice != null) {
				serviceTypeId = xxservice.getType();
				XXServiceDef xxservDef = daoMgr.getXXServiceDef().getById(serviceTypeId);
				if (xxservDef != null) {
					ServiceType = xxservDef.getName();
				}
			}
			int policyType = policy.getPolicyType();
			List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
			List<RangerPolicyItem> policyItems0 = new ArrayList<RangerPolicyItem>();
			List<RangerDataMaskPolicyItem> policyItems1 = new ArrayList<RangerDataMaskPolicyItem>();
			List<RangerRowFilterPolicyItem> policyItems2 = new ArrayList<RangerRowFilterPolicyItem>();
			switch (policyType) {
			case 0:
				policyItems0 = policy.getPolicyItems();
				policyItems.addAll(policyItems0);
				if (CollectionUtils.isNotEmpty(policy.getAllowExceptions())){
					policyItems.addAll(policy.getAllowExceptions());
				}
				if (CollectionUtils.isNotEmpty(policy.getDenyExceptions())){
					policyItems.addAll(policy.getDenyExceptions());
				}
				if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())){
					policyItems.addAll(policy.getDenyPolicyItems());
				}
				break;
			case 1:
				policyItems1 = policy.getDataMaskPolicyItems();
				policyItems.addAll(policyItems1);
				break;
			case 2:
				policyItems2 = policy.getRowFilterPolicyItems();
				policyItems.addAll(policyItems2);
				break;
			}

			if (serviceTypeId!=null && serviceTypeId.equals(Long.valueOf(100L))) {
				Map<String, RangerPolicyResource> resources = policy.getResources();

				if (resources != null) {
					for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
						resKey = resource.getKey();
						RangerPolicyResource policyResource = resource.getValue();
						List<String> resvalueList = policyResource.getValues();
						resValue = resvalueList.toString();
						resKeyVal = resKeyVal.append(resourceKeyVal).append(" ").append(resKey).append("=").append(resValue);
					}
				}
				resourceKeyVal = resKeyVal.toString();
				resourceKeyVal = resourceKeyVal.substring(1);

				if (!CollectionUtils.isEmpty(policyItems)) {
					for (RangerPolicyItem policyItem : policyItems) {
						groupNames = "";
						userNames = "";
						accessType = "";
						groups = null;
						users = null;
						accesses = null;
						groups = policyItem.getGroups();
						accesses = policyItem.getAccesses();
						users = policyItem.getUsers();

						for (RangerPolicyItemAccess access : accesses) {
							accessType = accessType + access.getType().replace("#", "").replace("|","") + "#";
						}
						accessType = accessType.substring(0, accessType.lastIndexOf("#"));
						if (CollectionUtils.isNotEmpty(groups)) {
							for (String group : groups){
								group=group.replace("|", "");
								group=group.replace("#", "");
								groupNames=groupNames+group+ "#";
							}
							groupNames = groupNames.substring(0, groupNames.lastIndexOf("#"));
						}

						if (CollectionUtils.isNotEmpty(users)) {
							for (String user : users){
								user=user.replace("|", "");
								user=user.replace("#", "");
								userNames=userNames +user + "#";
							}
							userNames=userNames.substring(0,userNames.lastIndexOf("#"));
						}

						csvBuffer.append(policyId);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(policyName);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(resourceKeyVal);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(groupNames);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(userNames);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(accessType);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(ServiceType);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(policyStatus);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(LINE_SEPARATOR);

					}
				} else {
					csvBuffer.append(policyId);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(policyName);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(resourceKeyVal);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(groupNames);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(userNames);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(accessType);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(ServiceType);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(policyStatus);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(LINE_SEPARATOR);
				}
			} else {
				Map<String, RangerPolicyResource> resources = policy.getResources();
				if (resources != null) {
					for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
						resKey = resource.getKey();
						RangerPolicyResource policyResource = resource.getValue();
						List<String> resvalueList = policyResource.getValues();
						resValue = resvalueList.toString();
						resKeyVal = resKeyVal.append(resourceKeyVal).append(" ").append(resKey).append("=").append(resValue);
					}
				}
				resourceKeyVal = resKeyVal.toString();
				resourceKeyVal = resourceKeyVal.substring(1);

				if (CollectionUtils.isNotEmpty(policyItems)) {
					for (RangerPolicyItem policyItem : policyItems) {
						groups = null;
						users = null;
						accesses = null;
						groupNames = "";
						userNames = "";
						accessType = "";
						groups = policyItem.getGroups();
						users = policyItem.getUsers();
						accesses = policyItem.getAccesses();

						if (CollectionUtils.isNotEmpty(accesses)) {
							for (RangerPolicyItemAccess access : accesses) {
								accessType = accessType + access.getType().replace("#", "").replace("|", "") + "#";
							}
							accessType = accessType.substring(0, accessType.lastIndexOf("#"));
						}
						if (CollectionUtils.isNotEmpty(groups)) {
							for (String group : groups) {
								group = group.replace("|", "");
								group = group.replace("#", "");
								groupNames = groupNames + group + "#";
							}
							groupNames = groupNames.substring(0, groupNames.lastIndexOf("#"));
						}
						if (CollectionUtils.isNotEmpty(users)) {
							for (String user : users) {
								user = user.replace("|", "");
								user = user.replace("#", "");
								userNames = userNames + user + "#";
							}
							userNames = userNames.substring(0, userNames.lastIndexOf("#"));
						}
						csvBuffer.append(policyId);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(policyName);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(resourceKeyVal);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(groupNames);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(userNames);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(accessType);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(ServiceType);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(policyStatus);
						csvBuffer.append(COMMA_DELIMITER);
						csvBuffer.append(LINE_SEPARATOR);
					}
				} else {
					csvBuffer.append(policyId);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(policyName);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(resourceKeyVal);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(groupNames);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(userNames);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(accessType);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(ServiceType);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(policyStatus);
					csvBuffer.append(COMMA_DELIMITER);
					csvBuffer.append(LINE_SEPARATOR);
				}
			}
		}
		response.setHeader("Content-Disposition", "attachment; filename=" + cSVFileName);
		response.setStatus(HttpServletResponse.SC_OK);
		return csvBuffer;
	}
	
	public void putMetaDataInfo(RangerExportPolicyList rangerExportPolicyList){
		Map<String, Object> metaDataInfo = new LinkedHashMap<String, Object>();
		UserSessionBase usb = ContextUtil.getCurrentUserSession();
		String userId = usb.getLoginId();
		
		metaDataInfo.put(HOSTNAME, LOCAL_HOSTNAME);
		metaDataInfo.put(USER_NAME, userId);
		metaDataInfo.put(TIMESTAMP, MiscUtil.getUTCDateForLocalDate(new Date()));
		metaDataInfo.put(RANGER_VERSION, RangerVersionInfo.getVersion());
		
		rangerExportPolicyList.setMetaDataInfo(metaDataInfo);
	}
	
	private void writeJson(List<RangerPolicy> policies, String jsonFileName,
			HttpServletResponse response) throws JSONException, IOException {
		response.setContentType("text/json");
		response.setHeader("Content-Disposition", "attachment; filename="+ jsonFileName);
		ServletOutputStream out = null;
		RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
		putMetaDataInfo(rangerExportPolicyList);
		rangerExportPolicyList.setPolicies(policies);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(rangerExportPolicyList, RangerExportPolicyList.class);
		try {
			out = response.getOutputStream();
			response.setStatus(HttpServletResponse.SC_OK);
			IOUtils.write(json, out, "UTF-8");
		} catch (Exception e) {
			LOG.error("Error while exporting json file " + jsonFileName, e);
		} finally {
			try {
				if (out != null) {
					out.flush();
					out.close();
				}
			} catch (Exception ex) {
			}
		}
	}
	
	public Map<String, String> getServiceMap(InputStream serviceMapStream)
			throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceMap()");
		}
		Map<String, String> serviceMap = new LinkedHashMap<String, String>();
		String serviceMapString = IOUtils.toString(serviceMapStream);
		if (StringUtils.isNotEmpty(serviceMapString)) {
			serviceMap = jsonUtil.jsonToMap(serviceMapString);
		}
		if(!CollectionUtils.sizeIsEmpty(serviceMap)){
			if (LOG.isDebugEnabled()) {
				LOG.debug("<== ServiceDBStore.getServiceMap()");
			}
			return serviceMap;
		}else{
			LOG.error("Provided service map is empty!!");
			throw restErrorUtil.createRESTException("Provided service map is empty!!");
		}
	}
	
	public Map<String, RangerPolicy> setPolicyMapKeyValue(Map<String, RangerPolicy> policiesMap, RangerPolicy policy){
		if (StringUtils.isNotEmpty(policy.getName().trim())
				&& StringUtils.isNotEmpty(policy.getService().trim())
				&& StringUtils.isNotEmpty(policy.getResources().toString().trim())) {
			policiesMap.put(policy.getName().trim() + " " + policy.getService().trim() + " " + policy.getResources().toString().trim(), policy);
		}else if (StringUtils.isEmpty(policy.getName().trim()) && StringUtils.isNotEmpty(policy.getService().trim())){
			LOG.error("Policy Name is not provided for service : " + policy.getService().trim());
			throw restErrorUtil.createRESTException("Policy Name is not provided for service : " + policy.getService().trim());
		}else if (StringUtils.isNotEmpty(policy.getName().trim()) && StringUtils.isEmpty(policy.getService().trim())){
			LOG.error("Service Name is not provided for policy : " + policy.getName().trim());
			throw restErrorUtil.createRESTException("Service Name is not provided for policy : " + policy.getName().trim());
		}else{
			LOG.error("Service Name or Policy Name is not provided!!");
			throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
		}
		return policiesMap;
	}
	
	public Map<String, RangerPolicy> createPolicyMap(
			Map<String, String> servicesMappingMap,
			List<String> sourceServices, List<String> destinationServices,
			RangerPolicy policy, Map<String, RangerPolicy> policiesMap) {
		if (!CollectionUtils.sizeIsEmpty(servicesMappingMap)) {
			if (!StringUtils.isEmpty(policy.getService().trim())){
				if (sourceServices.contains(policy.getService().trim())) {
					int index = sourceServices.indexOf(policy.getService().trim());
					policy.setService(destinationServices.get(index));
					policiesMap = setPolicyMapKeyValue(policiesMap, policy);
				}
			}else{
				LOG.error("Service Name or Policy Name is not provided!!");
				throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
			}
		} else if (CollectionUtils.sizeIsEmpty(servicesMappingMap)) {
			policiesMap = setPolicyMapKeyValue(policiesMap, policy);
		}
		return policiesMap;
	}

	private void writeBookForPolicyItems(RangerPolicy policy, RangerPolicyItem policyItem,
			RangerDataMaskPolicyItem dataMaskPolicyItem, RangerRowFilterPolicyItem rowFilterPolicyItem, Row row, String policyConditonType) {
		if (LOG.isDebugEnabled()) {
			// To avoid PMD violation
			LOG.debug("policyConditonType:[" + policyConditonType + "]");
		}
		List<String> groups = new ArrayList<String>();
		List<String> users = new ArrayList<String>();
		String groupNames = "";
		String userNames = "";
		String accessType = "";
		String policyStatus = "";
		Cell cell = row.createCell(0);
		cell.setCellValue(policy.getId());
		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
		cell = row.createCell(1);
		cell.setCellValue(policy.getName());
		cell = row.createCell(2);
		String resValue = "";
		String resourceKeyVal = "";
		String resKey = "";
		StringBuffer sb = new StringBuffer();
		Map<String, RangerPolicyResource> resources = policy.getResources();
		if (resources != null) {
			for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
				resKey = resource.getKey();
				RangerPolicyResource policyResource = resource.getValue();
				List<String> resvalueList = policyResource.getValues();
				resValue = resvalueList.toString();
				sb = sb.append(resourceKeyVal).append(" ").append(resKey).append("=").append(resValue);
			}
			resourceKeyVal = sb.toString();
			resourceKeyVal = resourceKeyVal.substring(1);
			cell.setCellValue(resourceKeyVal);
			if (policyItem != null && dataMaskPolicyItem == null && rowFilterPolicyItem == null) {
				groups = policyItem.getGroups();
				users = policyItem.getUsers();
				accesses = policyItem.getAccesses();
			} else if (dataMaskPolicyItem != null && policyItem == null && rowFilterPolicyItem == null) {
				groups = dataMaskPolicyItem.getGroups();
				users = dataMaskPolicyItem.getUsers();
				accesses = dataMaskPolicyItem.getAccesses();
			} else if (rowFilterPolicyItem != null && policyItem == null && dataMaskPolicyItem == null) {
				groups = rowFilterPolicyItem.getGroups();
				users = rowFilterPolicyItem.getUsers();
				accesses = rowFilterPolicyItem.getAccesses();
			}
			if (CollectionUtils.isNotEmpty(accesses)) {
				for (RangerPolicyItemAccess access : accesses) {
					accessType = accessType + access.getType();
					accessType = accessType + " ,";
				}
				accessType = accessType.substring(0, accessType.lastIndexOf(","));
			}
			if (CollectionUtils.isNotEmpty(groups)) {
				groupNames = groupNames + groups.toString();
				StringTokenizer groupToken = new StringTokenizer(groupNames, "[]");
				groupNames = groupToken.nextToken().toString();
			}
			if (CollectionUtils.isNotEmpty(users)) {
				userNames = userNames + users.toString();
				StringTokenizer userToken = new StringTokenizer(userNames, "[]");
				userNames = userToken.nextToken().toString();
			}
			cell = row.createCell(3);
			cell.setCellValue(groupNames);
			cell = row.createCell(4);
			cell.setCellValue(userNames);
			cell = row.createCell(5);
			cell.setCellValue(accessType.trim());
			cell = row.createCell(6);
			XXService xxservice = daoMgr.getXXService().findByName(policy.getService());
			String ServiceType = "";
			if (xxservice != null) {
				Long ServiceId = xxservice.getType();
				XXServiceDef xxservDef = daoMgr.getXXServiceDef().getById(ServiceId);
				if (xxservDef != null) {
					ServiceType = xxservDef.getName();
				}
			}
			cell.setCellValue(ServiceType);
			cell = row.createCell(7);

		}
		if (policy.getIsEnabled()) {
			policyStatus = "Enabled";
		} else {
			policyStatus = "Disabled";
		}
		cell.setCellValue(policyStatus);
	}

	private void createHeaderRow(Sheet sheet) {
		CellStyle cellStyle = sheet.getWorkbook().createCellStyle();
		Font font = sheet.getWorkbook().createFont();
		font.setBold(true);
		font.setFontHeightInPoints((short) 12);
		cellStyle.setFont(font);

		Row row = sheet.createRow(0);

		Cell cellID = row.createCell(0);
		cellID.setCellStyle(cellStyle);
		cellID.setCellValue("ID");

		Cell cellNAME = row.createCell(1);
		cellNAME.setCellStyle(cellStyle);
		cellNAME.setCellValue("Name");

		Cell cellResources = row.createCell(2);
		cellResources.setCellStyle(cellStyle);
		cellResources.setCellValue("Resources");

		Cell cellGroups = row.createCell(3);
		cellGroups.setCellStyle(cellStyle);
		cellGroups.setCellValue("Groups");

		Cell cellUsers = row.createCell(4);
		cellUsers.setCellStyle(cellStyle);
		cellUsers.setCellValue("Users");

		Cell cellAccesses = row.createCell(5);
		cellAccesses.setCellStyle(cellStyle);
		cellAccesses.setCellValue("Accesses");

		Cell cellServiceType = row.createCell(6);
		cellServiceType.setCellStyle(cellStyle);
		cellServiceType.setCellValue("Service Type");

		Cell cellStatus = row.createCell(7);
		cellStatus.setCellStyle(cellStyle);
		cellStatus.setCellValue("Status");
	}


	private RangerPolicyList searchRangerPolicies(SearchFilter searchFilter) {
		List<RangerPolicy> policyList = new ArrayList<RangerPolicy>();
		RangerPolicyList retList = new RangerPolicyList();
		Map<Long,RangerPolicy> policyMap=new HashMap<Long,RangerPolicy>();
		Set<Long> processedServices=new HashSet<Long>();
		Set<Long> processedPolicies=new HashSet<Long>();
		Comparator<RangerPolicy> comparator = new Comparator<RangerPolicy>() {
			public int compare(RangerPolicy c1, RangerPolicy c2) {
				return (int) ((c1.getId()).compareTo(c2.getId()));
			}
		};

		List<XXPolicy> xPolList = (List<XXPolicy>) policyService.searchResources(searchFilter, policyService.searchFields, policyService.sortFields, retList);
		if (!CollectionUtils.isEmpty(xPolList)) {
			for (XXPolicy xXPolicy : xPolList) {
				if(!processedServices.contains(xXPolicy.getService())){
					loadRangerPolicies(xXPolicy.getService(),processedServices,policyMap,searchFilter);
				}
			}
		}
		String userName = searchFilter.getParam("user");
		if (!StringUtils.isEmpty(userName)) {
			searchFilter.removeParam("user");
			Set<String> groupNames = daoMgr.getXXGroupUser().findGroupNamesByUserName(userName);
			if (!CollectionUtils.isEmpty(groupNames)) {
				List<XXPolicy> xPolList2 = null;
				for (String groupName : groupNames) {
					xPolList2 = new ArrayList<XXPolicy>();
					searchFilter.setParam("group", groupName);
					xPolList2 = (List<XXPolicy>) policyService.searchResources(searchFilter, policyService.searchFields, policyService.sortFields, retList);
					if (!CollectionUtils.isEmpty(xPolList2)) {
						for (XXPolicy xPol2 : xPolList2) {
							if(xPol2!=null){
								if(!processedPolicies.contains(xPol2.getId())){
									if(!processedServices.contains(xPol2.getService())){
										loadRangerPolicies(xPol2.getService(),processedServices,policyMap,searchFilter);
									}
									if(policyMap.containsKey(xPol2.getId())){
										policyList.add(policyMap.get(xPol2.getId()));
										processedPolicies.add(xPol2.getId());
									}
								}
							}
						}
					}
				}
			}
		}
		if (!CollectionUtils.isEmpty(xPolList)) {
			for (XXPolicy xPol : xPolList) {
				if(xPol!=null){
					if(!processedPolicies.contains(xPol.getId())){
						if(!processedServices.contains(xPol.getService())){
							loadRangerPolicies(xPol.getService(),processedServices,policyMap,searchFilter);
						}
						if(policyMap.containsKey(xPol.getId())){
							policyList.add(policyMap.get(xPol.getId()));
							processedPolicies.add(xPol.getId());
						}
					}
				}
			}
			Collections.sort(policyList, comparator);
		}
		retList.setPolicies(policyList);
		return retList;
	}

	private void loadRangerPolicies(Long serviceId,Set<Long> processedServices,Map<Long,RangerPolicy> policyMap,SearchFilter searchFilter){
		try {
			List<RangerPolicy> tempPolicyList = getServicePolicies(serviceId,searchFilter);
			if(!CollectionUtils.isEmpty(tempPolicyList)){
				for (RangerPolicy rangerPolicy : tempPolicyList) {
					if(!policyMap.containsKey(rangerPolicy.getId())){
						policyMap.put(rangerPolicy.getId(), rangerPolicy);
					}
				}
				processedServices.add(serviceId);
			}
		} catch (Exception e) {
		}
	}
	public void getServiceUpgraded(){
		LOG.info("==> ServiceDBStore.getServiceUpgraded()");
		updateServiceWithCustomProperty();
		LOG.info("<== ServiceDBStore.getServiceUpgraded()");
	}
	private void updateServiceWithCustomProperty() {		
			LOG.info("Adding custom properties to services");
			SearchFilter filter = new SearchFilter();
			try {
				List<RangerService> lstRangerService = getServices(filter);
				for(RangerService rangerService : lstRangerService){
					String serviceUser = PropertiesUtil.getProperty("ranger.plugins."+rangerService.getType()+".serviceuser");
					if(!StringUtils.isEmpty(serviceUser)){
						boolean chkServiceUpdate = false;
						LOG.debug("customproperty = " + rangerService.getConfigs().get(ServiceREST.Allowed_User_List_For_Download) + " for service = " + rangerService.getName());
						if(!rangerService.getConfigs().containsKey(ServiceREST.Allowed_User_List_For_Download)){
							rangerService.getConfigs().put(ServiceREST.Allowed_User_List_For_Download, serviceUser);
							chkServiceUpdate = true;
		                }
		                if((!rangerService.getConfigs().containsKey(ServiceREST.Allowed_User_List_For_Grant_Revoke)) && ("hbase".equalsIgnoreCase(rangerService.getType()) || "hive".equalsIgnoreCase(rangerService.getType()))){
							rangerService.getConfigs().put(ServiceREST.Allowed_User_List_For_Grant_Revoke, serviceUser);
							chkServiceUpdate = true;
		                }
		                if(!rangerService.getConfigs().containsKey(TagREST.Allowed_User_List_For_Tag_Download)){
							rangerService.getConfigs().put(TagREST.Allowed_User_List_For_Tag_Download, serviceUser);
							chkServiceUpdate = true;
		                }
		                if(chkServiceUpdate){
		                	updateService(rangerService, null);
							if(LOG.isDebugEnabled()){
								LOG.debug("Updated service "+rangerService.getName()+" with custom properties in secure environment");
							}
		                }
					}
				}
			} catch (Throwable e) {
				LOG.fatal("updateServiceWithCustomProperty failed with exception : "+e.getMessage());
			}
	}

	private String  getAuditMode(String serviceTypeName, String serviceName) {
		RangerConfiguration config = RangerConfiguration.getInstance();
		String ret = config.get("ranger.audit.global.mode");
		if (StringUtils.isNotBlank(ret)) {
			return ret;
		}
		ret = config.get("ranger.audit.servicedef." + serviceTypeName + ".mode");
		if (StringUtils.isNotBlank(ret)) {
			return ret;
		}
		ret = config.get("ranger.audit.service." + serviceName + ".mode");
		if (StringUtils.isNotBlank(ret)) {
			return ret;
		}
		return RangerPolicyEngine.AUDIT_DEFAULT;
	}

	private void createGenericUsers() {
		VXUser genericUser = new VXUser();

		genericUser.setName(RangerPolicyEngine.USER_CURRENT);
		genericUser.setDescription(RangerPolicyEngine.USER_CURRENT);
		xUserService.createXUserWithOutLogin(genericUser);

		genericUser.setName(RangerPolicyEngine.RESOURCE_OWNER);
		genericUser.setDescription(RangerPolicyEngine.RESOURCE_OWNER);
		xUserService.createXUserWithOutLogin(genericUser);
	}
}
