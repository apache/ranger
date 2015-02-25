package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXAccessTypeDefGrantsDao;
import org.apache.ranger.db.XXContextEnricherDefDao;
import org.apache.ranger.db.XXEnumDefDao;
import org.apache.ranger.db.XXEnumElementDefDao;
import org.apache.ranger.db.XXPolicyConditionDefDao;
import org.apache.ranger.db.XXPolicyItemAccessDao;
import org.apache.ranger.db.XXPolicyItemConditionDao;
import org.apache.ranger.db.XXPolicyItemDao;
import org.apache.ranger.db.XXPolicyItemGroupPermDao;
import org.apache.ranger.db.XXPolicyItemUserPermDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.db.XXPolicyResourceMapDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceConfigDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXAccessTypeDefGrants;
import org.apache.ranger.entity.XXContextEnricherDef;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXEnumElementDef;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyItemGroupPerm;
import org.apache.ranger.entity.XXPolicyItemUserPerm;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.apache.ranger.plugin.util.SearchFilter;


@Component
public class ServiceDBStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceDBStore.class);

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
	RangerAuditFields<XXDBBase> rangerAuditFields;
	
	@Autowired
	RangerPolicyService policyService;
	
	@Autowired
	XUserService xUserService;
	
	@Autowired
	XUserMgr xUserMgr;
	
	@Autowired
	RangerDataHistService dataHistService;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

	private static volatile boolean legacyServiceDefsInitDone = false;
	
	@Override
	public void init() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.init()");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.init()");
		}
	}

	@PostConstruct
	public void initStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.initStore()");
		}

		if(! legacyServiceDefsInitDone) {
			synchronized(ServiceDBStore.class) {
				if(!legacyServiceDefsInitDone) {
					TransactionTemplate txTemplate = new TransactionTemplate(txManager);

					final ServiceDBStore dbStore = this;

					txTemplate.execute(new TransactionCallback<Object>() {
						@Override
	                    public Object doInTransaction(TransactionStatus status) {
							EmbeddedServiceDefsUtil.instance().init(dbStore);

							return null;
	                    }
					});

					legacyServiceDefsInitDone = true;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.initStore()");
		}
	}

	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.createServiceDef(" + serviceDef + ")");
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

		// following fields will be auto populated
		serviceDef.setId(null);
		serviceDef.setCreateTime(null);
		serviceDef.setUpdateTime(null);
		
		serviceDef = serviceDefService.create(serviceDef);
		Long serviceDefId = serviceDef.getId();
		XXServiceDef createdSvcDef = daoMgr.getXXServiceDef().getById(serviceDefId);
		
		XXServiceConfigDefDao xxServiceConfigDao = daoMgr.getXXServiceConfigDef();
		for(RangerServiceConfigDef config : configs) {
			XXServiceConfigDef xConfig = new XXServiceConfigDef();
			xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef);
			xConfig = xxServiceConfigDao.create(xConfig);
		}
		
		XXResourceDefDao xxResDefDao = daoMgr.getXXResourceDef();
		for(RangerResourceDef resource : resources) {
			XXResourceDef parent = xxResDefDao.findByNameAndServiceDefId(resource.getParent(), serviceDefId);
			Long parentId = (parent != null) ? parent.getId() : null;
			
			XXResourceDef xResource = new XXResourceDef();
			xResource = serviceDefService.populateRangerResourceDefToXX(resource, xResource, createdSvcDef);
			xResource.setParent(parentId);
			xResource = xxResDefDao.create(xResource);
		}
		
		XXAccessTypeDefDao xxATDDao = daoMgr.getXXAccessTypeDef();
		for(RangerAccessTypeDef accessType : accessTypes) {
			XXAccessTypeDef xAccessType = new XXAccessTypeDef();
			xAccessType = serviceDefService.populateRangerAccessTypeDefToXX(accessType, xAccessType, createdSvcDef);
			xAccessType = xxATDDao.create(xAccessType);
			
			Collection<String> impliedGrants = accessType.getImpliedGrants();
			XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
			for(String impliedGrant : impliedGrants) {
				XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
				xImpliedGrant.setAtdid(xAccessType.getId());
				xImpliedGrant.setImpliedgrant(impliedGrant);
				xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
			}
		}
		
		XXPolicyConditionDefDao xxPolCondDao = daoMgr.getXXPolicyConditionDef();
		for (RangerPolicyConditionDef policyCondition : policyConditions) {
			XXPolicyConditionDef xPolicyCondition = new XXPolicyConditionDef();
			xPolicyCondition = serviceDefService
					.populateRangerPolicyConditionDefToXX(policyCondition,
							xPolicyCondition, createdSvcDef);
			xPolicyCondition = xxPolCondDao.create(xPolicyCondition);
		}
		
		XXContextEnricherDefDao xxContextEnricherDao = daoMgr.getXXContextEnricherDef();
		for (RangerContextEnricherDef contextEnricher : contextEnrichers) {
			XXContextEnricherDef xContextEnricher = new XXContextEnricherDef();
			xContextEnricher = serviceDefService
					.populateRangerContextEnricherDefToXX(contextEnricher,
							xContextEnricher, createdSvcDef);
			xContextEnricher = xxContextEnricherDao.create(xContextEnricher);
		}
		
		XXEnumDefDao xxEnumDefDao = daoMgr.getXXEnumDef();
		for(RangerEnumDef vEnum : enums) {
			XXEnumDef xEnum = new XXEnumDef();
			xEnum = serviceDefService.populateRangerEnumDefToXX(vEnum, xEnum, createdSvcDef);
			xEnum = xxEnumDefDao.create(xEnum);
			
			List<RangerEnumElementDef> elements = vEnum.getElements();
			XXEnumElementDefDao xxEnumEleDefDao = daoMgr.getXXEnumElementDef();
			for(RangerEnumElementDef element : elements) {
				XXEnumElementDef xElement = new XXEnumElementDef();
				xElement = serviceDefService.populateRangerEnumElementDefToXX(element, xElement, xEnum);
				xElement = xxEnumEleDefDao.create(xElement);
			}
		}
		RangerServiceDef createdServiceDef = serviceDefService.getPopulatedViewObject(createdSvcDef);
		dataHistService.createObjectDataHistory(createdServiceDef, RangerDataHistService.ACTION_CREATE);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.createServiceDef(" + serviceDef + "): " + createdServiceDef);
		}

		return createdServiceDef;
	}

	@Override
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef)
			throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		// TODO: updateServiceDef()

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteServiceDef(Long servceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.deleteServiceDef(" + servceId + ")");
		}

		// TODO: deleteServiceDef()

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.deleteServiceDef(" + servceId + ")");
		}
	}

	@Override
	public RangerServiceDef getServiceDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.getServiceDef(" + id + ")");
		}
		
		RangerServiceDef ret = null;

		ret = serviceDefService.read(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getServiceDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.getServiceDefByName(" + name + ")");
		}
		
		RangerServiceDef ret = null;
		
		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(name);

		if(xServiceDef != null) {
			ret = serviceDefService.getPopulatedViewObject(xServiceDef);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("== ServiceDefDBStore.getServiceDefByName(" + name + "): " + ret);
		}

		return  ret;
	}

	@Override
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + ")");
		}

		List<RangerServiceDef> ret = null;

		ret = serviceDefService.getServiceDefs(filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService createService(RangerService service) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.createService(" + service + ")");
		}
		
		UserSessionBase usb = ContextUtil.getCurrentUserSession();
		if (usb != null && usb.isUserAdmin()) {
			Map<String, String> configs = service.getConfigs();
			Map<String, String> validConfigs = validateRequiredConfigParams(
					service, configs);
			if (validConfigs == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("==> ConfigParams cannot be null, ServiceDefDBStore.createService(" + service + ")");
				}
				throw restErrorUtil.createRESTException(
						"ConfigParams cannot be null.",
						MessageEnums.ERROR_CREATING_OBJECT);
			}

			service = svcService.create(service);
			XXService xCreatedService = daoMgr.getXXService().getById(service.getId());
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
						vXUser = new VXUser();
						vXUser.setName(userName);
						vXUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
						vXUser = xUserMgr.createXUser(vXUser);
					}
				}

				XXServiceConfigMap xConfMap = new XXServiceConfigMap();
				xConfMap = (XXServiceConfigMap) rangerAuditFields.populateAuditFields(xConfMap, xCreatedService);
				xConfMap.setServiceId(xCreatedService.getId());
				xConfMap.setConfigkey(configKey);
				xConfMap.setConfigvalue(configValue);
				xConfMap = xConfMapDao.create(xConfMap);
			}
			RangerService createdService = svcService.getPopulatedViewObject(xCreatedService);
			dataHistService.createObjectDataHistory(createdService, RangerDataHistService.ACTION_CREATE);
			
			createDefaultPolicy(xCreatedService, vXUser);
			
			return createdService;
		} else {
			LOG.debug("User id : " + usb.getUserId() + " doesn't have admin access to create repository.");
			throw restErrorUtil.createRESTException(
							"Sorry, you don't have permission to perform the operation",
							MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);

		}
	}

	@Override
	public RangerService updateService(RangerService service) throws Exception {
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

			if(newNameService != null) {
				throw restErrorUtil.createRESTException("another service already exists with name '"
						+ service.getName() + "'. ID=" + newNameService.getId(), MessageEnums.DATA_NOT_UPDATABLE);
			}
		}
		
		Map<String, String> configs = service.getConfigs();
		Map<String, String> validConfigs = validateRequiredConfigParams(
				service, configs);
		if (validConfigs == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConfigParams cannot be null, ServiceDefDBStore.createService(" + service + ")");
			}
			throw restErrorUtil.createRESTException(
					"ConfigParams cannot be null.",
					MessageEnums.ERROR_CREATING_OBJECT);
		}
		service = svcService.update(service);
		XXService xUpdService = daoMgr.getXXService().getById(service.getId());
		
		List<XXServiceConfigMap> dbConfigMaps = daoMgr.getXXServiceConfigMap().findByServiceId(service.getId());
		for(XXServiceConfigMap dbConfigMap : dbConfigMaps) {
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
					vXUser = new VXUser();
					vXUser.setName(userName);
					vXUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
					vXUser = xUserMgr.createXUser(vXUser);
				}
			}

			XXServiceConfigMap xConfMap = new XXServiceConfigMap();
			xConfMap = (XXServiceConfigMap) rangerAuditFields.populateAuditFields(xConfMap, xUpdService);
			xConfMap.setServiceId(service.getId());
			xConfMap.setConfigkey(configKey);
			xConfMap.setConfigvalue(configValue);
			xConfMap = xConfMapDao.create(xConfMap);
		}

		RangerService updService = svcService.getPopulatedViewObject(xUpdService);
		dataHistService.createObjectDataHistory(updService, RangerDataHistService.ACTION_UPDATE);

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
		for(XXPolicy policy : policies) {
			LOG.info("Deleting Policy, policyName: " + policy.getName());
			deletePolicy(policy.getId());
		}
		
		XXServiceConfigMapDao configDao = daoMgr.getXXServiceConfigMap();
		List<XXServiceConfigMap> configs = configDao.findByServiceId(service.getId());
		for (XXServiceConfigMap configMap : configs) {
			configDao.remove(configMap);
		}
		
		svcService.delete(service);
		dataHistService.createObjectDataHistory(service, RangerDataHistService.ACTION_DELETE);
	}

	@Override
	public RangerService getService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getService()");
		}
		return svcService.read(id);
	}

	@Override
	public RangerService getServiceByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceByName()");
		}
		XXService xService = daoMgr.getXXService().findByName(name);
		return xService == null ? null : svcService.getPopulatedViewObject(xService);
	}

	@Override
	public List<RangerService> getServices(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServices()");
		}
		List<RangerService> serviceList = svcService.getServices(filter);

		return serviceList;
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
		
		XXPolicy existing = daoMgr.getXXPolicy().findByName(policy.getName());

		if(existing != null) {
			throw new Exception("policy already exists: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName() + ". ID=" + existing.getId());
		}
		
		Map<String, RangerPolicyResource> resources = policy.getResources();
		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

		policy = policyService.create(policy);
		XXPolicy xCreatedPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		createNewResourcesForPolicy(policy, xCreatedPolicy, resources);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, policyItems, xServiceDef);
		
		handlePolicyUpdate(service);
		RangerPolicy createdPolicy = policyService.getPopulatedViewObject(xCreatedPolicy);
		dataHistService.createObjectDataHistory(createdPolicy, RangerDataHistService.ACTION_CREATE);
		
		return createdPolicy;
	}

	@Override
	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updatePolicy(" + policy + ")");
		}

		RangerPolicy existing = getPolicy(policy.getId());

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
			XXPolicy newNamePolicy = daoMgr.getXXPolicy().findByName(policy.getName());

			if(newNamePolicy != null) {
				throw new Exception("another policy already exists with name '" + policy.getName() + "'. ID=" + newNamePolicy.getId());
			}
		}
		Map<String, RangerPolicyResource> newResources = policy.getResources();
		List<RangerPolicyItem> newPolicyItems = policy.getPolicyItems();
		
		policy = policyService.update(policy);
		XXPolicy newUpdPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		deleteExistingPolicyResources(policy);
		deleteExistingPolicyItems(policy);
		
		createNewResourcesForPolicy(policy, newUpdPolicy, newResources);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, newPolicyItems, xServiceDef);
		
		handlePolicyUpdate(service);
		RangerPolicy updPolicy = policyService.getPopulatedViewObject(newUpdPolicy);
		dataHistService.createObjectDataHistory(updPolicy, RangerDataHistService.ACTION_UPDATE);
		
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
		
		deleteExistingPolicyItems(policy);
		deleteExistingPolicyResources(policy);
		
		policyService.delete(policy);
		handlePolicyUpdate(service);
		
		dataHistService.createObjectDataHistory(policy, RangerDataHistService.ACTION_DELETE);
		
		LOG.info("Policy Deleted Successfully. PolicyName : " +policyName);
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

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();
		List<XXPolicy> policyList = daoMgr.getXXPolicy().getAll();
		for (XXPolicy xPolicy : policyList) {
			RangerPolicy policy = policyService.getPopulatedViewObject(xPolicy);
			ret.add(policy);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceId + ")");
		}

		List<XXPolicy> servicePolicyList = daoMgr.getXXPolicy().findByServiceId(serviceId);
		List<RangerPolicy> servicePolicies = new ArrayList<RangerPolicy>();
		for(XXPolicy xPolicy : servicePolicyList) {
			RangerPolicy servicePolicy = policyService.getPopulatedViewObject(xPolicy);
			servicePolicies.add(servicePolicy);
		}

		return servicePolicies;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			XXService service = daoMgr.getXXService().findByName(serviceName);

			if(service == null) {
				return ret;
			}

			List<XXPolicy> policyList = daoMgr.getXXPolicy().findByServiceId(service.getId());
			for (XXPolicy xPolicy : policyList) {
				RangerPolicy policy = policyService.getPopulatedViewObject(xPolicy);
				ret.add(policy);
			}
		} catch(Exception excp) {
			LOG.error("ServiceDBStore.getServicePolicies(" + serviceName + "): failed to read policies", excp);
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name=" + serviceName);
		}

		RangerServiceDef serviceDef = getServiceDefByName(service.getType());

		if(serviceDef == null) {
			throw new Exception(service.getType() + ": unknown service-def)");
		}

		if(lastKnownVersion == null || service.getPolicyVersion() == null || lastKnownVersion.longValue() != service.getPolicyVersion().longValue()) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_NAME, serviceName);

			List<RangerPolicy> policies = getServicePolicies(serviceName, filter);

			ret = new ServicePolicies();

			ret.setServiceId(service.getId());
			ret.setServiceName(service.getName());
			ret.setPolicyVersion(service.getPolicyVersion());
			ret.setPolicyUpdateTime(service.getPolicyUpdateTime());
			ret.setPolicies(policies);
			ret.setServiceDef(serviceDef);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		if(ret != null && ret.getPolicies() != null) {
			Collections.sort(ret.getPolicies(), idComparator);
		}

		return ret;
	}
	
	private void createDefaultPolicy(XXService createdService, VXUser vXUser) throws Exception {
		RangerPolicy policy = new RangerPolicy();
		String policyName=createdService.getName()+"-"+1+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
		
		policy.setIsEnabled(true);
		policy.setVersion(1L);
		policy.setName(policyName);
		policy.setService(createdService.getName());
		policy.setDescription("Default Policy for Service: " + createdService.getName());
		policy.setIsAuditEnabled(true);
		
		Map<String, RangerPolicyResource> resources = new HashMap<String, RangerPolicyResource>();
		List<XXResourceDef> resDefList = daoMgr.getXXResourceDef().findByServiceDefId(createdService.getType());
		
		for(XXResourceDef resDef : resDefList) {
			RangerPolicyResource polRes = new RangerPolicyResource();
			polRes.setIsExcludes(false);
			polRes.setIsRecursive(false);
			
			String value;
			if("path".equalsIgnoreCase(resDef.getName())) {
				value = "/*/*";
			} else {
				value = "*";
			}
			polRes.setValue(value);
			resources.put(resDef.getName(), polRes);
		}
		policy.setResources(resources);
		
		if (vXUser != null) {
			List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
			RangerPolicyItem policyItem = new RangerPolicyItem();

			List<String> users = new ArrayList<String>();
			users.add(vXUser.getName());
			policyItem.setUsers(users);
			
			List<XXAccessTypeDef> accessTypeDefs = daoMgr.getXXAccessTypeDef().findByServiceDefId(createdService.getType());
			List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
			for(XXAccessTypeDef accessTypeDef : accessTypeDefs) {
				RangerPolicyItemAccess access = new RangerPolicyItemAccess();
				access.setType(accessTypeDef.getName());
				access.setIsAllowed(true);
				accesses.add(access);
			}
			policyItem.setAccesses(accesses);

			policyItem.setDelegateAdmin(true);
			policyItems.add(policyItem);
			policy.setPolicyItems(policyItems);
		}
		policy = createPolicy(policy);
		handlePolicyUpdate(svcService.getPopulatedViewObject(createdService));
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
			String confKey = config.getKey();
			String confValue = config.getValue();

			boolean found = false;
			for(XXServiceConfigDef xConfDef : svcConfDefList) {
				if((xConfDef.getName()).equalsIgnoreCase(confKey)) {
					found = true;
					break;
				}
			}
			if(found) {
				validConfigs.put(confKey, confValue);
			} else {
				LOG.info("Ignoring this config parameter:" + confKey
						+ ", as its not valid conf param for service");
			}
		}
		return validConfigs;
	}
	
	private void handlePolicyUpdate(RangerService service) throws Exception {
		if(service == null) {
			return;
		}
		
		Long policyVersion = service.getPolicyVersion();

		if(policyVersion == null) {
			policyVersion = new Long(1);
		} else {
			policyVersion = new Long(policyVersion.longValue() + 1);
		}
		
		service.setPolicyVersion(policyVersion);
		service.setPolicyUpdateTime(new Date());
		service = updateService(service);
	}
	
	private void createNewPolicyItemsForPolicy(RangerPolicy policy, XXPolicy xPolicy, List<RangerPolicyItem> policyItems, XXServiceDef xServiceDef) {
		
		for (RangerPolicyItem policyItem : policyItems) {
			XXPolicyItem xPolicyItem = new XXPolicyItem();
			xPolicyItem = (XXPolicyItem) rangerAuditFields.populateAuditFields(
					xPolicyItem, xPolicy);
			xPolicyItem.setDelegateAdmin(policyItem.getDelegateAdmin());
			xPolicyItem.setPolicyId(policy.getId());
			xPolicyItem = daoMgr.getXXPolicyItem().create(xPolicyItem);

			List<RangerPolicyItemAccess> accesses = policyItem.getAccesses();
			for (RangerPolicyItemAccess access : accesses) {

				XXAccessTypeDef xAccTypeDef = daoMgr.getXXAccessTypeDef()
						.findByNameAndServiceId(access.getType(),
								xPolicy.getService());
				if (xAccTypeDef == null) {
					LOG.info("One of given accessType is not valid for this policy. access: "
							+ access.getType() + ", Ignoring this access");
					continue;
				}

				XXPolicyItemAccess xPolItemAcc = new XXPolicyItemAccess();
				xPolItemAcc = (XXPolicyItemAccess) rangerAuditFields.populateAuditFields(xPolItemAcc, xPolicyItem);
				xPolItemAcc.setIsAllowed(access.getIsAllowed());

				xPolItemAcc.setType(xAccTypeDef.getId());
				xPolItemAcc.setPolicyitemid(xPolicyItem.getId());
				xPolItemAcc = daoMgr.getXXPolicyItemAccess()
						.create(xPolItemAcc);
			}
			List<String> users = policyItem.getUsers();
			for(String user : users) {
				XXUser xUser = daoMgr.getXXUser().findByUserName(user);
				if(xUser == null) {
					LOG.info("User does not exists with username: " 
							+ user + ", Ignoring permissions given to this user for policy");
					continue;
				}
				XXPolicyItemUserPerm xUserPerm = new XXPolicyItemUserPerm();
				xUserPerm = (XXPolicyItemUserPerm) rangerAuditFields.populateAuditFields(xUserPerm, xPolicyItem);
				xUserPerm.setUserId(xUser.getId());
				xUserPerm.setPolicyItemId(xPolicyItem.getId());
				xUserPerm = daoMgr.getXXPolicyItemUserPerm().create(xUserPerm);
			}
			
			List<String> groups = policyItem.getGroups();
			for(String group : groups) {
				XXGroup xGrp = daoMgr.getXXGroup().findByGroupName(group);
				if(xGrp == null) {
					LOG.info("Group does not exists with groupName: " 
							+ group + ", Ignoring permissions given to this group for policy");
					continue;
				}
				XXPolicyItemGroupPerm xGrpPerm = new XXPolicyItemGroupPerm();
				xGrpPerm = (XXPolicyItemGroupPerm) rangerAuditFields.populateAuditFields(xGrpPerm, xPolicyItem);
				xGrpPerm.setGroupId(xGrp.getId());
				xGrpPerm.setPolicyItemId(xPolicyItem.getId());
				xGrpPerm = daoMgr.getXXPolicyItemGroupPerm().create(xGrpPerm);
			}
			
			List<RangerPolicyItemCondition> conditions = policyItem.getConditions();
			for(RangerPolicyItemCondition condition : conditions) {
				XXPolicyConditionDef xPolCond = daoMgr
						.getXXPolicyConditionDef().findByServiceDefIdAndName(
								xServiceDef.getId(), condition.getType());
				
				if(xPolCond == null) {
					LOG.info("PolicyCondition is not valid, condition: "
							+ condition.getType()
							+ ", Ignoring creation of this policy condition");
					continue;
				}
				
				for(String value : condition.getValues()) {
					XXPolicyItemCondition xPolItemCond = new XXPolicyItemCondition();
					xPolItemCond = (XXPolicyItemCondition) rangerAuditFields.populateAuditFields(xPolItemCond, xPolicyItem);
					xPolItemCond.setPolicyItemId(xPolicyItem.getId());
					xPolItemCond.setType(xPolCond.getId());
					xPolItemCond.setValue(value);
					xPolItemCond = daoMgr.getXXPolicyItemCondition().create(xPolItemCond);
				}
			}
		}
	}

	private void createNewResourcesForPolicy(RangerPolicy policy, XXPolicy xPolicy, Map<String, RangerPolicyResource> resources) {
		
		for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
			RangerPolicyResource policyRes = resource.getValue();

			XXResourceDef xResDef = daoMgr.getXXResourceDef()
					.findByNameAndPolicyId(resource.getKey(), policy.getId());
			if (xResDef == null) {
				LOG.info("No Such Resource found, resourceName : "
						+ resource.getKey() + ", Ignoring this resource.");
				continue;
			}

			XXPolicyResource xPolRes = new XXPolicyResource();
			xPolRes = (XXPolicyResource) rangerAuditFields.populateAuditFields(xPolRes, xPolicy);

			xPolRes.setIsExcludes(policyRes.getIsExcludes());
			xPolRes.setIsRecursive(policyRes.getIsRecursive());
			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResDefId(xResDef.getId());
			xPolRes = daoMgr.getXXPolicyResource().create(xPolRes);

			List<String> values = policyRes.getValues();
			for (String value : values) {
				XXPolicyResourceMap xPolResMap = new XXPolicyResourceMap();
				xPolResMap = (XXPolicyResourceMap) rangerAuditFields.populateAuditFields(xPolResMap, xPolRes);
				xPolResMap.setResourceId(xPolRes.getId());
				xPolResMap.setValue(value);

				xPolResMap = daoMgr.getXXPolicyResourceMap().create(xPolResMap);
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

	private final static Comparator<RangerBaseModelObject> idComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			Long val1 = (o1 != null) ? o1.getId() : null;
			Long val2 = (o2 != null) ? o2.getId() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};
}
