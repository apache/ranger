package org.apache.ranger.patch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXAuditMap;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.patch.BaseLoader;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermObj;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchMigration_J10002 extends BaseLoader {
	private static Logger logger = Logger.getLogger(PatchMigration_J10002.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	ServiceDBStore svcDBStore;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerPolicyService policyService;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	XPolicyService xPolService;

	@Autowired
	XPermMapService xPermMapService;
	
	@Autowired
	RangerBizUtil bizUtil;

	private static int policyCounter = 0;
	private static int serviceCounter = 0;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchMigration_J10002 loader = (PatchMigration_J10002) CLIUtil.getBean(PatchMigration_J10002.class);
			loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting!!!");
			System.exit(0);
		} catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}
	
	@Override
	public void init() throws Exception {
		// Do Nothing
	}

	@Override
	public void execLoad() {
		logger.info("==> MigrationPatch.execLoad()");
		try {
			migrateServicesToNewSchema();
			migratePoliciesToNewSchema();
			updateSequences();
		} catch (Exception e) {
			logger.error("Error whille migrating data.", e);
		}
		logger.info("<== MigrationPatch.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("Total Number of migrated repositories/services: " + serviceCounter);
		logger.info("Total Number of migrated resources/policies: " + policyCounter);
	}

	public void migrateServicesToNewSchema() throws Exception {
		logger.info("==> MigrationPatch.migrateServicesToNewSchema()");

		try {
			List<XXAsset> repoList = daoMgr.getXXAsset().getAll();

			if (repoList.size() <= 0) {
				return;
			}
			if (repoList.size() > 0) {
				EmbeddedServiceDefsUtil.instance().init(svcDBStore);
			}

			svcDBStore.setPopulateExistingBaseFields(true);
			for (XXAsset xAsset : repoList) {

				if (xAsset.getActiveStatus() == AppConstants.STATUS_DELETED) {
					continue;
				}

				RangerService existing = svcDBStore.getServiceByName(xAsset.getName());
				if (existing != null) {
					logger.info("Repository/Service already exists. Ignoring migration of repo: " + xAsset.getName());
					continue;
				}

				RangerService service = new RangerService();
				service = mapXAssetToService(service, xAsset);

				service = svcDBStore.createService(service);

				serviceCounter++;
				logger.info("New Service created. ServiceName: " + service.getName());
			}
			svcDBStore.setPopulateExistingBaseFields(false);
		} catch (Exception e) {
			throw new Exception("Error while migrating data to new Plugin Schema.", e);
		}
		logger.info("<== MigrationPatch.migrateServicesToNewSchema()");
	}

	public void migratePoliciesToNewSchema() throws Exception {
		logger.info("==> MigrationPatch.migratePoliciesToNewSchema()");

		try {
			List<XXResource> resList = daoMgr.getXXResource().getAll();
			if (resList.size() <= 0) {
				return;
			}

			svcDBStore.setPopulateExistingBaseFields(true);
			for (XXResource xRes : resList) {

				if (xRes.getResourceStatus() == AppConstants.STATUS_DELETED) {
					continue;
				}

				XXAsset xAsset = daoMgr.getXXAsset().getById(xRes.getAssetId());
				if (xAsset == null) {
					logger.error("No Repository found for policyName: " + xRes.getPolicyName());
					continue;
				}

				RangerService service = svcDBStore.getServiceByName(xAsset.getName());
				
				if (service == null) {
					logger.error("No Service found for policy. Ignoring migration of such policy, policyName: "
							+ xRes.getPolicyName());
					continue;
				}

				XXPolicy existing = daoMgr.getXXPolicy().findByNameAndServiceId(xRes.getPolicyName(), service.getId());
				if (existing != null) {
					logger.info("Policy already exists. Ignoring migration of policy: " + existing.getName());
					continue;
				}

				RangerPolicy policy = new RangerPolicy();
				policy = mapXResourceToPolicy(policy, xRes, service);

				policy = svcDBStore.createPolicy(policy);

				policyCounter++;
				logger.info("New policy created. policyName: " + policy.getName());
			}
			svcDBStore.setPopulateExistingBaseFields(false);
		} catch (Exception e) {
			throw new Exception("Error while migrating data to new Plugin Schema.", e);
		}
		logger.info("<== MigrationPatch.migratePoliciesToNewSchema()");
	}

	private RangerService mapXAssetToService(RangerService service, XXAsset xAsset) throws Exception {

		String type = "";
		String name = xAsset.getName();
		String description = xAsset.getDescription();
		Map<String, String> configs = null;

		int typeInt = xAsset.getAssetType();
		XXServiceDef serviceDef = daoMgr.getXXServiceDef().findByName(AppConstants.getLabelFor_AssetType(typeInt).toLowerCase());

		if (serviceDef == null) {
			throw new Exception("No ServiceDefinition found for repository: " + name);
		}
		type = serviceDef.getName();
		configs = jsonUtil.jsonToMap(xAsset.getConfig());

		List<XXServiceConfigDef> mandatoryConfigs = daoMgr.getXXServiceConfigDef().findByServiceDefName(type);
		for (XXServiceConfigDef serviceConf : mandatoryConfigs) {
			if (serviceConf.getIsMandatory()) {
				if (!stringUtil.isEmpty(configs.get(serviceConf.getName()))) {
					continue;
				}
				String dataType = serviceConf.getType();
				String defaultValue = serviceConf.getDefaultvalue();

				if (stringUtil.isEmpty(defaultValue)) {
					defaultValue = getDefaultValueForDataType(dataType);
				}
				configs.put(serviceConf.getName(), defaultValue);
			}
		}

		service.setType(type);
		service.setName(name);
		service.setDescription(description);
		service.setConfigs(configs);

		service.setCreateTime(xAsset.getCreateTime());
		service.setUpdateTime(xAsset.getUpdateTime());

		XXPortalUser createdByUser = daoMgr.getXXPortalUser().getById(xAsset.getAddedByUserId());
		XXPortalUser updByUser = daoMgr.getXXPortalUser().getById(xAsset.getUpdatedByUserId());

		if (createdByUser != null) {
			service.setCreatedBy(createdByUser.getLoginId());
		}
		if (updByUser != null) {
			service.setUpdatedBy(updByUser.getLoginId());
		}
		service.setId(xAsset.getId());

		return service;
	}

	private String getDefaultValueForDataType(String dataType) {

		String defaultValue = "";
		switch (dataType) {
		case "int":
			defaultValue = "0";
			break;
		case "string":
			defaultValue = "unknown";
			break;
		case "bool":
			defaultValue = "false";
			break;
		case "enum":
			defaultValue = "0";
			break;
		case "password":
			defaultValue = "password";
			break;
		default:
			break;
		}
		return defaultValue;
	}

	private RangerPolicy mapXResourceToPolicy(RangerPolicy policy, XXResource xRes, RangerService service) {

		String serviceName = service.getName();
		String serviceDef = service.getType();
		String name = xRes.getPolicyName();
		String description = xRes.getDescription();
		Boolean isAuditEnabled = true;
		Boolean isEnabled = true;
		Map<String, RangerPolicyResource> resources = new HashMap<String, RangerPolicyResource>();
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();

		List<XXAuditMap> auditMapList = daoMgr.getXXAuditMap().findByResourceId(xRes.getId());
		if (stringUtil.isEmpty(auditMapList)) {
			isAuditEnabled = false;
		}
		if (xRes.getResourceStatus() == AppConstants.STATUS_DISABLED) {
			isEnabled = false;
		}

		boolean tableExcludes = false;
		boolean columnExcludes = false;

		if (xRes.getTableType() == AppConstants.POLICY_EXCLUSION) {
			tableExcludes = true;
		}
		if (xRes.getColumnType() == AppConstants.POLICY_EXCLUSION) {
			columnExcludes = true;
		}

		if (serviceDef.equalsIgnoreCase("hdfs")) {
			resources.put("path", new RangerPolicyResource(Arrays.asList(xRes.getName()), false, AppConstants
							.getBooleanFor_BooleanValue(xRes.getIsRecursive())));

		} else if (serviceDef.equalsIgnoreCase("hbase")) {
			resources.put("table", new RangerPolicyResource(Arrays.asList(xRes.getTables()), tableExcludes, false));
			resources.put("column", new RangerPolicyResource(Arrays.asList(xRes.getColumns()), columnExcludes, false));
			resources.put("column-family", new RangerPolicyResource(Arrays.asList(xRes.getColumnFamilies()), false, false));

		} else if (serviceDef.equalsIgnoreCase("hive")) {
			resources.put("table", new RangerPolicyResource(Arrays.asList(xRes.getTables()), tableExcludes, false));
			resources.put("column", new RangerPolicyResource(Arrays.asList(xRes.getColumns()), columnExcludes, false));
			resources.put("database", new RangerPolicyResource(Arrays.asList(xRes.getDatabases()), false, false));
			resources.put("udf", new RangerPolicyResource(Arrays.asList(xRes.getUdfs()), false, false));
		} else if (serviceDef.equalsIgnoreCase("knox")) {
			resources.put("topology", new RangerPolicyResource(Arrays.asList(xRes.getTopologies()), false, false));
			resources.put("service", new RangerPolicyResource(Arrays.asList(xRes.getServices()), false, false));
		} else if (serviceDef.equalsIgnoreCase("storm")) {
			resources.put("topology", new RangerPolicyResource(Arrays.asList(xRes.getTopologies()), false, false));
		}

		policyItems = getPolicyItemListForRes(xRes, serviceDef);

		policy.setService(serviceName);
		policy.setName(name);
		policy.setDescription(description);
		policy.setIsAuditEnabled(isAuditEnabled);
		policy.setIsEnabled(isEnabled);
		policy.setResources(resources);
		policy.setPolicyItems(policyItems);

		policy.setCreateTime(xRes.getCreateTime());
		policy.setUpdateTime(xRes.getUpdateTime());

		XXPortalUser createdByUser = daoMgr.getXXPortalUser().getById(xRes.getAddedByUserId());
		XXPortalUser updByUser = daoMgr.getXXPortalUser().getById(xRes.getUpdatedByUserId());

		if (createdByUser != null) {
			policy.setCreatedBy(createdByUser.getLoginId());
		}
		if (updByUser != null) {
			policy.setUpdatedBy(updByUser.getLoginId());
		}

		policy.setId(xRes.getId());

		return policy;
	}

	private List<RangerPolicyItem> getPolicyItemListForRes(XXResource xRes, String serviceDefName) {
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();

		SearchCriteria sc = new SearchCriteria();
		sc.addParam("resourceId", xRes.getId());
		List<VXPermMap> permMapList = xPermMapService.searchXPermMaps(sc).getVXPermMaps();
		List<VXPermObj> permObjList = xPolService.mapPermMapToPermObj(permMapList);

		XXServiceDef svcDef = daoMgr.getXXServiceDef().findByName(serviceDefName);
		if (svcDef == null) {
			return new ArrayList<RangerPolicyItem>();
		}

		XXPolicyConditionDef policyCond = daoMgr.getXXPolicyConditionDef().findByServiceDefIdAndName(svcDef.getId(),
				"ip-range");

		for (VXPermObj permObj : permObjList) {

			List<String> permList = permObj.getPermList();
			if (permList == null) {
				continue;
			}

			RangerPolicyItem policyItem = new RangerPolicyItem();
			List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
			List<RangerPolicyItemCondition> conditions = new ArrayList<RangerPolicyItemCondition>();

			if (permObj.getPermList().contains("Admin")) {
				policyItem.setDelegateAdmin(true);
			}

			for (String perm : permList) {
				RangerPolicyItemAccess access = new RangerPolicyItemAccess();
				access.setIsAllowed(true);
				access.setType(perm);
				accesses.add(access);
			}
			if (!stringUtil.isEmpty(permObj.getIpAddress()) && policyCond != null) {
				RangerPolicyItemCondition condition = new RangerPolicyItemCondition();
				condition.setType("ip-range");

				List<String> ipRangeList = Arrays.asList(permObj.getIpAddress());

				condition.setValues(ipRangeList);
				conditions.add(condition);
			}

			policyItem.setUsers(permObj.getUserList());
			policyItem.setGroups(permObj.getGroupList());
			policyItem.setAccesses(accesses);
			policyItem.setConditions(conditions);

			policyItems.add(policyItem);
		}
		return policyItems;
	}
	
	private void updateSequences() {
		
		if(RangerBizUtil.getDBFlavor() != AppConstants.DB_FLAVOR_ORACLE) {
			return;
		}
		
		List<String> queryList = new ArrayList<String>();
		String policySequence = "X_POLICY_SEQ";
		String svcSequence = "X_SERVICE_SEQ";
		
		if(serviceCounter > 0) {
			
			Long maxSvcId = daoMgr.getXXService().getMaxIdOfXXService();
			
			if(maxSvcId != null) {
				String query1 = "ALTER SEQUENCE " + svcSequence + " INCREMENT BY " + maxSvcId;
				String query2 = "select " + svcSequence + ".nextval from dual";
				String query3 = "ALTER SEQUENCE " + svcSequence + " INCREMENT BY 1 NOCACHE NOCYCLE";
				queryList.add(query1);
				queryList.add(query2);
				queryList.add(query3);
			}
		}
		
		if(policyCounter > 0) {
			
			Long maxPolId = daoMgr.getXXPolicy().getMaxIdOfXXPolicy();
			
			if(maxPolId != null) {				
				String query1 = "ALTER SEQUENCE " + policySequence + " INCREMENT BY " + maxPolId;
				String query2 = "select " + policySequence + ".nextval from dual";
				String query3 = "ALTER SEQUENCE " + policySequence + " INCREMENT BY 1 NOCACHE NOCYCLE";
				queryList.add(query1);
				queryList.add(query2);
				queryList.add(query3);
			}
		}
		
		for(String query : queryList) {
			daoMgr.getEntityManager().createNativeQuery(query).executeUpdate();
		}
		
	}

}