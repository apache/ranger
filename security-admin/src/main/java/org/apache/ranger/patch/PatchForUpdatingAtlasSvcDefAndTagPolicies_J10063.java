/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.patch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063 extends BaseLoader {
	private static final Logger logger = LoggerFactory
			.getLogger(PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	ServiceDBStore svcStore;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063 loader = (PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063) CLIUtil
					.getBean(PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.class);
			loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			logger.info("Load complete. Exiting.");
			System.exit(0);
		} catch (Exception e) {
			logger.error("Error loading", e);
			System.exit(1);
		}
	}

	@Override
	public void printStats() {
		logger.info("PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063 data ");
	}

	@Override
	public void execLoad() {
		logger.info("==> PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.execLoad()");
		try {
			if (updateAtlasServiceDef()) {
				disableAtlasAccessForTagPolicies();
			}
		} catch (Exception e) {
			logger.error("Error while updateTagServiceDef()data.", e);
			System.exit(1);
		}
		logger.info("<== PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.execLoad()");
	}

	@Override
	public void init() throws Exception {
		// Do Nothing
	}

	private boolean updateAtlasServiceDef() throws Exception {
		logger.info("==> PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.updateAtlasServiceDef()");
		RangerServiceDef embeddedAtlasServiceDef;
		XXServiceDef xXServiceDefObj;

		embeddedAtlasServiceDef = EmbeddedServiceDefsUtil.instance()
				.getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

		if (embeddedAtlasServiceDef != null) {
			xXServiceDefObj = daoMgr.getXXServiceDef().findByName(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME);

			if (xXServiceDefObj != null) {
				String jsonStrUpdate = xXServiceDefObj.getDefOptions();
				Map<String, String> serviceDefOptionsUpdate = jsonStringToMap(jsonStrUpdate);
				if (serviceDefOptionsUpdate == null) {
					serviceDefOptionsUpdate = new HashMap<>();
				}
				serviceDefOptionsUpdate.put(RangerServiceDef.OPTION_ENABLE_TAG_BASED_POLICIES, "false");
				xXServiceDefObj.setDefOptions(mapToJsonString(serviceDefOptionsUpdate));
				daoMgr.getXXServiceDef().update(xXServiceDefObj);
			} else {
				logger.error("Atlas service-definition does not exist in the Ranger DAO.");
				return false;
			}
		} else {
			logger.error("The embedded Atlas service-definition does not exist.");
			return false;
		}
		logger.info("<== PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.updateAtlasServiceDef()");
		return true;
	}

	private void disableAtlasAccessForTagPolicies() throws Exception {
		logger.info("==> PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.disableAtlasAccessForTagPolicies()");
		RangerServiceDef embeddedTagServiceDef = EmbeddedServiceDefsUtil.instance()
				.getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);
		if (embeddedTagServiceDef != null) {
			List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByServiceDefId(embeddedTagServiceDef.getId());
			if (CollectionUtils.isNotEmpty(xxPolicies)) {
				for (XXPolicy xxPolicy : xxPolicies) {
					RangerPolicy rPolicy = svcStore.getPolicy(xxPolicy.getId());
					if (CollectionUtils.isNotEmpty(rPolicy.getPolicyItems()) || CollectionUtils.isNotEmpty(rPolicy.getAllowExceptions())
							|| CollectionUtils.isNotEmpty(rPolicy.getDenyPolicyItems()) || CollectionUtils.isNotEmpty(rPolicy.getDenyExceptions())) {
						updateAccessTypeForTagPolicies(rPolicy.getPolicyItems());
						updateAccessTypeForTagPolicies(rPolicy.getAllowExceptions());
						updateAccessTypeForTagPolicies(rPolicy.getDenyPolicyItems());
						updateAccessTypeForTagPolicies(rPolicy.getDenyExceptions());
						svcStore.updatePolicy(rPolicy);
					}
				}
			}
		} else {
			logger.error("The embedded Tag service-definition does not exist.");
		}

		// delete XXAccessTypeDef records of tagDef where name startWith Atlas
		List<XXAccessTypeDef> xxAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(embeddedTagServiceDef.getId());
		for (XXAccessTypeDef xAccess : xxAccessTypes) {
			if (xAccess != null && xAccess.getName().startsWith(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME)) {
				svcStore.deleteXXAccessTypeDef(xAccess);
			}
		}
		logger.info("<== PatchForUpdatingAtlasSvcDefAndTagPolicies_J10063.disableAtlasAccessForTagPolicies()");
	}

	private void updateAccessTypeForTagPolicies(List<RangerPolicyItem> policyItems) throws Exception {
		logger.info("==> PatchForDisableAccessTypeForTagPolicies_J10063.updateAccessTypeForTagPolicies() "+policyItems);
		if (CollectionUtils.isEmpty(policyItems)) {
			logger.info("==> PatchForDisableAccessTypeForTagPolicies_J10063.updateAccessTypeForTagPolicies() policy items collection was null/empty");
		} else {
			List<RangerPolicy.RangerPolicyItem> removePolicyItem = new ArrayList<RangerPolicy.RangerPolicyItem>();
			for (RangerPolicyItem policyItem : policyItems) {
				if (policyItem != null && policyItem.getAccesses() != null) {
					List<RangerPolicy.RangerPolicyItemAccess> accessesToRemove = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
					for (RangerPolicyItemAccess access : policyItem.getAccesses()) {
						if (access != null) {
							String[] svcDefAccType = access.getType().split(":");
							String serviceDefName = svcDefAccType.length > 0 ? svcDefAccType[0] : null;
							if (serviceDefName != null && serviceDefName.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME)) {
								accessesToRemove.add(access);
							}
						}
					}
					policyItem.getAccesses().removeAll(accessesToRemove);
				}
				if(policyItem != null && CollectionUtils.isEmpty(policyItem.getAccesses())) {
					removePolicyItem.add(policyItem);
				}
			}
			policyItems.removeAll(removePolicyItem);
		}
		logger.info("<== PatchForDisableAccessTypeForTagPolicies_J10063.updateAccessTypeForTagPolicies() "+policyItems);
	}

	private String mapToJsonString(Map<String, String> map) {
		String ret = null;
		if (map != null) {
			try {
				ret = jsonUtil.readMapToString(map);
			} catch (Exception ex) {
				logger.warn("mapToJsonString() failed to convert map: " + map, ex);
			}
		}
		return ret;
	}

	private Map<String, String> jsonStringToMap(String jsonStr) {
		Map<String, String> ret = null;
		if (!StringUtils.isEmpty(jsonStr)) {
			try {
				ret = jsonUtil.jsonToMap(jsonStr);
			} catch (Exception ex) {
				// fallback to earlier format: "name1=value1;name2=value2"
				for (String optionString : jsonStr.split(";")) {
					if (StringUtils.isEmpty(optionString)) {
						continue;
					}
					String[] nvArr = optionString.split("=");
					String name = (nvArr.length > 0) ? nvArr[0].trim() : null;
					String value = (nvArr.length > 1) ? nvArr[1].trim() : null;
					if (StringUtils.isEmpty(name)) {
						continue;
					}
					if (ret == null) {
						ret = new HashMap<>();
					}
					ret.put(name, value);
				}
			}
		}
		return ret;
	}

}
