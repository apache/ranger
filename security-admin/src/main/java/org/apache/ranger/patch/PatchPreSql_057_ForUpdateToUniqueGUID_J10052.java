/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.patch;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * This patch will regenerate new GUID and update policies which has duplicate GUID for every service.
 *
 */
@Component
public class PatchPreSql_057_ForUpdateToUniqueGUID_J10052 extends BaseLoader {
	private static final Logger logger = LoggerFactory.getLogger(PatchPreSql_057_ForUpdateToUniqueGUID_J10052.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	ServiceDBStore svcStore;

	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;


	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchPreSql_057_ForUpdateToUniqueGUID_J10052 loader = (PatchPreSql_057_ForUpdateToUniqueGUID_J10052) CLIUtil.getBean(PatchPreSql_057_ForUpdateToUniqueGUID_J10052.class);

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

		try {
			logger.info("==> updatePolicyGUIDToUniqueValue()");
			updatePolicyGUIDToUniqueValue();
		} catch (Exception e) {
			logger.error("Error while updatePolicyGUIDToUniqueValue()", e);
			System.exit(1);
		}

		logger.info("<== updatePolicyGUIDToUniqueValue.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("runnig updatePolicyGUIDToUniqueValue ");
	}

	private void updatePolicyGUIDToUniqueValue() throws Exception {
		logger.info("==> updatePolicyGUIDToUniqueValue() ");

		List<XXSecurityZone> allXXZones = null;
		List<XXService> allXXService = null;

		allXXZones = daoMgr.getXXSecurityZoneDao().getAll();
		allXXService = daoMgr.getXXService().getAll();

		if (CollectionUtils.isNotEmpty(allXXZones) && CollectionUtils.isNotEmpty(allXXService)) {
			logger.info("Total number of zones " + allXXZones.size() +", service :" +allXXService.size());
			for (XXSecurityZone xSecurityZone : allXXZones) {
				for (XXService xService : allXXService) {
					logger.info("serching duplicate guid policies for service :" + xService.getName() + " zone : "
							+ xSecurityZone.getName());
					List<String> duplicateGuidList = daoMgr.getXXPolicy()
							.findDuplicateGUIDByServiceIdAndZoneId(xService.getId(), xSecurityZone.getId());
					if (CollectionUtils.isNotEmpty(duplicateGuidList)) {
						logger.info("Total number of duplicate GUIDs :" + duplicateGuidList.size() + " for service :"
								+ xService.getName() + " and zone :" + xSecurityZone.getName());
						for (String guid : duplicateGuidList) {
							List<XXPolicy> xxPolicyList = daoMgr.getXXPolicy().findPolicyByGUIDAndServiceIdAndZoneId(
									guid, xService.getId(), xSecurityZone.getId());
							boolean isFirstElement = false;
							if (CollectionUtils.isNotEmpty(xxPolicyList)) {
								isFirstElement = true;
								for (XXPolicy xxPolicy : xxPolicyList) {
									if (isFirstElement) {
										isFirstElement = false;
										continue;
									}
									RangerPolicy policy = getPolicy(xxPolicy);
									if (policy != null) {
										guid = guidUtil.genGUID();
										xxPolicy.setGuid(guid);
										policy.setGuid(guid);
										xxPolicy.setPolicyText(JsonUtils.objectToJson(policy));

										daoMgr.getXXPolicy().update(xxPolicy);
									}
								}
							} else {
								logger.info("No policy found with guid:" + guid);
							}
						}
					} else {
						logger.info("No duplicate GUID found in policy for Service :" + xService.getName() + ", Zone : "
								+ xSecurityZone.getName());
					}
				}
			}
		} else {
			logger.info("No zone or service found");
		}
	}

	private RangerPolicy getPolicy(final XXPolicy xPolicy) {
		final RangerPolicy ret;

		if (xPolicy != null) {
			String policyText = xPolicy.getPolicyText();
			if (logger.isDebugEnabled()) {
				logger.debug("Ranger Policy text:[" + policyText + "]");
			}
			ret = JsonUtils.jsonToObject(policyText, RangerPolicy.class);

			if (ret != null) {
				ret.setId(xPolicy.getId());
				ret.setGuid(xPolicy.getGuid());
				ret.setCreateTime(xPolicy.getCreateTime());
				ret.setUpdateTime(xPolicy.getUpdateTime());
				ret.setVersion(xPolicy.getVersion());
				ret.setPolicyType(xPolicy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : xPolicy.getPolicyType());
				XXSecurityZone xSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneId(xPolicy.getZoneId());
				if (xSecurityZone != null) {
					ret.setZoneName(xSecurityZone.getName());
				}
			}
		} else {
			ret = null;
		}
		return ret;
	}
}