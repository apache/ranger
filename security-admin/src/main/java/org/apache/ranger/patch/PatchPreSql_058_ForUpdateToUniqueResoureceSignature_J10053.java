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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RangerFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPolicyLabelMapDao;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyLabelMap;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * This patch will re-calculate and update policy resource_signature for all disabled Ranger Policies.
 *
 */
@Component
public class PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053 extends BaseLoader {
	private static final Logger logger = LoggerFactory.getLogger(PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	ServiceDBStore svcStore;

	@Autowired
	RangerFactory factory;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;

	private final Boolean isPolicyEnabled = false;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053 loader = (PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053) CLIUtil.getBean(PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053.class);

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
		logger.info("==> PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053.execLoad()");

		try {
			updateDisabledPolicyResourceSignature();
			removeDuplicateResourceSignaturesPolicies();
		} catch (Exception e) {
			logger.error("Error while PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053()", e);
			System.exit(1);
		}

		logger.info("<== PatchPreSql_058_ForUpdateToUniqueResoureceSignature_J10053.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("Updating resource_signature of disabled Policy");
	}

	private void updateDisabledPolicyResourceSignature() throws Exception {
		logger.info("==> updateDisabledPolicyResourceSignature() ");

		List<XXPolicy> xxPolicyList = daoMgr.getXXPolicy().findByPolicyStatus(isPolicyEnabled);
		if (CollectionUtils.isNotEmpty(xxPolicyList)) {
			logger.info("==> Total number of disabled policies :" + xxPolicyList.size());

			for (XXPolicy xxPolicy : xxPolicyList) {
				RangerPolicy policy = svcStore.getPolicy(xxPolicy.getId());
				if (policy != null) {
					policy.setResourceSignature(null);
					xxPolicy.setResourceSignature(null);
					RangerPolicyResourceSignature policySignature = factory.createPolicyResourceSignature(policy);
					String signature = policySignature.getSignature();
					policy.setResourceSignature(signature);
					xxPolicy.setPolicyText(JsonUtils.objectToJson(policy));
					xxPolicy.setResourceSignature(signature);

					daoMgr.getXXPolicy().update(xxPolicy);
				}
			}
		} else {
			logger.info("no disabled Policy found");
		}
	}

	private void removeDuplicateResourceSignaturesPolicies() throws Exception {
		logger.info("==> removeDuplicateResourceSignaturesPolicies() ");
		Map<String, Long> duplicateEntries = daoMgr.getXXPolicy().findDuplicatePoliciesByServiceAndResourceSignature();
		if (duplicateEntries != null && duplicateEntries.size() > 0) {
			logger.info("Total number of possible duplicate policies:" + duplicateEntries.size());
			for (Map.Entry<String, Long> entry : duplicateEntries.entrySet()) {
				logger.info("Duplicate policy Entry - {ResourceSignature:" + entry.getKey() + ", ServiceId:" + entry.getValue() + "}");
				List<XXPolicy> xxPolicyList = daoMgr.getXXPolicy().findByServiceIdAndResourceSignature(entry.getValue(), entry.getKey());
				if (CollectionUtils.isNotEmpty(xxPolicyList) && xxPolicyList.size() > 1) {
					Iterator<XXPolicy> duplicatePolicies = xxPolicyList.iterator();
					duplicatePolicies.next();
					while (duplicatePolicies.hasNext()) {
						XXPolicy xxPolicy = duplicatePolicies.next();
						if (xxPolicy != null) {
							logger.info("Attempting to Remove duplicate policy:{" + xxPolicy.getId() + ":" + xxPolicy.getName() + "}");
							if (cleanupRefTables(xxPolicy.getId())) {
								daoMgr.getXXPolicy().remove(xxPolicy.getId());
							}
						}
					}
				}
			}
		} else {
			logger.info("no duplicate Policy found");
		}
	}

	private Boolean cleanupRefTables(Long policyId) {
		if (policyId == null) {
			return false;
		}
		daoMgr.getXXPolicyRefResource().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefRole().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefGroup().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefUser().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefAccessType().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefCondition().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefDataMaskType().deleteByPolicyId(policyId);
		XXPolicyLabelMapDao policyLabelMapDao = daoMgr.getXXPolicyLabelMap();
		List<XXPolicyLabelMap> xxPolicyLabelMaps = policyLabelMapDao.findByPolicyId(policyId);
		for (XXPolicyLabelMap xxPolicyLabelMap : xxPolicyLabelMaps) {
			policyLabelMapDao.remove(xxPolicyLabelMap);
		}
		return true;
	}
}
