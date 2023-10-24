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

package org.apache.ranger.patch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchForAllServiceDefForPolicyConditionUpdate_J10060 extends BaseLoader{
	private static final Logger logger = LoggerFactory.getLogger(PatchForAllServiceDefForPolicyConditionUpdate_J10060.class);

	@Autowired
	ServiceDBStore svcStore;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchForAllServiceDefForPolicyConditionUpdate_J10060 loader = (PatchForAllServiceDefForPolicyConditionUpdate_J10060) CLIUtil.getBean(PatchForAllServiceDefForPolicyConditionUpdate_J10060.class);
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
		logger.info("==> PatchForAllServiceDefForPolicyConditionUpdate_J10060.execLoad()");
		try {
			updateAllServiceDef();
		} catch (Exception e) {
			logger.error("Error whille PatchForAllServiceDefForPolicyConditionUpdate_J10060()data.", e);
		}
		logger.info("<== PatchForAllServiceDefForPolicyConditionUpdate_J10060.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("PatchForAllServiceDefForPolicyConditionUpdate_J10060 data ");
	}

	private void updateAllServiceDef() {

		try {
		List<RangerServiceDef> allServiceDefs = svcStore.getServiceDefs(new SearchFilter());

		if (CollectionUtils.isNotEmpty(allServiceDefs)) {
				for (RangerServiceDef serviceDef : allServiceDefs) {
					if(CollectionUtils.isNotEmpty(serviceDef.getPolicyConditions())) {
						Map<Long,String> uiHintPreVal = new HashMap<>();
						List<RangerPolicyConditionDef> updatedPolicyCondition = new ArrayList<>();
						RangerServiceDef embeddedTagServiceDef=EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(serviceDef.getName());

						List<RangerPolicyConditionDef> policyConditionsOld = embeddedTagServiceDef.getPolicyConditions();
						for(RangerPolicyConditionDef policyConditionOld : policyConditionsOld) {
							uiHintPreVal.put(policyConditionOld.getItemId(), policyConditionOld.getUiHint());
						}

						List<RangerPolicyConditionDef> policyConditionsNew = serviceDef.getPolicyConditions();
						for (RangerPolicyConditionDef policyConditionNew : policyConditionsNew) {
							if(StringUtils.isNotEmpty(uiHintPreVal.get(policyConditionNew.getItemId()))) {
								policyConditionNew.setUiHint(uiHintPreVal.get(policyConditionNew.getItemId()));
							}
							updatedPolicyCondition.add(policyConditionNew);
						}

						serviceDef.setPolicyConditions(updatedPolicyCondition);
						svcStore.updateServiceDef(serviceDef);
					}
			}
		}
		}catch (Exception e) {
			logger.error("Error while patching service-def for policy condition:", e);
		}
	}
}