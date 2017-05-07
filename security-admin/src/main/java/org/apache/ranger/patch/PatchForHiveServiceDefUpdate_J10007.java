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

import org.apache.log4j.Logger;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.util.CLIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PatchForHiveServiceDefUpdate_J10007 extends BaseLoader {
	private static final Logger logger = Logger.getLogger(PatchForHiveServiceDefUpdate_J10007.class);
	public static final String SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME  = "hive";
	public static final String URL_RESOURCE_NAME ="url";

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

	@Autowired
	RangerValidatorFactory validatorFactory;

	@Autowired
	ServiceDBStore svcStore;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchForHiveServiceDefUpdate_J10007 loader = (PatchForHiveServiceDefUpdate_J10007) CLIUtil.getBean(PatchForHiveServiceDefUpdate_J10007.class);
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
		logger.info("==> PatchForHiveServiceDefUpdate.execLoad()");
		try {
			updateHiveServiceDef();
		} catch (Exception e) {
			logger.error("Error whille updateHiveServiceDef()data.", e);
		}
		logger.info("<== PatchForHiveServiceDefUpdate.execLoad()");
	}

	@Override
	public void printStats() {
		logger.info("PatchForHiveServiceDefUpdate data ");
	}

	private void updateHiveServiceDef(){
		RangerServiceDef ret  					= null;
		RangerServiceDef embeddedHiveServiceDef = null;
		RangerServiceDef dbHiveServiceDef 		= null;
		List<RangerServiceDef.RangerResourceDef> 	embeddedHiveResourceDefs  = null;
		List<RangerServiceDef.RangerAccessTypeDef> 	embeddedHiveAccessTypes   = null;

		try{
			embeddedHiveServiceDef=EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME);
			if(embeddedHiveServiceDef!=null){

				dbHiveServiceDef=svcDBStore.getServiceDefByName(SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME);
				
				if(dbHiveServiceDef!=null){
					embeddedHiveResourceDefs = embeddedHiveServiceDef.getResources();
					embeddedHiveAccessTypes  = embeddedHiveServiceDef.getAccessTypes();

					if (checkURLresourcePresent(embeddedHiveResourceDefs)) {
						// This is to check if URL def is added to the resource definition, if so update the resource def and accessType def
						if (embeddedHiveResourceDefs != null) {
							dbHiveServiceDef.setResources(embeddedHiveResourceDefs);
						}
						if (embeddedHiveAccessTypes != null) {
							dbHiveServiceDef.setAccessTypes(embeddedHiveAccessTypes);
						}
					}

					RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
					validator.validate(dbHiveServiceDef, Action.UPDATE);

					ret = svcStore.updateServiceDef(dbHiveServiceDef);
					if(ret==null){
						logger.error("Error while updating "+SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME+"service-def");
						throw new RuntimeException("Error while updating "+SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME+"service-def");
					}
				}
			}
			}catch(Exception e)
			{
				logger.error("Error while updating "+SERVICEDBSTORE_SERVICEDEFBYNAME_HIVE_NAME+"service-def", e);
			}
	}

	private boolean checkURLresourcePresent(List<RangerServiceDef.RangerResourceDef> resourceDefs) {
		boolean ret = false;
		for(RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
			if ( URL_RESOURCE_NAME.equals(resourceDef.getName()) ) {
				ret = true ;
				break;
			}
		}
		return ret;
	}
}