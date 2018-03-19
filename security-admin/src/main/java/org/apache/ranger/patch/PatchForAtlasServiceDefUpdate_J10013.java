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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.util.CLIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchForAtlasServiceDefUpdate_J10013 extends BaseLoader {
	private static final Logger LOG = Logger.getLogger(PatchForAtlasServiceDefUpdate_J10013.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	ServiceDBStore svcDBStore;
	
	@Autowired
	RangerServiceService svcService;

	public static void main(String[] args) {
		LOG.info("main()");
		try {
			PatchForAtlasServiceDefUpdate_J10013 loader = (PatchForAtlasServiceDefUpdate_J10013) CLIUtil.getBean(PatchForAtlasServiceDefUpdate_J10013.class);
			loader.init();
			while (loader.isMoreToProcess()) {
				loader.load();
			}
			LOG.info("Load complete. Exiting!!!");
			System.exit(0);
		} catch (Exception e) {
			LOG.error("Error loading", e);
			System.exit(1);
		}
	}
	
	@Override
	public void init() throws Exception {
	}

	@Override
	public void execLoad() {
		LOG.info("==> PatchForAtlasServiceDefUpdate.execLoad()");
		try {
			updateAtlasServiceDef();
		} catch (Exception e) {
			LOG.error("Error whille updateAtlasServiceDef()data.", e);
		}
		LOG.info("<== PatchForAtlasServiceDefUpdate.execLoad()");
	}

	@Override
	public void printStats() {
		LOG.info("PatchForAtlasServiceDefUpdate data ");
	}

	private void updateAtlasServiceDef(){
		String serviceDefName=EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_ATLAS_NAME;
		XXServiceDefDao serviceDefDao = daoMgr.getXXServiceDef();
		XXServiceDef serviceDef = serviceDefDao.findByName(serviceDefName);
		// if service-def named 'atlas' does not exist then no need to process this patch further.
		if(serviceDef == null) {
			LOG.info(serviceDefName + ": service-def not found. No patching is needed");
			return;
		}
		// if older atlas service-def doesn't exist then no need to process this patch further.
		if(!checkIfHasOlderServiceDef(serviceDef)) {
			LOG.info("Older version of "+serviceDefName + " service-def not found. No patching is needed");
			return;
		}
		String suffix = null;
		for (int i = 1; true; i++) {
			suffix = ".v" + i;
			if (serviceDefDao.findByName(serviceDefName + suffix) == null) {
				break;
			}
		}
		String serviceDefNewName = serviceDefName + suffix;
		LOG.info("Renaming service-def " + serviceDefName + " as " + serviceDefNewName);
		serviceDef.setName(serviceDefNewName);
		serviceDefDao.update(serviceDef);
		LOG.info("Renamed service-def " + serviceDefName + " as " + serviceDefNewName);
		XXServiceDao serviceDao = daoMgr.getXXService();
		List<XXService> services = serviceDao.findByServiceDefId(serviceDef.getId());
		if (CollectionUtils.isNotEmpty(services)) {
			for (XXService service : services) {
				String serviceName = service.getName();
				String serviceNewName = serviceName + suffix;
				LOG.info("Renaming service " + serviceName + " as " + serviceNewName);
				if (serviceDao.findByName(serviceNewName) != null) {
					LOG.warn("Another service named " + serviceNewName + " already exists. Not renaming " + serviceName);
					continue;
				}
				service.setName(serviceNewName);
				serviceDao.update(service);
				LOG.info("Renamed service " + serviceName + " as " + serviceNewName);
			}
		}
	}

	/*
	 * This method shall check whether atlas service def resources and access types
	 * are matching with older service def resources and access types or not.
	 * returns true if all resources and access types matches with older service def
	 * resources and access types.
	 */
	private boolean checkIfHasOlderServiceDef(XXServiceDef serviceDef) {
		boolean result = true;
		Set<String> atlasResources = new HashSet<>(Arrays.asList("entity", "type", "operation", "taxonomy", "term"));
		XXResourceDefDao resourceDefDao=daoMgr.getXXResourceDef();
		List<XXResourceDef> xxResourceDefs = resourceDefDao.findByServiceDefId(serviceDef.getId());
		for (XXResourceDef xxResourceDef : xxResourceDefs) {
			if(! atlasResources.contains(xxResourceDef.getName())) {
				result = false;
				break;
			}
		}
		if(result){
			Set<String> atlasAccessTypes = new HashSet<>(Arrays.asList("read", "create", "update", "delete", "all"));
			XXAccessTypeDefDao accessTypeDefDao=daoMgr.getXXAccessTypeDef();
			List<XXAccessTypeDef> xxAccessTypeDefs = accessTypeDefDao.findByServiceDefId(serviceDef.getId());
			for (XXAccessTypeDef xxAccessTypeDef : xxAccessTypeDefs) {
				if(! atlasAccessTypes.contains(xxAccessTypeDef.getName())) {
					result = false;
					break;
				}
			}
		}
		return result;
	}
}