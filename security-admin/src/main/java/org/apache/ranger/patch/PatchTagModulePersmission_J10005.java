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

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXModuleDef;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.service.XPortalUserService;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXPortalUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchTagModulePersmission_J10005 extends BaseLoader {
	private static Logger logger = Logger
			.getLogger(PatchTagModulePersmission_J10005.class);

	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	XPortalUserService xPortalUserService;

	@Autowired
	RangerDaoManager daoManager;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchTagModulePersmission_J10005 loader = (PatchTagModulePersmission_J10005) CLIUtil
					.getBean(PatchTagModulePersmission_J10005.class);

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
		logger.info("==> PermissionPatch.execLoad()");
		assignPermissionOnTagModuleToAdminUsers();
		logger.info("<== PermissionPatch.execLoad()");
	}

	public void assignPermissionOnTagModuleToAdminUsers() {
		int countUserPermissionUpdated = 0;
		XXModuleDef xModDef = daoManager.getXXModuleDef().findByModuleName(RangerConstants.MODULE_TAG_BASED_POLICIES);
		if(xModDef==null){
			return;
		}
		List<XXPortalUser> allAdminUsers = daoManager.getXXPortalUser().findByRole(RangerConstants.ROLE_SYS_ADMIN);
		if(!CollectionUtils.isEmpty(allAdminUsers)){
			for (XXPortalUser xPortalUser : allAdminUsers) {
				VXPortalUser vPortalUser = xPortalUserService.populateViewBean(xPortalUser);
				if(vPortalUser!=null){
					vPortalUser.setUserRoleList(daoManager.getXXPortalUserRole().findXPortalUserRolebyXPortalUserId(vPortalUser.getId()));
					xUserMgr.createOrUpdateUserPermisson(vPortalUser,xModDef.getId(), false);
					countUserPermissionUpdated += 1;
					logger.info("Added '" + xModDef.getModule() + "' permission to user '" + xPortalUser.getLoginId() + "'");
				}
			}
		}
		logger.info(countUserPermissionUpdated + " permissions were assigned");
	}

	@Override
	public void printStats() {
	}

}
