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
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.service.XPortalUserService;

import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXPortalUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatchPersmissionModel_J10003 extends BaseLoader {
	private static Logger logger = Logger
			.getLogger(PatchPersmissionModel_J10003.class);

	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	XPortalUserService xPortalUserService;

	@Autowired
	RangerDaoManager daoManager;

	public static void main(String[] args) {
		logger.info("main()");
		try {
			PatchPersmissionModel_J10003 loader = (PatchPersmissionModel_J10003) CLIUtil
					.getBean(PatchPersmissionModel_J10003.class);

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
		printStats();
		logger.info("<== PermissionPatch.execLoad()");
	}

	@Override
	public void printStats() {
		int countUserPermissionUpdated = 1;
		List<XXPortalUser> allPortalUser = daoManager.getXXPortalUser()
				.findAllXPortalUser();
		List<VXPortalUser> vPortalUsers = new ArrayList<VXPortalUser>();
		for (XXPortalUser xPortalUser : allPortalUser) {
			VXPortalUser vPortalUser = xPortalUserService
					.populateViewBean(xPortalUser);
			vPortalUsers.add(vPortalUser);
			vPortalUser.setUserRoleList(daoManager.getXXPortalUser()
					.findXPortalUserRolebyXPortalUserId(vPortalUser.getId()));
			xUserMgr.assignPermissionToUser(vPortalUser, false);
			countUserPermissionUpdated += 1;
			logger.info(" Permission was assigned to UserId - "
					+ xPortalUser.getId());
		}
		logger.info(countUserPermissionUpdated + " permissions where assigned");
	}
}
