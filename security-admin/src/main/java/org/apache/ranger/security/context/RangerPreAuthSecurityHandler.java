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

package org.apache.ranger.security.context;

import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("rangerPreAuthSecurityHandler")
public class RangerPreAuthSecurityHandler {
	Logger logger = Logger.getLogger(RangerPreAuthSecurityHandler.class);

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerAPIMapping rangerAPIMapping;

	public boolean isAPIAccessible(String methodName) throws Exception {

		if (methodName == null) {
			return false;
		}

		UserSessionBase userSession = ContextUtil.getCurrentUserSession();
		if (userSession == null) {
			logger.warn("WARNING: UserSession found null. Some non-authorized user might be trying to access the API.");
			return false;
		}

		if (userSession.isUserAdmin()) {
			if (logger.isDebugEnabled()) {
				logger.debug("WARNING: Logged in user is System Admin, System Admin is allowed to access all the tabs except Key Manager."
						+ "Reason for returning true is, In few cases system admin needs to have access on Key Manager tabs as well.");
			}
			return true;
		}

		Set<String> associatedTabs = rangerAPIMapping.getAssociatedTabsWithAPI(methodName);
		if (CollectionUtils.isEmpty(associatedTabs)) {
			return true;
		}
		return isAPIAccessible(associatedTabs);
	}

	public boolean isAPIAccessible(Set<String> associatedTabs) throws Exception {

		XXUser xUser = daoManager.getXXUser().findByUserName(ContextUtil.getCurrentUserLoginId());
		if (xUser == null) {
			restErrorUtil.createRESTException("x_user cannot be null.", MessageEnums.ERROR_SYSTEM);
		}

		List<String> accessibleModules = daoManager.getXXModuleDef().findAccessibleModulesByUserId(ContextUtil.getCurrentUserId(), xUser.getId());
		if (CollectionUtils.containsAny(accessibleModules, associatedTabs)) {
			return true;
		}

		throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not allowed to access the API", true);
	}

}
