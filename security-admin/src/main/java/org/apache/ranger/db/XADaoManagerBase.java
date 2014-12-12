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

 package org.apache.ranger.db;

/**
 * 
 */

import javax.persistence.*;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.ranger.common.*;
import org.apache.ranger.common.db.*;
import org.apache.ranger.entity.*;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;


public abstract class XADaoManagerBase {
	final static Logger logger = Logger.getLogger(XADaoManagerBase.class);

	@Autowired
	protected RESTErrorUtil restErrorUtil;
	abstract public EntityManager getEntityManager();

	public XADaoManagerBase() {
	}

	public BaseDao<?> getDaoForClassType(int classType) {
		if (classType == AppConstants.CLASS_TYPE_AUTH_SESS) {
			return getXXAuthSession();
		}
		if (classType == AppConstants.CLASS_TYPE_USER_PROFILE) {
			return getXXPortalUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ASSET) {
			return getXXAsset();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_RESOURCE) {
			return getXXResource();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_CRED_STORE) {
			return getXXCredentialStore();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP) {
			return getXXGroup();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_USER) {
			return getXXUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP_USER) {
			return getXXGroupUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP_GROUP) {
			return getXXGroupGroup();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_PERM_MAP) {
			return getXXPermMap();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_AUDIT_MAP) {
			return getXXAuditMap();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_POLICY_EXPORT_AUDIT) {
			return getXXPolicyExportAudit();
		}
		if (classType == AppConstants.CLASS_TYPE_TRX_LOG) {
			return getXXTrxLog();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ACCESS_AUDIT) {
			return getXXAccessAudit();
		}

		logger.error("No DaoManager found for classType=" + classType, new Throwable());
		return null;
	}

	public BaseDao<?> getDaoForClassName(String className) {
		if (className.equals("XXDBBase")) {
			return getXXDBBase();
		}
		if (className.equals("XXAuthSession")) {
			return getXXAuthSession();
		}
		if (className.equals("XXPortalUser")) {
			return getXXPortalUser();
		}
		if (className.equals("XXPortalUserRole")) {
			return getXXPortalUserRole();
		}
		if (className.equals("XXAsset")) {
			return getXXAsset();
		}
		if (className.equals("XXResource")) {
			return getXXResource();
		}
		if (className.equals("XXCredentialStore")) {
			return getXXCredentialStore();
		}
		if (className.equals("XXGroup")) {
			return getXXGroup();
		}
		if (className.equals("XXUser")) {
			return getXXUser();
		}
		if (className.equals("XXGroupUser")) {
			return getXXGroupUser();
		}
		if (className.equals("XXGroupGroup")) {
			return getXXGroupGroup();
		}
		if (className.equals("XXPermMap")) {
			return getXXPermMap();
		}
		if (className.equals("XXAuditMap")) {
			return getXXAuditMap();
		}
		if (className.equals("XXPolicyExportAudit")) {
			return getXXPolicyExportAudit();
		}
		if (className.equals("XXTrxLog")) {
			return getXXTrxLog();
		}
		if (className.equals("XXAccessAudit")) {
			return getXXAccessAudit();
		}

		logger.error("No DaoManager found for className=" + className, new Throwable());
		return null;
	}

	public XXDBBaseDao getXXDBBase() {
		return new XXDBBaseDao(this);
	}

	public XXAuthSessionDao getXXAuthSession() {
		return new XXAuthSessionDao(this);
	}

	public XXPortalUserDao getXXPortalUser() {
		return new XXPortalUserDao(this);
	}

	public XXPortalUserRoleDao getXXPortalUserRole() {
		return new XXPortalUserRoleDao(this);
	}

	public XXAssetDao getXXAsset() {
		return new XXAssetDao(this);
	}

	public XXResourceDao getXXResource() {
		return new XXResourceDao(this);
	}

	public XXCredentialStoreDao getXXCredentialStore() {
		return new XXCredentialStoreDao(this);
	}

	public XXGroupDao getXXGroup() {
		return new XXGroupDao(this);
	}

	public XXUserDao getXXUser() {
		return new XXUserDao(this);
	}

	public XXGroupUserDao getXXGroupUser() {
		return new XXGroupUserDao(this);
	}

	public XXGroupGroupDao getXXGroupGroup() {
		return new XXGroupGroupDao(this);
	}

	public XXPermMapDao getXXPermMap() {
		return new XXPermMapDao(this);
	}

	public XXAuditMapDao getXXAuditMap() {
		return new XXAuditMapDao(this);
	}

	public XXPolicyExportAuditDao getXXPolicyExportAudit() {
		return new XXPolicyExportAuditDao(this);
	}

	public XXTrxLogDao getXXTrxLog() {
		return new XXTrxLogDao(this);
	}

	public XXAccessAuditDao getXXAccessAudit() {
		return new XXAccessAuditDao(this);
	}


}

