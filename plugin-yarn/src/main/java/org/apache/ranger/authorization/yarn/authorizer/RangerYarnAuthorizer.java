
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

package org.apache.ranger.authorization.yarn.authorizer;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.security.*;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import com.google.common.collect.Sets;

public class RangerYarnAuthorizer extends YarnAuthorizationProvider {
	public static final String ACCESS_TYPE_ADMIN_QUEUE = "admin-queue";
	public static final String ACCESS_TYPE_SUBMIT_APP  = "submit-app";
	public static final String ACCESS_TYPE_ADMIN       = "admin";

	private static final Log LOG = LogFactory.getLog(RangerYarnAuthorizer.class);

	private static volatile RangerYarnPlugin yarnPlugin = null;

	private AccessControlList admins = null;

	@Override
	public void init(Configuration conf) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.init()");
		}

		RangerYarnPlugin plugin = yarnPlugin;

		if(plugin == null) {
			synchronized(RangerYarnAuthorizer.class) {
				plugin = yarnPlugin;

				if(plugin == null) {
					plugin = new RangerYarnPlugin();
					plugin.init();
					
					yarnPlugin = plugin;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.init()");
		}
	}

	@Override
	public boolean checkPermission(AccessType accessType, PrivilegedEntity entity, UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.checkPermission(" + accessType + ", " + entity + ", " + ugi + ")");
		}

		boolean ret = false;

		RangerYarnPlugin plugin = yarnPlugin;

		if(plugin != null) {
			RangerYarnAccessRequest request = new RangerYarnAccessRequest(entity, getRangerAccessType(accessType), accessType.name(), ugi);

			RangerAccessResult result = plugin.isAccessAllowed(request);

			ret = result == null ? false : result.getIsAllowed();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.checkPermission(" + accessType + ", " + entity + ", " + ugi + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAdmin(UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.isAdmin(" + ugi + ")");
		}

		boolean ret = false;
		
		if(admins != null) {
			ret = admins.isUserAllowed(ugi);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.isAdmin(" + ugi + "): " + ret);
		}

		return ret;
	}

	@Override
	public void setAdmins(AccessControlList acl, UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.setAdmins(" + acl + ", " + ugi + ")");
		}

		admins = acl;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.setAdmins(" + acl + ", " + ugi + ")");
		}
	}

	@Override
	public void setPermission(PrivilegedEntity entity, Map<AccessType, AccessControlList> permission, UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.setPermission(" + entity + ", " + permission + ", " + ugi + ")");
		}

		RangerYarnPlugin plugin = yarnPlugin;

		if(plugin != null && entity != null && !MapUtils.isEmpty(permission) && ugi != null) {
			RangerYarnResource resource = new RangerYarnResource(entity);

			GrantRevokeRequest request = new GrantRevokeRequest();
			request.setResource(resource.getAsMap());
			request.setGrantor(ugi.getShortUserName());
			request.setDelegateAdmin(Boolean.FALSE);
			request.setEnableAudit(Boolean.TRUE);
			request.setReplaceExistingPermissions(Boolean.FALSE);
			request.setIsRecursive(Boolean.TRUE);

			for(Map.Entry<AccessType, AccessControlList> e : permission.entrySet()) {
				AccessType        accessType = e.getKey();
				AccessControlList acl        = e.getValue();
				
				Set<String> accessTypes = new HashSet<String>();
				accessTypes.add(getRangerAccessType(accessType));
				request.setAccessTypes(accessTypes);

				if(acl.isAllAllowed()) {
					Set<String> publicGroup = new HashSet<String>();
					publicGroup.add(RangerPolicyEngine.GROUP_PUBLIC);

					request.setUsers(null);
					request.setGroups(publicGroup);
				} else if(CollectionUtils.isEmpty(acl.getUsers()) && CollectionUtils.isEmpty(acl.getGroups())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug("grantAccess(): empty users and groups - skipped");
					}

					continue;
				} else {
					request.setUsers(getSet(acl.getUsers()));
					request.setGroups(getSet(acl.getGroups()));
				}

				try {
					plugin.grantAccess(request, plugin.getResultProcessor());
				} catch(Exception excp) {
					LOG.error("grantAccess(" + request + ") failed", excp);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.setPermission(" + entity + ", " + permission + ", " + ugi + ")");
		}
	}

	private static String getRangerAccessType(AccessType accessType) {
		String ret = null;

		switch(accessType) {
			case ADMINISTER_QUEUE:
				ret = RangerYarnAuthorizer.ACCESS_TYPE_ADMIN_QUEUE;
			break;

			case SUBMIT_APP:
				ret = RangerYarnAuthorizer.ACCESS_TYPE_SUBMIT_APP;
			break;
		}

		return ret;
	}

	private Set<String> getSet(Collection<String> strings) {
		Set<String> ret = null;

		if(! CollectionUtils.isEmpty(strings)) {
			if(strings instanceof Set<?>) {
				ret = (Set<String>)strings;
			} else {
				ret = new HashSet<String>();
				for(String str : strings) {
					ret.add(str);
				}
			}
		}

		return ret;
	}
}

class RangerYarnPlugin extends RangerBasePlugin {
	public RangerYarnPlugin() {
		super("yarn", "yarn");
	}

	@Override
	public void init() {
		super.init();

		RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();

		super.setResultProcessor(auditHandler);
	}
}

class RangerYarnResource extends RangerAccessResourceImpl {
	private static final String KEY_QUEUE = "queue";

	public RangerYarnResource(PrivilegedEntity entity) {
		setValue(KEY_QUEUE, entity != null ? entity.getName() : null);
	}
}

class RangerYarnAccessRequest extends RangerAccessRequestImpl {
	public RangerYarnAccessRequest(PrivilegedEntity entity, String accessType, String action, UserGroupInformation ugi) {
		super.setResource(new RangerYarnResource(entity));
		super.setAccessType(accessType);
		super.setUser(ugi.getShortUserName());
		super.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
		super.setAccessTime(StringUtil.getUTCDate());
		super.setClientIPAddress(getRemoteIp());
		super.setAction(accessType);
	}
	
	private static String getRemoteIp() {
		String ret = null ;
		InetAddress ip = Server.getRemoteIp() ;
		if (ip != null) {
			ret = ip.getHostAddress();
		}
		return ret ;
	}
}