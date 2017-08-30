
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.security.*;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import com.google.common.collect.Sets;
import org.apache.ranger.plugin.util.RangerPerfTracer;

public class RangerYarnAuthorizer extends YarnAuthorizationProvider {
	public static final String ACCESS_TYPE_ADMIN_QUEUE = "admin-queue";
	public static final String ACCESS_TYPE_SUBMIT_APP  = "submit-app";
	public static final String ACCESS_TYPE_ADMIN       = "admin";

    public static final String KEY_RESOURCE_QUEUE = "queue";

    private static boolean yarnAuthEnabled = RangerHadoopConstants.RANGER_ADD_YARN_PERMISSION_DEFAULT;

	private static final Log LOG = LogFactory.getLog(RangerYarnAuthorizer.class);

	private static final Log PERF_YARNAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("yarnauth.request");

	private static volatile RangerYarnPlugin yarnPlugin = null;

	private AccessControlList admins = null;
	private Map<PrivilegedEntity, Map<AccessType, AccessControlList>> yarnAcl = new HashMap<PrivilegedEntity, Map<AccessType, AccessControlList>>();

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

		RangerYarnAuthorizer.yarnAuthEnabled = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.RANGER_ADD_YARN_PERMISSION_PROP, RangerHadoopConstants.RANGER_ADD_YARN_PERMISSION_DEFAULT);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.init()");
		}
	}

	@Override
	public boolean checkPermission(AccessType accessType, PrivilegedEntity entity, UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.checkPermission(" + accessType + ", " + toString(entity) + ", " + ugi + ")");
		}

		boolean                ret          = false;
		RangerYarnPlugin       plugin       = yarnPlugin;
		RangerYarnAuditHandler auditHandler = null;
		RangerAccessResult     result       = null;
		String				   clusterName  = yarnPlugin.getClusterName();

		RangerPerfTracer perf = null;
		RangerPerfTracer yarnAclPerf = null;

		if(plugin != null) {

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_YARNAUTH_REQUEST_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_YARNAUTH_REQUEST_LOG, "RangerYarnAuthorizer.checkPermission(entity=" + entity + ")");
			}

			RangerYarnAccessRequest request = new RangerYarnAccessRequest(entity, getRangerAccessType(accessType), accessType.name(), ugi, clusterName);

			auditHandler = new RangerYarnAuditHandler();

			result = plugin.isAccessAllowed(request, auditHandler);
		}

		if(RangerYarnAuthorizer.yarnAuthEnabled && (result == null || !result.getIsAccessDetermined())) {

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_YARNAUTH_REQUEST_LOG)) {
				yarnAclPerf = RangerPerfTracer.getPerfTracer(PERF_YARNAUTH_REQUEST_LOG, "RangerYarnNativeAuthorizer.isAllowedByYarnAcl(entity=" + entity + ")");
			}

			ret = isAllowedByYarnAcl(accessType, entity, ugi, auditHandler);
		} else {
			ret = result != null && result.getIsAllowed();
		}

		if(auditHandler != null) {
			auditHandler.flushAudit();
		}

		RangerPerfTracer.log(yarnAclPerf);

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.checkPermission(" + accessType + ", " + toString(entity) + ", " + ugi + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAdmin(UserGroupInformation ugi) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.isAdmin(" + ugi + ")");
		}

		boolean ret = false;
		
		AccessControlList admins = this.admins;

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
			LOG.debug("==> RangerYarnAuthorizer.setPermission(" + toString(entity) + ", " + permission + ", " + ugi + ")");
		}

		yarnAcl.put(entity, permission);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.setPermission(" + toString(entity) + ", " + permission + ", " + ugi + ")");
		}
	}

	public boolean isAllowedByYarnAcl(AccessType accessType, PrivilegedEntity entity, UserGroupInformation ugi, RangerYarnAuditHandler auditHandler) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuthorizer.isAllowedByYarnAcl(" + accessType + ", " + toString(entity) + ", " + ugi + ")");
		}

		boolean ret = false;

		for(Map.Entry<PrivilegedEntity, Map<AccessType, AccessControlList>> e : yarnAcl.entrySet()) {
			PrivilegedEntity                   aclEntity         = e.getKey();
			Map<AccessType, AccessControlList> entityPermissions = e.getValue();

			AccessControlList acl = entityPermissions == null ? null : entityPermissions.get(accessType);

			if(acl != null && acl.isUserAllowed(ugi) && isSelfOrChildOf(entity, aclEntity)) {
			    ret = true;
		    	break;
            }
		}

		if(auditHandler != null) {
			auditHandler.logYarnAclEvent(ret);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuthorizer.isAllowedByYarnAcl(" + accessType + ", " + toString(entity) + ", " + ugi + "): " + ret);
		}

		return ret;
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

	private boolean isSelfOrChildOf(PrivilegedEntity queue, PrivilegedEntity parentQueue) {
		boolean ret = queue.equals(parentQueue);

		if(!ret && queue.getType() == EntityType.QUEUE) {
			String queueName       = queue.getName();
			String parentQueueName = parentQueue.getName();

			if(queueName.contains(".") && !StringUtil.isEmpty(parentQueueName)) {
				if(parentQueueName.charAt(parentQueueName.length() - 1) != '.') {
					parentQueueName += ".";
				}

				ret = queueName.startsWith(parentQueueName);
			}
		}

		return ret;
	}

	private String toString(PrivilegedEntity entity) {
		if(entity != null) {
			return "{name=" + entity.getName() + "; type=" + entity.getType() + "}";
		}

		return "null";
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
	public RangerYarnResource(PrivilegedEntity entity) {
		setValue(RangerYarnAuthorizer.KEY_RESOURCE_QUEUE, entity != null ? entity.getName() : null);
	}
}

class RangerYarnAccessRequest extends RangerAccessRequestImpl {
	public RangerYarnAccessRequest(PrivilegedEntity entity, String accessType, String action, UserGroupInformation ugi, String clusterName) {
		super.setResource(new RangerYarnResource(entity));
		super.setAccessType(accessType);
		super.setUser(ugi.getShortUserName());
		super.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
		super.setAccessTime(new Date());
		super.setClientIPAddress(getRemoteIp());
		super.setAction(action);
		super.setClusterName(clusterName);
	}
	
	private static String getRemoteIp() {
		String ret = null;
		InetAddress ip = Server.getRemoteIp();
		if (ip != null) {
			ret = ip.getHostAddress();
		}
		return ret;
	}
}

class RangerYarnAuditHandler extends RangerDefaultAuditHandler {
	private static final Log LOG = LogFactory.getLog(RangerYarnAuditHandler.class);

	private static final String YarnModuleName = RangerConfiguration.getInstance().get(RangerHadoopConstants.AUDITLOG_YARN_MODULE_ACL_NAME_PROP , RangerHadoopConstants.DEFAULT_YARN_MODULE_ACL_NAME);

	private boolean         isAuditEnabled = false;
	private AuthzAuditEvent auditEvent     = null;

	public RangerYarnAuditHandler() {
	}

	@Override
	public void processResult(RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuditHandler.logAudit(" + result + ")");
		}

		if(! isAuditEnabled && result.getIsAudited()) {
			isAuditEnabled = true;
		}

		auditEvent = super.getAuthzEvents(result);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuditHandler.logAudit(" + result + "): " + auditEvent);
		}
	}

	public void logYarnAclEvent(boolean accessGranted) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuditHandler.logYarnAclEvent(" + accessGranted + ")");
		}

		if(auditEvent != null) {
			auditEvent.setAccessResult((short) (accessGranted ? 1 : 0));
			auditEvent.setAclEnforcer(YarnModuleName);
			auditEvent.setPolicyId(-1);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuditHandler.logYarnAclEvent(" + accessGranted + "): " + auditEvent);
		}
	}

	public void flushAudit() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerYarnAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}

		if(isAuditEnabled) {
			super.logAuthzAudit(auditEvent);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerYarnAuditHandler.flushAudit(" + isAuditEnabled + ", " + auditEvent + ")");
		}
	}
}
