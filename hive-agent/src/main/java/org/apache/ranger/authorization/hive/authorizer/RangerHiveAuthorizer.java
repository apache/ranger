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

package org.apache.ranger.authorization.hive.authorizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskTypeDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

import com.google.common.collect.Sets;

import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerRequestedResources;

public class RangerHiveAuthorizer extends RangerHiveAuthorizerBase {
	private static final Log LOG = LogFactory.getLog(RangerHiveAuthorizer.class);

	private static final Log PERF_HIVEAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("hiveauth.request");

	private static final char COLUMN_SEP = ',';

	private static final String HIVE_CONF_VAR_QUERY_STRING = "hive.query.string";

	private static volatile RangerHivePlugin hivePlugin = null;

	public RangerHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
								  HiveConf                   hiveConf,
								  HiveAuthenticationProvider hiveAuthenticator,
								  HiveAuthzSessionContext    sessionContext) {
		super(metastoreClientFactory, hiveConf, hiveAuthenticator, sessionContext);

		LOG.debug("RangerHiveAuthorizer.RangerHiveAuthorizer()");

		RangerHivePlugin plugin = hivePlugin;
		
		if(plugin == null) {
			synchronized(RangerHiveAuthorizer.class) {
				plugin = hivePlugin;

				if(plugin == null) {
					String appType = "unknown";

					if(sessionContext != null) {
						switch(sessionContext.getClientType()) {
							case HIVECLI:
								appType = "hiveCLI";
							break;

							case HIVESERVER2:
								appType = "hiveServer2";
							break;
						}
					}

					plugin = new RangerHivePlugin(appType);
					plugin.init();

					hivePlugin = plugin;
				}
			}
		}
	}


	/**
	 * Grant privileges for principals on the object
	 * @param hivePrincipals
	 * @param hivePrivileges
	 * @param hivePrivObject
	 * @param grantorPrincipal
	 * @param grantOption
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public void grantPrivileges(List<HivePrincipal> hivePrincipals,
								List<HivePrivilege> hivePrivileges,
								HivePrivilegeObject hivePrivObject,
								HivePrincipal       grantorPrincipal,
								boolean             grantOption)
										throws HiveAuthzPluginException, HiveAccessControlException {
		if(! RangerHivePlugin.UpdateXaPoliciesOnGrantRevoke) {
			throw new HiveAuthzPluginException("GRANT/REVOKE not supported in Ranger HiveAuthorizer. Please use Ranger Security Admin to setup access control.");
		}

		RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

		try {
			RangerHiveResource resource = getHiveResource(HiveOperationType.GRANT_PRIVILEGE, hivePrivObject);
			GrantRevokeRequest request  = createGrantRevokeData(resource, hivePrincipals, hivePrivileges, grantorPrincipal, grantOption);
			request.setClusterName(hivePlugin.getClusterName());

			LOG.info("grantPrivileges(): " + request);
			if(LOG.isDebugEnabled()) {
				LOG.debug("grantPrivileges(): " + request);
			}

			hivePlugin.grantAccess(request, auditHandler);
		} catch(Exception excp) {
			throw new HiveAccessControlException(excp);
		} finally {
			auditHandler.flushAudit();
		}
	}

	/**
	 * Revoke privileges for principals on the object
	 * @param hivePrincipals
	 * @param hivePrivileges
	 * @param hivePrivObject
	 * @param grantorPrincipal
	 * @param grantOption
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public void revokePrivileges(List<HivePrincipal> hivePrincipals,
								 List<HivePrivilege> hivePrivileges,
								 HivePrivilegeObject hivePrivObject,
								 HivePrincipal       grantorPrincipal,
								 boolean             grantOption)
										 throws HiveAuthzPluginException, HiveAccessControlException {
		if(! RangerHivePlugin.UpdateXaPoliciesOnGrantRevoke) {
			throw new HiveAuthzPluginException("GRANT/REVOKE not supported in Ranger HiveAuthorizer. Please use Ranger Security Admin to setup access control.");
		}

		RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

		try {
			RangerHiveResource resource = getHiveResource(HiveOperationType.REVOKE_PRIVILEGE, hivePrivObject);
			GrantRevokeRequest request  = createGrantRevokeData(resource, hivePrincipals, hivePrivileges, grantorPrincipal, grantOption);
			request.setClusterName(hivePlugin.getClusterName());

			LOG.info("revokePrivileges(): " + request);
			if(LOG.isDebugEnabled()) {
				LOG.debug("revokePrivileges(): " + request);
			}

			hivePlugin.revokeAccess(request, auditHandler);
		} catch(Exception excp) {
			throw new HiveAccessControlException(excp);
		} finally {
			auditHandler.flushAudit();
		}
	}

	/**
	 * Check if user has privileges to do this action on these objects
	 * @param hiveOpType
	 * @param inputHObjs
	 * @param outputHObjs
	 * @param context
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
	@Override
	public void checkPrivileges(HiveOperationType         hiveOpType,
								List<HivePrivilegeObject> inputHObjs,
							    List<HivePrivilegeObject> outputHObjs,
							    HiveAuthzContext          context)
		      throws HiveAuthzPluginException, HiveAccessControlException {
		UserGroupInformation ugi = getCurrentUserGroupInfo();

		if(ugi == null) {
			throw new HiveAccessControlException("Permission denied: user information not available");
		}

		RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

		RangerPerfTracer perf = null;

		try {
			HiveAuthzSessionContext sessionContext = getHiveAuthzSessionContext();
			String                  user           = ugi.getShortUserName();
			Set<String>             groups         = Sets.newHashSet(ugi.getGroupNames());
			String clusterName = hivePlugin.getClusterName();

			if(LOG.isDebugEnabled()) {
				LOG.debug(toString(hiveOpType, inputHObjs, outputHObjs, context, sessionContext));
			}

			if(hiveOpType == HiveOperationType.DFS) {
				handleDfsCommand(hiveOpType, inputHObjs, user, auditHandler);

				return;
			}

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_HIVEAUTH_REQUEST_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_HIVEAUTH_REQUEST_LOG, "RangerHiveAuthorizer.checkPrivileges(hiveOpType=" + hiveOpType + ")");
			}

			List<RangerHiveAccessRequest> requests = new ArrayList<RangerHiveAccessRequest>();

			if(!CollectionUtils.isEmpty(inputHObjs)) {
				for(HivePrivilegeObject hiveObj : inputHObjs) {
					RangerHiveResource resource = getHiveResource(hiveOpType, hiveObj);

					if (resource == null) { // possible if input object/object is of a kind that we don't currently authorize
						continue;
					}

					String 	path         		= hiveObj.getObjectName();
					HiveObjectType hiveObjType  = resource.getObjectType();

					if(hiveObjType == HiveObjectType.URI && isPathInFSScheme(path)) {
						FsAction permission = getURIAccessType(hiveOpType);

						if(!isURIAccessAllowed(user, permission, path, getHiveConf())) {
							throw new HiveAccessControlException(String.format("Permission denied: user [%s] does not have [%s] privilege on [%s]", user, permission.name(), path));
						}

						continue;
					}

					HiveAccessType accessType = getAccessType(hiveObj, hiveOpType, hiveObjType, true);

					if(accessType == HiveAccessType.NONE) {
						continue;
					}

					if(!existsByResourceAndAccessType(requests, resource, accessType)) {
						RangerHiveAccessRequest request = new RangerHiveAccessRequest(resource, user, groups, hiveOpType, accessType, context, sessionContext, clusterName);

						requests.add(request);
					}
				}
			} else {
				// this should happen only for SHOWDATABASES
				if (hiveOpType == HiveOperationType.SHOWDATABASES) {
					RangerHiveResource resource = new RangerHiveResource(HiveObjectType.DATABASE, null);
					RangerHiveAccessRequest request = new RangerHiveAccessRequest(resource, user, groups, hiveOpType.name(), HiveAccessType.USE, context, sessionContext, clusterName);
					requests.add(request);
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerHiveAuthorizer.checkPrivileges: Unexpected operation type[" + hiveOpType + "] received with empty input objects list!");
					}
				}
			}

			if(!CollectionUtils.isEmpty(outputHObjs)) {
				for(HivePrivilegeObject hiveObj : outputHObjs) {
					RangerHiveResource resource = getHiveResource(hiveOpType, hiveObj);

					if (resource == null) { // possible if input object/object is of a kind that we don't currently authorize
						continue;
					}

					String   path       = hiveObj.getObjectName();
					HiveObjectType hiveObjType  = resource.getObjectType();

					if(hiveObjType == HiveObjectType.URI  && isPathInFSScheme(path)) {
						FsAction permission = getURIAccessType(hiveOpType);

		                if(!isURIAccessAllowed(user, permission, path, getHiveConf())) {
		    				throw new HiveAccessControlException(String.format("Permission denied: user [%s] does not have [%s] privilege on [%s]", user, permission.name(), path));
		                }

						continue;
					}

					HiveAccessType accessType = getAccessType(hiveObj, hiveOpType, hiveObjType, false);

					if(accessType == HiveAccessType.NONE) {
						continue;
					}

					if(!existsByResourceAndAccessType(requests, resource, accessType)) {
						RangerHiveAccessRequest request = new RangerHiveAccessRequest(resource, user, groups, hiveOpType, accessType, context, sessionContext, clusterName);

						requests.add(request);
					}
				}
			}

			buildRequestContextWithAllAccessedResources(requests);

			for(RangerHiveAccessRequest request : requests) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("request: " + request);
				}
				RangerHiveResource resource = (RangerHiveResource)request.getResource();
				RangerAccessResult result   = null;

				if(resource.getObjectType() == HiveObjectType.COLUMN && StringUtils.contains(resource.getColumn(), COLUMN_SEP)) {
					List<RangerAccessRequest> colRequests = new ArrayList<RangerAccessRequest>();

					String[] columns = StringUtils.split(resource.getColumn(), COLUMN_SEP);

					// in case of multiple columns, original request is not sent to the plugin; hence service-def will not be set
					resource.setServiceDef(hivePlugin.getServiceDef());

					for(String column : columns) {
						if (column != null) {
							column = column.trim();
						}
						if(StringUtils.isBlank(column)) {
							continue;
						}

						RangerHiveResource colResource = new RangerHiveResource(HiveObjectType.COLUMN, resource.getDatabase(), resource.getTable(), column);

						RangerHiveAccessRequest colRequest = request.copy();
						colRequest.setResource(colResource);

						colRequests.add(colRequest);
					}

					Collection<RangerAccessResult> colResults = hivePlugin.isAccessAllowed(colRequests, auditHandler);

					if(colResults != null) {
						for(RangerAccessResult colResult : colResults) {
							result = colResult;

							if(result != null && !result.getIsAllowed()) {
								break;
							}
						}
					}
				} else {
					result = hivePlugin.isAccessAllowed(request, auditHandler);
				}

				if((result == null || result.getIsAllowed()) && isBlockAccessIfRowfilterColumnMaskSpecified(hiveOpType, request)) {
					// check if row-filtering is applicable for the table/view being accessed
					HiveAccessType     savedAccessType = request.getHiveAccessType();
					RangerHiveResource tblResource     = new RangerHiveResource(HiveObjectType.TABLE, resource.getDatabase(), resource.getTable());

					request.setHiveAccessType(HiveAccessType.SELECT); // filtering/masking policies are defined only for SELECT
					request.setResource(tblResource);

					RangerAccessResult rowFilterResult = getRowFilterResult(request);

					if (isRowFilterEnabled(rowFilterResult)) {
						if(result == null) {
							result = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, rowFilterResult.getServiceName(), rowFilterResult.getServiceDef(), request);
						}

						result.setIsAllowed(false);
						result.setPolicyId(rowFilterResult.getPolicyId());
						result.setReason("User does not have acces to all rows of the table");
					} else {
						// check if masking is enabled for any column in the table/view
						request.setResourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

						RangerAccessResult dataMaskResult = getDataMaskResult(request);

						if (isDataMaskEnabled(dataMaskResult)) {
							if(result == null) {
								result = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, dataMaskResult.getServiceName(), dataMaskResult.getServiceDef(), request);
							}

							result.setIsAllowed(false);
							result.setPolicyId(dataMaskResult.getPolicyId());
							result.setReason("User does not have access to unmasked column values");
						}
					}

					request.setHiveAccessType(savedAccessType);
					request.setResource(resource);

					if(result != null && !result.getIsAllowed()) {
						auditHandler.processResult(result);
					}
				}

				if(result == null || !result.getIsAllowed()) {
					String path = resource.getAsString();
					path = (path == null) ? "Unknown resource!!" : buildPathForException(path, hiveOpType);
					throw new HiveAccessControlException(String.format("Permission denied: user [%s] does not have [%s] privilege on [%s]",
														 user, request.getHiveAccessType().name(), path));
				}
			}
		} finally {
			auditHandler.flushAudit();
			RangerPerfTracer.log(perf);
		}
	}

	/**
	 * Check if user has privileges to do this action on these objects
	 * @param objs
	 * @param context
	 * @throws HiveAuthzPluginException
	 * @throws HiveAccessControlException
	 */
    // Commented out to avoid build errors until this interface is stable in Hive Branch
	// @Override
	public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> objs,
														  HiveAuthzContext          context)
		      throws HiveAuthzPluginException, HiveAccessControlException {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> filterListCmdObjects(%s, %s)", objs, context));
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_HIVEAUTH_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_HIVEAUTH_REQUEST_LOG, "RangerHiveAuthorizer.filterListCmdObjects()");
		}

		List<HivePrivilegeObject> ret = null;

		// bail out early if nothing is there to validate!
		if (objs == null) {
			LOG.debug("filterListCmdObjects: meta objects list was null!");
		} else if (objs.isEmpty()) {
			LOG.debug("filterListCmdObjects: meta objects list was empty!");
			ret = objs;
		} else if (getCurrentUserGroupInfo() == null) {
			/*
			 * This is null for metastore and there doesn't seem to be a way to tell if one is running as metastore or hiveserver2!
			 */
			LOG.warn("filterListCmdObjects: user information not available");
			ret = objs;
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("filterListCmdObjects: number of input objects[" + objs.size() + "]");
			}
			// get user/group info
			UserGroupInformation ugi = getCurrentUserGroupInfo(); // we know this can't be null since we checked it above!
			HiveAuthzSessionContext sessionContext = getHiveAuthzSessionContext();
			String user = ugi.getShortUserName();
			Set<String> groups = Sets.newHashSet(ugi.getGroupNames());
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("filterListCmdObjects: user[%s], groups%s", user, groups));
			}
			
			if (ret == null) { // if we got any items to filter then we can't return back a null.  We must return back a list even if its empty.
				ret = new ArrayList<HivePrivilegeObject>(objs.size());
			}
			for (HivePrivilegeObject privilegeObject : objs) {
				if (LOG.isDebugEnabled()) {
					HivePrivObjectActionType actionType = privilegeObject.getActionType();
					HivePrivilegeObjectType objectType = privilegeObject.getType();
					String objectName = privilegeObject.getObjectName();
					String dbName = privilegeObject.getDbname();
					List<String> columns = privilegeObject.getColumns();
					List<String> partitionKeys = privilegeObject.getPartKeys();
					String commandString = context == null ? null : context.getCommandString();
					String ipAddress = context == null ? null : context.getIpAddress();

					final String format = "filterListCmdObjects: actionType[%s], objectType[%s], objectName[%s], dbName[%s], columns[%s], partitionKeys[%s]; context: commandString[%s], ipAddress[%s]";
					LOG.debug(String.format(format, actionType, objectType, objectName, dbName, columns, partitionKeys, commandString, ipAddress));
				}
				
				RangerHiveResource resource = createHiveResource(privilegeObject);
				if (resource == null) {
					LOG.error("filterListCmdObjects: RangerHiveResource returned by createHiveResource is null");
				} else {
					RangerHiveAccessRequest request = new RangerHiveAccessRequest(resource, user, groups, context, sessionContext, hivePlugin.getClusterName());
					RangerAccessResult result = hivePlugin.isAccessAllowed(request);
					if (result == null) {
						LOG.error("filterListCmdObjects: Internal error: null RangerAccessResult object received back from isAccessAllowed()!");
					} else if (!result.getIsAllowed()) {
						if (!LOG.isDebugEnabled()) {
							String path = resource.getAsString();
							LOG.debug(String.format("filterListCmdObjects: Permission denied: user [%s] does not have [%s] privilege on [%s]. resource[%s], request[%s], result[%s]",
									user, request.getHiveAccessType().name(), path, resource, request, result));
						}
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug(String.format("filterListCmdObjects: access allowed. resource[%s], request[%s], result[%s]", resource, request, result));
						}
						ret.add(privilegeObject);
					}
				}
			}
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			int count = ret == null ? 0 : ret.size();
			LOG.debug(String.format("<== filterListCmdObjects: count[%d], ret[%s]", count, ret));
		}
		return ret;
	}

	@Override
	public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext queryContext, List<HivePrivilegeObject> hiveObjs) throws SemanticException {
		List<HivePrivilegeObject> ret = new ArrayList<HivePrivilegeObject>();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> applyRowFilterAndColumnMasking(" + queryContext + ", objCount=" + hiveObjs.size() + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_HIVEAUTH_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_HIVEAUTH_REQUEST_LOG, "RangerHiveAuthorizer.applyRowFilterAndColumnMasking()");
		}

		if(CollectionUtils.isNotEmpty(hiveObjs)) {
			for (HivePrivilegeObject hiveObj : hiveObjs) {
				HivePrivilegeObjectType hiveObjType = hiveObj.getType();

				if(hiveObjType == null) {
					hiveObjType = HivePrivilegeObjectType.TABLE_OR_VIEW;
				}

				if(LOG.isDebugEnabled()) {
					LOG.debug("applyRowFilterAndColumnMasking(hiveObjType=" + hiveObjType + ")");
				}

				boolean needToTransform = false;

				if (hiveObjType == HivePrivilegeObjectType.TABLE_OR_VIEW) {
					String database = hiveObj.getDbname();
					String table    = hiveObj.getObjectName();

					String rowFilterExpr = getRowFilterExpression(queryContext, database, table);

					if (StringUtils.isNotBlank(rowFilterExpr)) {
						if(LOG.isDebugEnabled()) {
							LOG.debug("rowFilter(database=" + database + ", table=" + table + "): " + rowFilterExpr);
						}

						hiveObj.setRowFilterExpression(rowFilterExpr);
						needToTransform = true;
					}

					if (CollectionUtils.isNotEmpty(hiveObj.getColumns())) {
						List<String> columnTransformers = new ArrayList<String>();

						for (String column : hiveObj.getColumns()) {
							boolean isColumnTransformed = addCellValueTransformerAndCheckIfTransformed(queryContext, database, table, column, columnTransformers);

							if(LOG.isDebugEnabled()) {
								LOG.debug("addCellValueTransformerAndCheckIfTransformed(database=" + database + ", table=" + table + ", column=" + column + "): " + isColumnTransformed);
							}

							needToTransform = needToTransform || isColumnTransformed;
						}

						hiveObj.setCellValueTransformers(columnTransformers);
					}
				}

				if (needToTransform) {
					ret.add(hiveObj);
				}
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== applyRowFilterAndColumnMasking(" + queryContext + ", objCount=" + hiveObjs.size() + "): retCount=" + ret.size());
		}

		return ret;
	}

	@Override
	public boolean needTransform() {
		return true; // TODO: derive from the policies
	}

	private RangerAccessResult getDataMaskResult(RangerHiveAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> getDataMaskResult(request=" + request + ")");
		}

		RangerAccessResult ret = hivePlugin.evalDataMaskPolicies(request, null);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== getDataMaskResult(request=" + request + "): ret=" + ret);
		}

		return ret;
	}

	private RangerAccessResult getRowFilterResult(RangerHiveAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> getRowFilterResult(request=" + request + ")");
		}

		RangerAccessResult ret = hivePlugin.evalRowFilterPolicies(request, null);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== getRowFilterResult(request=" + request + "): ret=" + ret);
		}

		return ret;
	}

	private boolean isDataMaskEnabled(RangerAccessResult result) {
		return result != null && result.isMaskEnabled();
	}

	private boolean isRowFilterEnabled(RangerAccessResult result) {
		return result != null && result.isRowFilterEnabled() && StringUtils.isNotEmpty(result.getFilterExpr());
	}

	private String getRowFilterExpression(HiveAuthzContext context, String databaseName, String tableOrViewName) throws SemanticException {
		UserGroupInformation ugi = getCurrentUserGroupInfo();

		if(ugi == null) {
			throw new SemanticException("user information not available");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> getRowFilterExpression(" + databaseName + ", " + tableOrViewName + ")");
		}

		String ret = null;

		RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

		try {
			HiveAuthzSessionContext sessionContext = getHiveAuthzSessionContext();
			String                  user           = ugi.getShortUserName();
			Set<String>             groups         = Sets.newHashSet(ugi.getGroupNames());
			HiveObjectType          objectType     = HiveObjectType.TABLE;
			String 					clusterName    = hivePlugin.getClusterName();
			RangerHiveResource      resource       = new RangerHiveResource(objectType, databaseName, tableOrViewName);
			RangerHiveAccessRequest request        = new RangerHiveAccessRequest(resource, user, groups, objectType.name(), HiveAccessType.SELECT, context, sessionContext, clusterName);

			RangerAccessResult result = hivePlugin.evalRowFilterPolicies(request, auditHandler);

			if(isRowFilterEnabled(result)) {
				ret = result.getFilterExpr();
			}
		} finally {
			auditHandler.flushAudit();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== getRowFilterExpression(" + databaseName + ", " + tableOrViewName + "): " + ret);
		}

		return ret;
	}

	private boolean addCellValueTransformerAndCheckIfTransformed(HiveAuthzContext context, String databaseName, String tableOrViewName, String columnName, List<String> columnTransformers) throws SemanticException {
		UserGroupInformation ugi = getCurrentUserGroupInfo();

		String clusterName = hivePlugin.getClusterName();
		if(ugi == null) {
			throw new SemanticException("user information not available");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addCellValueTransformerAndCheckIfTransformed(" + databaseName + ", " + tableOrViewName + ", " + columnName + ")");
		}

		boolean ret = false;
		String columnTransformer = columnName;

		RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

		try {
			HiveAuthzSessionContext sessionContext = getHiveAuthzSessionContext();
			String                  user           = ugi.getShortUserName();
			Set<String>             groups         = Sets.newHashSet(ugi.getGroupNames());
			HiveObjectType          objectType     = HiveObjectType.COLUMN;
			RangerHiveResource      resource       = new RangerHiveResource(objectType, databaseName, tableOrViewName, columnName);
			RangerHiveAccessRequest request        = new RangerHiveAccessRequest(resource, user, groups, objectType.name(), HiveAccessType.SELECT, context, sessionContext, clusterName);

			RangerAccessResult result = hivePlugin.evalDataMaskPolicies(request, auditHandler);

			ret = isDataMaskEnabled(result);

			if(ret) {
				String                maskType    = result.getMaskType();
				RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
				String transformer	= null;
				if (maskTypeDef != null) {
					transformer = maskTypeDef.getTransformer();
				}

				if(StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
					columnTransformer = "NULL";
				} else if(StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
					String maskedValue = result.getMaskedValue();

					if(maskedValue == null) {
						columnTransformer = "NULL";
					} else {
						columnTransformer = maskedValue.replace("{col}", columnName);
					}

				} else if(StringUtils.isNotEmpty(transformer)) {
					columnTransformer = transformer.replace("{col}", columnName);
				}

				/*
				String maskCondition = result.getMaskCondition();

				if(StringUtils.isNotEmpty(maskCondition)) {
					ret = "if(" + maskCondition + ", " + ret + ", " + columnName + ")";
				}
				*/
			}
		} finally {
			auditHandler.flushAudit();
		}

		columnTransformers.add(columnTransformer);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== addCellValueTransformerAndCheckIfTransformed(" + databaseName + ", " + tableOrViewName + ", " + columnName + "): " + ret);
		}

		return ret;
	}

	RangerHiveResource createHiveResource(HivePrivilegeObject privilegeObject) {
		RangerHiveResource resource = null;

		HivePrivilegeObjectType objectType = privilegeObject.getType();
		String objectName = privilegeObject.getObjectName();
		String dbName = privilegeObject.getDbname();

		switch(objectType) {
		case DATABASE:
			resource = new RangerHiveResource(HiveObjectType.DATABASE, objectName);
			break;
		case TABLE_OR_VIEW:
			resource = new RangerHiveResource(HiveObjectType.TABLE, dbName, objectName);
			break;
		default:
			LOG.warn("RangerHiveAuthorizer.getHiveResource: unexpected objectType:" + objectType);
		}

		if (resource != null) {
			resource.setServiceDef(hivePlugin == null ? null : hivePlugin.getServiceDef());
		}

		return resource;
	}


	private RangerHiveResource getHiveResource(HiveOperationType   hiveOpType,
											   HivePrivilegeObject hiveObj) {
		RangerHiveResource ret = null;

		HiveObjectType objectType = getObjectType(hiveObj, hiveOpType);

		switch(objectType) {
			case DATABASE:
				ret = new RangerHiveResource(objectType, hiveObj.getDbname());
			break;
	
			case TABLE:
			case VIEW:
			case PARTITION:
			case INDEX:
			case FUNCTION:
				ret = new RangerHiveResource(objectType, hiveObj.getDbname(), hiveObj.getObjectName());
			break;
	
			case COLUMN:
				ret = new RangerHiveResource(objectType, hiveObj.getDbname(), hiveObj.getObjectName(), StringUtils.join(hiveObj.getColumns(), COLUMN_SEP));
			break;

            case URI:
				ret = new RangerHiveResource(objectType, hiveObj.getObjectName());
            break;

			case NONE:
			break;
		}

		if (ret != null) {
			ret.setServiceDef(hivePlugin == null ? null : hivePlugin.getServiceDef());
		}

		return ret;
	}

	private HiveObjectType getObjectType(HivePrivilegeObject hiveObj, HiveOperationType hiveOpType) {
		HiveObjectType objType = HiveObjectType.NONE;
		if (hiveObj.getType() == null) {
			return HiveObjectType.DATABASE;
		}

		switch(hiveObj.getType()) {
			case DATABASE:
				objType = HiveObjectType.DATABASE;
			break;

			case PARTITION:
				objType = HiveObjectType.PARTITION;
			break;

			case TABLE_OR_VIEW:
				String hiveOpTypeName = hiveOpType.name().toLowerCase();
				if(hiveOpTypeName.contains("index")) {
					objType = HiveObjectType.INDEX;
				} else if(! StringUtil.isEmpty(hiveObj.getColumns())) {
					objType = HiveObjectType.COLUMN;
				} else if(hiveOpTypeName.contains("view")) {
					objType = HiveObjectType.VIEW;
				} else {
					objType = HiveObjectType.TABLE;
				}
			break;

			case FUNCTION:
				objType = HiveObjectType.FUNCTION;
			break;

			case DFS_URI:
			case LOCAL_URI:
                objType = HiveObjectType.URI;
            break;

			case COMMAND_PARAMS:
			case GLOBAL:
			break;

			case COLUMN:
				// Thejas: this value is unused in Hive; the case should not be hit.
			break;
		}

		return objType;
	}

	private HiveAccessType getAccessType(HivePrivilegeObject hiveObj, HiveOperationType hiveOpType, HiveObjectType hiveObjectType, boolean isInput) {
		HiveAccessType           accessType       = HiveAccessType.NONE;
		HivePrivObjectActionType objectActionType = hiveObj.getActionType();

		// This is for S3 read operation
		if (hiveObjectType == HiveObjectType.URI && isInput ) {
			accessType = HiveAccessType.READ;
			return accessType;
		}
		// This is for S3 write
		if (hiveObjectType == HiveObjectType.URI && !isInput ) {
			accessType = HiveAccessType.WRITE;
			return accessType;
		}

		switch(objectActionType) {
			case INSERT:
			case INSERT_OVERWRITE:
			case UPDATE:
			case DELETE:
				accessType = HiveAccessType.UPDATE;
			break;
			case OTHER:
			switch(hiveOpType) {
				case CREATEDATABASE:
					if(hiveObj.getType() == HivePrivilegeObjectType.DATABASE) {
						accessType = HiveAccessType.CREATE;
					}
				break;

				case CREATEFUNCTION:
					if(hiveObj.getType() == HivePrivilegeObjectType.FUNCTION) {
						accessType = HiveAccessType.CREATE;
					}
				break;

				case CREATETABLE:
				case CREATEVIEW:
				case CREATETABLE_AS_SELECT:
					if(hiveObj.getType() == HivePrivilegeObjectType.TABLE_OR_VIEW) {
						accessType = isInput ? HiveAccessType.SELECT : HiveAccessType.CREATE;
					}
				break;

				case ALTERDATABASE:
				case ALTERDATABASE_OWNER:
				case ALTERINDEX_PROPS:
				case ALTERINDEX_REBUILD:
				case ALTERPARTITION_BUCKETNUM:
				case ALTERPARTITION_FILEFORMAT:
				case ALTERPARTITION_LOCATION:
				case ALTERPARTITION_MERGEFILES:
				case ALTERPARTITION_PROTECTMODE:
				case ALTERPARTITION_SERDEPROPERTIES:
				case ALTERPARTITION_SERIALIZER:
				case ALTERTABLE_ADDCOLS:
				case ALTERTABLE_ADDPARTS:
				case ALTERTABLE_ARCHIVE:
				case ALTERTABLE_BUCKETNUM:
				case ALTERTABLE_CLUSTER_SORT:
				case ALTERTABLE_COMPACT:
				case ALTERTABLE_DROPPARTS:
				case ALTERTABLE_DROPCONSTRAINT:
				case ALTERTABLE_ADDCONSTRAINT:
				case ALTERTABLE_FILEFORMAT:
				case ALTERTABLE_LOCATION:
				case ALTERTABLE_MERGEFILES:
				case ALTERTABLE_PARTCOLTYPE:
				case ALTERTABLE_PROPERTIES:
				case ALTERTABLE_PROTECTMODE:
				case ALTERTABLE_RENAME:
				case ALTERTABLE_RENAMECOL:
				case ALTERTABLE_RENAMEPART:
				case ALTERTABLE_REPLACECOLS:
				case ALTERTABLE_SERDEPROPERTIES:
				case ALTERTABLE_SERIALIZER:
				case ALTERTABLE_SKEWED:
				case ALTERTABLE_TOUCH:
				case ALTERTABLE_UNARCHIVE:
				case ALTERTABLE_UPDATEPARTSTATS:
				case ALTERTABLE_UPDATETABLESTATS:
				case ALTERTBLPART_SKEWED_LOCATION:
				case ALTERVIEW_AS:
				case ALTERVIEW_PROPERTIES:
				case ALTERVIEW_RENAME:
				case DROPVIEW_PROPERTIES:
				case MSCK:
					accessType = HiveAccessType.ALTER;
				break;

				case DROPFUNCTION:
				case DROPINDEX:
				case DROPTABLE:
				case DROPVIEW:
				case DROPDATABASE:
					accessType = HiveAccessType.DROP;
				break;

				case CREATEINDEX:
					accessType = HiveAccessType.INDEX;
				break;

				case IMPORT:
					/*
					This can happen during hive IMPORT command IFF a table is also being created as part of IMPORT.
					If so then
					- this would appear in the outputHObjs, i.e. accessType == false
					- user then must have CREATE permission on the database

					During IMPORT command it is not possible for a database to be in inputHObj list. Thus returning SELECT
					when accessType==true is never expected to be hit in practice.
					 */
					accessType = isInput ? HiveAccessType.SELECT : HiveAccessType.CREATE;
					break;

				case EXPORT:
				case LOAD:
					accessType = isInput ? HiveAccessType.SELECT : HiveAccessType.UPDATE;
				break;

				case LOCKDB:
				case LOCKTABLE:
				case UNLOCKDB:
				case UNLOCKTABLE:
					accessType = HiveAccessType.LOCK;
				break;

				/*
				 * SELECT access is done for many of these metadata operations since hive does not call back for filtering.
				 * Overtime these should move to _any/USE access (as hive adds support for filtering).
				 */
				case QUERY:
				case SHOW_TABLESTATUS:
				case SHOW_CREATETABLE:
				case SHOWINDEXES:
				case SHOWPARTITIONS:
				case SHOW_TBLPROPERTIES:
				case ANALYZE_TABLE:
					accessType = HiveAccessType.SELECT;
				break;

				case SHOWCOLUMNS:
				case DESCTABLE:
					switch (StringUtil.toLower(hivePlugin.DescribeShowTableAuth)){
						case "show-allowed":
							// This is not implemented so defaulting to current behaviour of blocking describe/show columns not to show any columns.
							// This has to be implemented when hive provides the necessary filterListCmdObjects for
							// SELECT/SHOWCOLUMS/DESCTABLE to filter the columns based on access provided in ranger.
						case "none":
						case "":
							accessType = HiveAccessType.SELECT;
							break;
						case "show-all":
							accessType = HiveAccessType.USE;
							break;
					}
				break;

				// any access done for metadata access of actions that have support from hive for filtering
				case SHOWDATABASES:
				case SWITCHDATABASE:
				case DESCDATABASE:
				case SHOWTABLES:
					accessType = HiveAccessType.USE;
				break;

				case TRUNCATETABLE:
					accessType = HiveAccessType.UPDATE;
				break;

				case GRANT_PRIVILEGE:
				case REVOKE_PRIVILEGE:
					accessType = HiveAccessType.NONE; // access check will be performed at the ranger-admin side
				break;

				case ADD:
				case DELETE:
				case COMPILE:
				case CREATEMACRO:
				case CREATEROLE:
				case DESCFUNCTION:
				case DFS:
				case DROPMACRO:
				case DROPROLE:
				case EXPLAIN:
				case GRANT_ROLE:
				case REVOKE_ROLE:
				case RESET:
				case SET:
				case SHOWCONF:
				case SHOWFUNCTIONS:
				case SHOWLOCKS:
				case SHOW_COMPACTIONS:
				case SHOW_GRANT:
				case SHOW_ROLES:
				case SHOW_ROLE_GRANT:
				case SHOW_ROLE_PRINCIPALS:
				case SHOW_TRANSACTIONS:
				break;
			}
			break;
		}
		
		return accessType;
	}

	private FsAction getURIAccessType(HiveOperationType hiveOpType) {
		FsAction ret = FsAction.NONE;

		switch(hiveOpType) {
			case LOAD:
			case IMPORT:
				ret = FsAction.READ;
			break;

			case EXPORT:
				ret = FsAction.WRITE;
			break;

			case CREATEDATABASE:
			case CREATETABLE:
			case CREATETABLE_AS_SELECT:
			case ALTERDATABASE:
			case ALTERDATABASE_OWNER:
			case ALTERTABLE_ADDCOLS:
			case ALTERTABLE_REPLACECOLS:
			case ALTERTABLE_RENAMECOL:
			case ALTERTABLE_RENAMEPART:
			case ALTERTABLE_RENAME:
			case ALTERTABLE_DROPPARTS:
			case ALTERTABLE_ADDPARTS:
			case ALTERTABLE_TOUCH:
			case ALTERTABLE_ARCHIVE:
			case ALTERTABLE_UNARCHIVE:
			case ALTERTABLE_PROPERTIES:
			case ALTERTABLE_SERIALIZER:
			case ALTERTABLE_PARTCOLTYPE:
			case ALTERTABLE_DROPCONSTRAINT:
			case ALTERTABLE_ADDCONSTRAINT:
			case ALTERTABLE_SERDEPROPERTIES:
			case ALTERTABLE_CLUSTER_SORT:
			case ALTERTABLE_BUCKETNUM:
			case ALTERTABLE_UPDATETABLESTATS:
			case ALTERTABLE_UPDATEPARTSTATS:
			case ALTERTABLE_PROTECTMODE:
			case ALTERTABLE_FILEFORMAT:
			case ALTERTABLE_LOCATION:
			case ALTERINDEX_PROPS:
			case ALTERTABLE_MERGEFILES:
			case ALTERTABLE_SKEWED:
			case ALTERTABLE_COMPACT:
			case ALTERTABLE_EXCHANGEPARTITION:
			case ALTERPARTITION_SERIALIZER:
			case ALTERPARTITION_SERDEPROPERTIES:
			case ALTERPARTITION_BUCKETNUM:
			case ALTERPARTITION_PROTECTMODE:
			case ALTERPARTITION_FILEFORMAT:
			case ALTERPARTITION_LOCATION:
			case ALTERPARTITION_MERGEFILES:
			case ALTERTBLPART_SKEWED_LOCATION:
			case QUERY:
				ret = FsAction.ALL;
				break;

			case EXPLAIN:
			case DROPDATABASE:
			case SWITCHDATABASE:
			case LOCKDB:
			case UNLOCKDB:
			case DROPTABLE:
			case DESCTABLE:
			case DESCFUNCTION:
			case MSCK:
			case ANALYZE_TABLE:
			case CACHE_METADATA:
			case SHOWDATABASES:
			case SHOWTABLES:
			case SHOWCOLUMNS:
			case SHOW_TABLESTATUS:
			case SHOW_TBLPROPERTIES:
			case SHOW_CREATEDATABASE:
			case SHOW_CREATETABLE:
			case SHOWFUNCTIONS:
			case SHOWINDEXES:
			case SHOWPARTITIONS:
			case SHOWLOCKS:
			case SHOWCONF:
			case CREATEFUNCTION:
			case DROPFUNCTION:
			case RELOADFUNCTION:
			case CREATEMACRO:
			case DROPMACRO:
			case CREATEVIEW:
			case DROPVIEW:
			case CREATEINDEX:
			case DROPINDEX:
			case ALTERINDEX_REBUILD:
			case ALTERVIEW_PROPERTIES:
			case DROPVIEW_PROPERTIES:
			case LOCKTABLE:
			case UNLOCKTABLE:
			case CREATEROLE:
			case DROPROLE:
			case GRANT_PRIVILEGE:
			case REVOKE_PRIVILEGE:
			case SHOW_GRANT:
			case GRANT_ROLE:
			case REVOKE_ROLE:
			case SHOW_ROLES:
			case SHOW_ROLE_GRANT:
			case SHOW_ROLE_PRINCIPALS:
			case TRUNCATETABLE:
			case DESCDATABASE:
			case ALTERVIEW_RENAME:
			case ALTERVIEW_AS:
			case SHOW_COMPACTIONS:
			case SHOW_TRANSACTIONS:
			case ABORT_TRANSACTIONS:
			case SET:
			case RESET:
			case DFS:
			case ADD:
			case DELETE:
			case COMPILE:
			case START_TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case SET_AUTOCOMMIT:
			case GET_CATALOGS:
			case GET_COLUMNS:
			case GET_FUNCTIONS:
			case GET_SCHEMAS:
			case GET_TABLES:
			case GET_TABLETYPES:
			case GET_TYPEINFO:
				break;
		}

		return ret;
	}

	private String buildPathForException(String path, HiveOperationType hiveOpType) {
		String ret  	= path;
		int endIndex 	= 0;
		switch(hiveOpType) {
			case DESCTABLE:
				ret = path + "/*";
				break;
			case QUERY:
				try {
					endIndex = StringUtils.ordinalIndexOf(path, "/", 2);
					ret = path.substring(0,endIndex) + "/*";
				} catch( Exception e) {
					//omit and return the path.Log error only in debug.
					if(LOG.isDebugEnabled()) {
						LOG.debug("RangerHiveAuthorizer.buildPathForException(): Error while creating exception message ", e);
					}
				}
				break;
		}
		return ret;
	}

    private boolean isURIAccessAllowed(String userName, FsAction action, String uri, HiveConf conf) {
        boolean ret = false;

        if(action == FsAction.NONE) {
            ret = true;
        } else {
            try {
                Path       filePath   = new Path(uri);
                FileSystem fs         = FileSystem.get(filePath.toUri(), conf);
                FileStatus[] filestat = fs.globStatus(filePath);

                if(filestat != null && filestat.length > 0) {
                    boolean isDenied = false;

                    for(FileStatus file : filestat) {
                        if (FileUtils.isOwnerOfFileHierarchy(fs, file, userName) ||
							FileUtils.isActionPermittedForFileHierarchy(fs, file, userName, action)) {
								continue;
						} else {
							isDenied = true;
							break;
						}
                     }
                     ret = !isDenied;
                } else { // if given path does not exist then check for parent
                    FileStatus file = FileUtils.getPathOrParentThatExists(fs, filePath);

                    FileUtils.checkFileAccessWithImpersonation(fs, file, action, userName);
                    ret = true;
                }
            } catch(Exception excp) {
				ret = false;
                LOG.error("Error getting permissions for " + uri, excp);
            }
        }

        return ret;
    }

	private boolean isPathInFSScheme(String uri) {
		// This is to find if HIVE URI operation done is for hdfs,file scheme
		// else it may be for s3 which needs another set of authorization calls.
		boolean ret = false;
		String[] fsScheme = hivePlugin.getFSScheme();
		if (fsScheme != null) {
			for (String scheme : fsScheme) {
				if (!uri.isEmpty() && uri.startsWith(scheme)) {
					ret = true;
					break;
				}
			}
		}
		return ret;
	}


	private void handleDfsCommand(HiveOperationType         hiveOpType,
								  List<HivePrivilegeObject> inputHObjs,
								  String                    user,
								  RangerHiveAuditHandler    auditHandler)
	      throws HiveAuthzPluginException, HiveAccessControlException {

		String dfsCommandParams = null;

		if(inputHObjs != null) {
			for(HivePrivilegeObject hiveObj : inputHObjs) {
				if(hiveObj.getType() == HivePrivilegeObjectType.COMMAND_PARAMS) {
					dfsCommandParams = StringUtil.toString(hiveObj.getCommandParams());

					if(! StringUtil.isEmpty(dfsCommandParams)) {
						break;
					}
				}
			}
		}

		int    serviceType = -1;
		String serviceName = null;

		if(hivePlugin != null) {
			serviceType = hivePlugin.getServiceDefId();
			serviceName = hivePlugin.getServiceName();
		}

		auditHandler.logAuditEventForDfs(user, dfsCommandParams, false, serviceType, serviceName);

		throw new HiveAccessControlException(String.format("Permission denied: user [%s] does not have privilege for [%s] command",
											 user, hiveOpType.name()));
	}

	private boolean existsByResourceAndAccessType(Collection<RangerHiveAccessRequest> requests, RangerHiveResource resource, HiveAccessType accessType) {
		boolean ret = false;

		if(requests != null && resource != null) {
			for(RangerHiveAccessRequest request : requests) {
				if(request.getHiveAccessType() == accessType && request.getResource().equals(resource)) {
					ret = true;

					break;
				}
			}
		}

		return ret;
	}

	private String getGrantorUsername(HivePrincipal grantorPrincipal) {
		String grantor = grantorPrincipal != null ? grantorPrincipal.getName() : null;

		if(StringUtil.isEmpty(grantor)) {
			UserGroupInformation ugi = this.getCurrentUserGroupInfo();

			grantor = ugi != null ? ugi.getShortUserName() : null;
		}

		return grantor;
	}

	private Set<String> getGrantorGroupNames(HivePrincipal grantorPrincipal) {
		Set<String> ret = null;

		String grantor = grantorPrincipal != null ? grantorPrincipal.getName() : null;

		UserGroupInformation ugi = StringUtil.isEmpty(grantor) ? this.getCurrentUserGroupInfo() : UserGroupInformation.createRemoteUser(grantor);

		String[] groups = ugi != null ? ugi.getGroupNames() : null;

		if (groups != null && groups.length > 0) {
			ret = new HashSet<>(Arrays.asList(groups));
		}

		return ret;
	}

	private GrantRevokeRequest createGrantRevokeData(RangerHiveResource  resource,
													 List<HivePrincipal> hivePrincipals,
													 List<HivePrivilege> hivePrivileges,
													 HivePrincipal       grantorPrincipal,
													 boolean             grantOption)
														  throws HiveAccessControlException {
		if(resource == null ||
		  ! (   resource.getObjectType() == HiveObjectType.DATABASE
		     || resource.getObjectType() == HiveObjectType.TABLE
		     || resource.getObjectType() == HiveObjectType.VIEW
		     || resource.getObjectType() == HiveObjectType.COLUMN
		   )
		  ) {
			throw new HiveAccessControlException("grant/revoke: unexpected object type '" + (resource == null ? null : resource.getObjectType().name()));
		}

		GrantRevokeRequest ret = new GrantRevokeRequest();

		ret.setGrantor(getGrantorUsername(grantorPrincipal));
		ret.setGrantorGroups(getGrantorGroupNames(grantorPrincipal));
		ret.setDelegateAdmin(grantOption ? Boolean.TRUE : Boolean.FALSE);
		ret.setEnableAudit(Boolean.TRUE);
		ret.setReplaceExistingPermissions(Boolean.FALSE);

		String database = StringUtils.isEmpty(resource.getDatabase()) ? "*" : resource.getDatabase();
		String table    = StringUtils.isEmpty(resource.getTable()) ? "*" : resource.getTable();
		String column   = StringUtils.isEmpty(resource.getColumn()) ? "*" : resource.getColumn();

		Map<String, String> mapResource = new HashMap<String, String>();
		mapResource.put(RangerHiveResource.KEY_DATABASE, database);
		mapResource.put(RangerHiveResource.KEY_TABLE, table);
		mapResource.put(RangerHiveResource.KEY_COLUMN, column);

		ret.setResource(mapResource);

		SessionState ss = SessionState.get();
		if(ss != null) {
			ret.setClientIPAddress(ss.getUserIpAddress());
			ret.setSessionId(ss.getSessionId());

			HiveConf hiveConf = ss.getConf();

			if(hiveConf != null) {
				ret.setRequestData(hiveConf.get(HIVE_CONF_VAR_QUERY_STRING));
			}
		}

		HiveAuthzSessionContext sessionContext = getHiveAuthzSessionContext();
		if(sessionContext != null) {
			ret.setClientType(sessionContext.getClientType() == null ? null : sessionContext.getClientType().toString());
		}

		for(HivePrincipal principal : hivePrincipals) {
			switch(principal.getType()) {
				case USER:
					ret.getUsers().add(principal.getName());
				break;

				case GROUP:
				case ROLE:
					ret.getGroups().add(principal.getName());
				break;

				case UNKNOWN:
				break;
			}
		}

		for(HivePrivilege privilege : hivePrivileges) {
			String privName = privilege.getName();
			
			if(StringUtils.equalsIgnoreCase(privName, HiveAccessType.ALL.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.ALTER.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.CREATE.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.DROP.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.INDEX.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.LOCK.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.SELECT.name()) ||
			   StringUtils.equalsIgnoreCase(privName, HiveAccessType.UPDATE.name())) {
				ret.getAccessTypes().add(privName.toLowerCase());
			} else if (StringUtils.equalsIgnoreCase(privName, "Insert") ||
							StringUtils.equalsIgnoreCase(privName, "Delete")) {
				// Mapping Insert/Delete to Update
				ret.getAccessTypes().add(HiveAccessType.UPDATE.name().toLowerCase());
			} else {
				LOG.warn("grant/revoke: unexpected privilege type '" + privName + "'. Ignored");
			}
		}

		return ret;
	}

	@Override
	public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
			HivePrivilegeObject privObj) throws HiveAuthzPluginException {
		try {

			LOG.debug("RangerHiveAuthorizer.showPrivileges()");
			IMetaStoreClient mClient = getMetastoreClientFactory()
					.getHiveMetastoreClient();
			List<HivePrivilegeInfo> resPrivInfos = new ArrayList<HivePrivilegeInfo>();
			String principalName = null;
			PrincipalType principalType = null;
			if (principal != null) {
				principalName = principal.getName();
				principalType = AuthorizationUtils
						.getThriftPrincipalType(principal.getType());
			}

			List<HiveObjectPrivilege> msObjPrivs = mClient.list_privileges(
					principalName, principalType,
					this.getThriftHiveObjectRef(privObj));
			if (msObjPrivs != null) {
				for (HiveObjectPrivilege msObjPriv : msObjPrivs) {
					HiveObjectRef msObjRef = msObjPriv.getHiveObject();
					org.apache.hadoop.hive.metastore.api.HiveObjectType objectType = msObjRef
							.getObjectType();
					if (!isSupportedObjectType(objectType)) {
						continue;
					}
					HivePrincipal resPrincipal = new HivePrincipal(
							msObjPriv.getPrincipalName(),
							AuthorizationUtils.getHivePrincipalType(msObjPriv
									.getPrincipalType()));

					PrivilegeGrantInfo msGrantInfo = msObjPriv.getGrantInfo();
					HivePrivilege resPrivilege = new HivePrivilege(
							msGrantInfo.getPrivilege(), null);

					HivePrivilegeObject resPrivObj = new HivePrivilegeObject(
							getPluginPrivilegeObjType(objectType),
							msObjRef.getDbName(), msObjRef.getObjectName(),
							msObjRef.getPartValues(), msObjRef.getColumnName());

					HivePrincipal grantorPrincipal = new HivePrincipal(
							msGrantInfo.getGrantor(),
							AuthorizationUtils.getHivePrincipalType(msGrantInfo
									.getGrantorType()));

					HivePrivilegeInfo resPrivInfo = new HivePrivilegeInfo(
							resPrincipal, resPrivilege, resPrivObj,
							grantorPrincipal, msGrantInfo.isGrantOption(),
							msGrantInfo.getCreateTime());
					resPrivInfos.add(resPrivInfo);
				}

			} else {
				throw new HiveAccessControlException(
						"RangerHiveAuthorizer.showPrivileges():User has to specify"
								+ " a user name or role in the show grant. ");
			}
			return resPrivInfos;

		} catch (Exception e) {
			LOG.error("RangerHiveAuthorizer.showPrivileges: showPrivileges returned by showPrivileges is null");
			throw new HiveAuthzPluginException("hive showPrivileges" + ": "
					+ e.getMessage(), e);
		}
	}

	private boolean isSupportedObjectType(
			org.apache.hadoop.hive.metastore.api.HiveObjectType objectType) {
		switch (objectType) {
		case DATABASE:
		case TABLE:
			return true;
		default:
			return false;
		}

	}

	private HivePrivilegeObjectType getPluginPrivilegeObjType(
			org.apache.hadoop.hive.metastore.api.HiveObjectType objectType) {
		switch (objectType) {
		case DATABASE:
			return HivePrivilegeObjectType.DATABASE;
		case TABLE:
			return HivePrivilegeObjectType.TABLE_OR_VIEW;
		default:
			throw new AssertionError("Unexpected object type " + objectType);
		}
	}

	static HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj)
			throws HiveAuthzPluginException {
		try {
			return AuthorizationUtils.getThriftHiveObjectRef(privObj);
		} catch (HiveException e) {
			throw new HiveAuthzPluginException(e);
		}
	}

	private RangerRequestedResources buildRequestContextWithAllAccessedResources(List<RangerHiveAccessRequest> requests) {

		RangerRequestedResources requestedResources = new RangerRequestedResources();

		for (RangerHiveAccessRequest request : requests) {
			// Build list of all things requested and put it in the context of each request
			RangerAccessRequestUtil.setRequestedResourcesInContext(request.getContext(), requestedResources);

			RangerHiveResource resource = (RangerHiveResource) request.getResource();

			if (resource.getObjectType() == HiveObjectType.COLUMN && StringUtils.contains(resource.getColumn(), COLUMN_SEP)) {

				String[] columns = StringUtils.split(resource.getColumn(), COLUMN_SEP);

				// in case of multiple columns, original request is not sent to the plugin; hence service-def will not be set
				resource.setServiceDef(hivePlugin.getServiceDef());

				for (String column : columns) {
					if (column != null) {
						column = column.trim();
					}
					if (StringUtils.isBlank(column)) {
						continue;
					}

					RangerHiveResource colResource = new RangerHiveResource(HiveObjectType.COLUMN, resource.getDatabase(), resource.getTable(), column);
					colResource.setServiceDef(hivePlugin.getServiceDef());

					requestedResources.addRequestedResource(colResource);

				}
			} else {
				resource.setServiceDef(hivePlugin.getServiceDef());
				requestedResources.addRequestedResource(resource);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("RangerHiveAuthorizer.buildRequestContextWithAllAccessedResources() - " + requestedResources);
		}

		return requestedResources;
	}

	private boolean isBlockAccessIfRowfilterColumnMaskSpecified(HiveOperationType hiveOpType, RangerHiveAccessRequest request) {
		boolean            ret      = false;
		RangerHiveResource resource = (RangerHiveResource)request.getResource();
		HiveObjectType     objType  = resource.getObjectType();

		if(objType == HiveObjectType.TABLE || objType == HiveObjectType.VIEW || objType == HiveObjectType.COLUMN) {
			ret = hiveOpType == HiveOperationType.EXPORT;

			if(!ret) {
				if (request.getHiveAccessType() == HiveAccessType.UPDATE && hivePlugin.BlockUpdateIfRowfilterColumnMaskSpecified) {
					ret = true;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("isBlockAccessIfRowfilterColumnMaskSpecified(" + hiveOpType + ", " + request + "): " + ret);
		}

		return ret;
	}

	private String toString(HiveOperationType         hiveOpType,
							List<HivePrivilegeObject> inputHObjs,
							List<HivePrivilegeObject> outputHObjs,
							HiveAuthzContext          context,
							HiveAuthzSessionContext   sessionContext) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("'checkPrivileges':{");
		sb.append("'hiveOpType':").append(hiveOpType);

		sb.append(", 'inputHObjs':[");
		toString(inputHObjs, sb);
		sb.append("]");

		sb.append(", 'outputHObjs':[");
		toString(outputHObjs, sb);
		sb.append("]");

		sb.append(", 'context':{");
		sb.append("'clientType':").append(sessionContext == null ? null : sessionContext.getClientType());
		sb.append(", 'commandString':").append(context == null ? "null" : context.getCommandString());
		sb.append(", 'ipAddress':").append(context == null ? "null" : context.getIpAddress());
		sb.append(", 'forwardedAddresses':").append(context == null ? "null" : StringUtils.join(context.getForwardedAddresses(), ", "));
		sb.append(", 'sessionString':").append(sessionContext == null ? "null" : sessionContext.getSessionString());
		sb.append("}");

		sb.append(", 'user':").append(this.getCurrentUserGroupInfo().getUserName());
		sb.append(", 'groups':[").append(StringUtil.toString(this.getCurrentUserGroupInfo().getGroupNames())).append("]");

		sb.append("}");

		return sb.toString();
	}

	private StringBuilder toString(List<HivePrivilegeObject> privObjs, StringBuilder sb) {
		if(privObjs != null && privObjs.size() > 0) {
			toString(privObjs.get(0), sb);
			for(int i = 1; i < privObjs.size(); i++) {
				sb.append(",");
				toString(privObjs.get(i), sb);
			}
		}
		
		return sb;
	}

	private StringBuilder toString(HivePrivilegeObject privObj, StringBuilder sb) {
		sb.append("'HivePrivilegeObject':{");
		sb.append("'type':").append(privObj.getType().toString());
		sb.append(", 'dbName':").append(privObj.getDbname());
		sb.append(", 'objectType':").append(privObj.getType());
		sb.append(", 'objectName':").append(privObj.getObjectName());
		sb.append(", 'columns':[").append(StringUtil.toString(privObj.getColumns())).append("]");
		sb.append(", 'partKeys':[").append(StringUtil.toString(privObj.getPartKeys())).append("]");
		sb.append(", 'commandParams':[").append(StringUtil.toString(privObj.getCommandParams())).append("]");
		sb.append(", 'actionType':").append(privObj.getActionType().toString());
		sb.append("}");

		return sb;
	}
}

enum HiveObjectType { NONE, DATABASE, TABLE, VIEW, PARTITION, INDEX, COLUMN, FUNCTION, URI };
enum HiveAccessType { NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, READ, WRITE, ALL, ADMIN };

class RangerHivePlugin extends RangerBasePlugin {
	public static boolean UpdateXaPoliciesOnGrantRevoke = RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE;
	public static boolean BlockUpdateIfRowfilterColumnMaskSpecified = RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_DEFAULT_VALUE;
	public static String DescribeShowTableAuth = RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP_DEFAULT_VALUE;

	private static String RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES = "ranger.plugin.hive.urlauth.filesystem.schemes";
	private static String RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES_DEFAULT = "hdfs:,file:";
	private static String FILESYSTEM_SCHEMES_SEPARATOR_CHAR = ",";
	private String[] fsScheme = null;

	public RangerHivePlugin(String appType) {
		super("hive", appType);
	}

	@Override
	public void init() {
		super.init();

		RangerHivePlugin.UpdateXaPoliciesOnGrantRevoke = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_PROP, RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE);
		RangerHivePlugin.BlockUpdateIfRowfilterColumnMaskSpecified = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_PROP, RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_DEFAULT_VALUE);
		RangerHivePlugin.DescribeShowTableAuth = RangerConfiguration.getInstance().get(RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP, RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP_DEFAULT_VALUE);

		String fsSchemesString = RangerConfiguration.getInstance().get(RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES, RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES_DEFAULT);
		fsScheme = StringUtils.split(fsSchemesString, FILESYSTEM_SCHEMES_SEPARATOR_CHAR);

		if (fsScheme != null) {
			for (int i = 0; i < fsScheme.length; i++) {
				fsScheme[i] = fsScheme[i].trim();
			}
		}
	}

	public String[] getFSScheme() {
		return fsScheme;
	}
}
