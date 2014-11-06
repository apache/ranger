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
package com.xasecure.authorization.hbase;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.access.XaAccessControlLists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.xasecure.admin.client.XaAdminRESTClient;
import com.xasecure.admin.client.datatype.GrantRevokeData;
import com.xasecure.admin.client.datatype.GrantRevokeData.PermMap;
import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;
import com.xasecure.authorization.utils.StringUtil;

public class XaSecureAuthorizationCoprocessor extends XaSecureAuthorizationCoprocessorBase implements AccessControlService.Interface, CoprocessorService {
	private static final Log LOG = LogFactory.getLog(XaSecureAuthorizationCoprocessor.class.getName());
	private static final String XaSecureModuleName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_XASECURE_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME) ;
	private static final short  accessGrantedFlag  = 1;
	private static final short  accessDeniedFlag   = 0;
	private static final String repositoryName          = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);
	private static final boolean UpdateXaPoliciesOnGrantRevoke = XaSecureConfiguration.getInstance().getBoolean(XaSecureHadoopConstants.HBASE_UPDATE_XAPOLICIES_ON_GRANT_REVOKE_PROP, XaSecureHadoopConstants.HBASE_UPDATE_XAPOLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE);
	private static final String GROUP_PREFIX = "@";

		
	private static final String SUPERUSER_CONFIG_PROP = "hbase.superuser";
	private static final String WILDCARD = "*";
	private static final byte[] WILDCARD_MATCH = "*".getBytes();
	
    private static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT+0");

    private RegionCoprocessorEnvironment regionEnv;
	private Map<InternalScanner, String> scannerOwners = new MapMaker().weakKeys().makeMap();
	
	private HBaseAccessController accessController = HBaseAccessControllerFactory.getInstance();
	private List<String> superUserList = null;
	// Utilities Methods 
	protected byte[] getTableName(RegionCoprocessorEnvironment e) {
		HRegion region = e.getRegion();
		byte[] tableName = null;
		if (region != null) {
			HRegionInfo regionInfo = region.getRegionInfo();
			if (regionInfo != null) {
				tableName = regionInfo.getTable().getName() ;
			}
		}
		return tableName;
	}
	protected void isSystemOrSuperUser(Configuration conf) throws IOException {
		User user = User.getCurrent();
		if (user == null) {
			throw new IOException("Unable to obtain the current user, authorization checks for internal operations will not work correctly!");
		}
		String currentUser = user.getShortName();
		List<String> superusers = Lists.asList(currentUser, conf.getStrings(SUPERUSER_CONFIG_PROP, new String[0]));
		User activeUser = getActiveUser();
		if (!(superusers.contains(activeUser.getShortName()))) {
			throw new AccessDeniedException("User '" + (user != null ? user.getShortName() : "null") + "is not system or super user.");
		}
	}
	private boolean isSuperUser(User user) {
		boolean isSuper = false;
		isSuper = (superUserList != null && superUserList.contains(user.getShortName()));
		if (LOG.isDebugEnabled()) {
			LOG.debug("IsSuperCheck on [" + user.getShortName() + "] returns [" + isSuper + "]");
		}
		return isSuper;
	}
	protected boolean isSpecialTable(HRegionInfo regionInfo) {
		return isSpecialTable(regionInfo.getTable().getName());
	}
	protected boolean isSpecialTable(byte[] tableName) {
		return isSpecialTable(Bytes.toString(tableName));
	}
	protected boolean isSpecialTable(String tableNameStr) {
		return tableNameStr.equals("hbase:meta") ||  tableNameStr.equals("-ROOT-") || tableNameStr.equals(".META.");
	}
	@SuppressWarnings("unused")
	private String getUser() {
		User u = getActiveUser();
		if (u == null) {
			return "(user:unknown)";
		} else {
			String groups = (u.getGroupNames() == null) ? "" : StringUtils.join(u.getGroupNames(), ",");
			return "(user:" + u.getShortName() + ", groups: [" + groups + "])";
		}
	}
	private User getActiveUser() {
		User user = RequestContext.getRequestUser();
		if (!RequestContext.isInRequestContext()) {
			// for non-rpc handling, fallback to system user
			try {
				user = User.getCurrent();
			} catch (IOException e) {
				LOG.error("Unable to find the current user");
				user = null;
			}
		}
		return user;
	}
	
	private String getRemoteAddress() {
		RequestContext reqContext = RequestContext.get();
		InetAddress    remoteAddr = reqContext != null ? reqContext.getRemoteAddress() : null;
		String         strAddr    = remoteAddr != null ? remoteAddr.getHostAddress() : null;

		return strAddr;
	}

	// Methods that are used within the CoProcessor 
	private void requireScannerOwner(InternalScanner s) throws AccessDeniedException {
		if (RequestContext.isInRequestContext()) {
			String requestUserName = RequestContext.getRequestUserName();
			String owner = scannerOwners.get(s);
			if (owner != null && !owner.equals(requestUserName)) {
				throw new AccessDeniedException("User '" + requestUserName + "' is not the scanner owner!");
			}
		}
	}
	// Methods that are delegated to AUTHManager 
	public boolean isPermissionGranted(User user, Action action) {
		if (isSuperUser(user)) {
			return true;
		} else {
			return accessController.isAccessAllowed(user, action);
		}
	}
	public boolean isPermissionGranted(User user, byte[] tableName, Action action) {
		if (isSuperUser(user)) {
			return true;
		} else {
			return accessController.isAccessAllowed(user, tableName, action);
		}
	}
	public boolean isPermissionGranted(User user, byte[] tableName, byte[] colf, Action action) {
		if (isSuperUser(user)) {
			return true;
		} else {
			return accessController.isAccessAllowed(user, tableName, colf, WILDCARD_MATCH, action);
		}
	}
	public boolean isPermissionGranted(User user, byte[] tableName, byte[] colf, byte[] col, Action action) {
		if (isSuperUser(user)) {
			return true;
		} else {
			return accessController.isAccessAllowed(user, tableName, colf, col, action);
		}
	}
	// Methods that are internally used by co-processors 
	@SuppressWarnings("unchecked")
	public void requirePermission(String request, Action action, RegionCoprocessorEnvironment rEnv, Map<byte[], ? extends Collection<?>> families) throws IOException {
		HRegionInfo hri = rEnv.getRegion().getRegionInfo();
		byte[] tableName = hri.getTable().getName() ;
		String tableNameStr = Bytes.toString(tableName);
		if (hri.isMetaTable() || hri.isMetaRegion()) {
			if (action == TablePermission.Action.READ) {
				return;
			}
		}
		User user = getActiveUser();
		if (user == null) {
			throw new AccessDeniedException("No user associated with request (" + request + ") for action: " + action + "on table:" + tableName);
		}
		if (isSuperUser(user)) {
			return;
		}
		if (action == TablePermission.Action.WRITE && (hri.isMetaTable() || hri.isMetaRegion()) && (isPermissionGranted(user, Permission.Action.CREATE) || isPermissionGranted(user, Permission.Action.ADMIN))) {
			return;
		}
		if (isPermissionGranted(user, tableName, (byte[]) null, action)) {
			return;
		}
		if (families != null && families.size() > 0) {
			// all families must pass
			for (Map.Entry<byte[], ? extends Collection<?>> family : families.entrySet()) {
				// a) check for family level access
				if (isPermissionGranted(user, tableName, family.getKey(), action)) {
					continue; // family-level permission overrides per-qualifier
				}
				// b) qualifier level access can still succeed
				if ((family.getValue() != null) && (family.getValue().size() > 0)) {
					if (family.getValue() instanceof Set) { // Set<byte[]> - Set
															// of Columns
						// for each qualifier of the family
						Set<byte[]> qualifierSet = (Set<byte[]>) family.getValue();
						for (byte[] qualifier : qualifierSet) {
							if (!isPermissionGranted(user, tableName, family.getKey(), qualifier, action)) {
								if (accessController.isAudited(tableName)) {
									auditEvent(request, tableName, family.getKey(), qualifier, null, null, user, accessDeniedFlag);
								}
								throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + action + ", tableName:" + tableNameStr + ", family:" + Bytes.toString(family.getKey()) + ",column: " + Bytes.toString(qualifier));
							}
						}
					} else if (family.getValue() instanceof List) { // List<KeyValue>
																	// - List of
																	// KeyValue
																	// pair
						List<KeyValue> kvList = (List<KeyValue>) family.getValue();
						for (KeyValue kv : kvList) {
							if (!isPermissionGranted(user, tableName, family.getKey(), kv.getQualifier(), action)) {
								if (accessController.isAudited(tableName)) {
									auditEvent(request, tableName, family.getKey(), kv.getQualifier(), null, null, user, accessDeniedFlag);
								}
								throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + action + ", tableName:" + tableNameStr + ", family:" + Bytes.toString(family.getKey()) + ",column: " + Bytes.toString(kv.getQualifier()));
							}
						}
					}
				} else {
					if (accessController.isAudited(tableName)) {
						auditEvent(request, tableName, family.getKey(), null, null, null, user, accessDeniedFlag);
					}
					throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + action + ", tableName:" + tableNameStr + ", family:" + Bytes.toString(family.getKey()) + ", no columns found.");
				}
			}
			return;
		}
		if (accessController.isAudited(tableName)) {
			auditEvent(request, tableName, null, null, null, null, user, accessDeniedFlag);
		}
		throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + action + ", tableName:" + tableNameStr);
	}
	// Check if the user has global permission ...
	protected void requireGlobalPermission(String request, String objName, Permission.Action action) throws AccessDeniedException {
		User user = getActiveUser();
		if (!isPermissionGranted(user, action)) {
			if (accessController.isAudited(WILDCARD_MATCH)) {
				auditEvent(request, objName, null, null, null, null, user, accessDeniedFlag);
			}
			throw new AccessDeniedException("Insufficient permissions for user '" + getActiveUser() + "' (global, action=" + action + ")");
		}
	}
	protected void requirePermission(String request, byte[] tableName, Permission.Action action) throws AccessDeniedException {
		User user = getActiveUser();
		if (!isPermissionGranted(user, tableName, action)) {
			if (accessController.isAudited(tableName)) {
				auditEvent(request, tableName, null, null, null, null, user, accessDeniedFlag);
			}
			throw new AccessDeniedException("Insufficient permissions for user '" + getActiveUser() + "' (global, action=" + action + ")");
		}
	}
	protected void requirePermission(String request, byte[] aTableName, byte[] aColumnFamily, byte[] aQualifier, Permission.Action... actions) throws AccessDeniedException {
		User user = getActiveUser();
		boolean isAllowed = false;

		for (Action action : actions) {
			isAllowed = isPermissionGranted(user, aTableName, aColumnFamily, aQualifier, action);

			if(isAllowed) {
				break;
			}
		}
		
		if (!isAllowed) {
			if (accessController.isAudited(aTableName)) {
				auditEvent(request, aTableName, aColumnFamily, aQualifier, null, null, user, accessDeniedFlag);
			}
			Permission.Action deniedAction = actions.length > 0 ? actions[0] : null;

			throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + deniedAction + ", tableName:" + Bytes.toString(aTableName) + ", family:" + Bytes.toString(aColumnFamily) + ",column: " + Bytes.toString(aQualifier));
		}
	}
	protected void requirePermission(String request, Permission.Action perm, RegionCoprocessorEnvironment env, Collection<byte[]> families) throws IOException {
		HashMap<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();

		if(families != null) {
			for (byte[] family : families) {
				familyMap.put(family, null);
			}
		}
		requirePermission(request, perm, env, familyMap);
	}
	protected boolean isPermissionGranted(String request, User requestUser, Permission.Action perm, RegionCoprocessorEnvironment env, Map<byte[], NavigableSet<byte[]>> familyMap) {
		boolean ret = true;
		try {
			requirePermission(request, perm, env, familyMap);
		} catch (Throwable t) {
			ret = false;
		}
		return ret;
	}
	protected boolean hasFamilyQualifierPermission(User requestUser, Permission.Action perm, RegionCoprocessorEnvironment env, Map<byte[], NavigableSet<byte[]>> familyMap) {
		User user = requestUser;
		byte[] tableName = getTableName(env);
		if (familyMap != null && familyMap.size() > 0) {
			for (Map.Entry<byte[], NavigableSet<byte[]>> family : familyMap.entrySet()) {
				if (family.getValue() != null && !family.getValue().isEmpty()) {
					for (byte[] qualifier : family.getValue()) {
						boolean isGranted = isPermissionGranted(user, tableName, family.getKey(), qualifier, perm);
						LOG.info(":=> hasFamilyQualifierPermission: T(" + Bytes.toString(tableName) + "), family: (" + Bytes.toString(family.getKey() ) + "), Q(" + Bytes.toString(qualifier) + "), Permission: [" + perm + "] => [" + isGranted + "]") ;
						if (isGranted) {
							return true;
						}
					}
				} else {
					boolean isGranted = isPermissionGranted(user, tableName, family.getKey(), perm);
					LOG.info(":=>  hasFamilyPermission: T(" + Bytes.toString(tableName) + "), family: (" + Bytes.toString(family.getKey() ) + ", Permission: [" + perm + "] => [" + isGranted + "]") ;
					if (isGranted) {
						return true;
					}
				}
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Empty family map passed for permission check");
			}
		}
		return false;
	}
	
	
	public void checkPermissions(Permission[] permissions) throws IOException {
		String tableName = regionEnv.getRegion().getTableDesc().getTableName().getNameAsString() ;
		for (Permission permission : permissions) {
			if (permission instanceof TablePermission) {
				TablePermission tperm = (TablePermission) permission;
				for (Permission.Action action : permission.getActions()) {
					if (! tperm.getTableName().getNameAsString().equals(tableName)) {
						throw new AccessDeniedException(String.format("This method can only execute at the table specified in TablePermission. " + "Table of the region:%s , requested table:%s", tableName, 
																	  tperm.getTableName().getNameAsString()));
					}
					HashMap<byte[], Set<byte[]>> familyMap = Maps.newHashMapWithExpectedSize(1);
					if (tperm.getFamily() != null) {
						if (tperm.getQualifier() != null) {
							familyMap.put(tperm.getFamily(), Sets.newHashSet(tperm.getQualifier()));
						} else {
							familyMap.put(tperm.getFamily(), null);
						}
					}
					requirePermission("checkPermissions", action, regionEnv, familyMap);
				}
			} else {
				for (Permission.Action action : permission.getActions()) {
					byte[] tname = regionEnv.getRegion().getTableDesc().getTableName().getName() ;
					requirePermission("checkPermissions", tname, action);
				}
			}
		}
	}
	
	@Override
	public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor column) throws IOException {
		auditEvent("addColumn", tableName.getName(), column.getName(), null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		auditEvent("assign", regionInfo.getTable().getNameAsString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postBalance(ObserverContext<MasterCoprocessorEnvironment> c,List<RegionPlan> aRegPlanList) throws IOException {
		auditEvent("balance", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean oldValue, boolean newValue) throws IOException {
		auditEvent("balanceSwitch", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		auditEvent("cloneSnapshot", hTableDescriptor.getNameAsString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		auditEvent("createTable", desc.getNameAsString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		auditEvent("delete", delete.toString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, byte[] col) throws IOException {
		auditEvent("deleteColumn", tableName.getName(), col, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		auditEvent("deleteSnapShot", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		auditEvent("deleteTable", tableName.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		auditEvent("disableTable", tableName.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		auditEvent("enableTable", tableName.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		auditEvent("modifyColumn", tableName.getName(), descriptor.getName(), null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HTableDescriptor htd) throws IOException {
		auditEvent("modifyTable", tableName.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		auditEvent("move", region.getTable().getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
		auditEvent("open", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		auditEvent("restoreSnapshot", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		try {
			scannerOwners.remove(s);
		} finally {
			byte[] tableName = getTableName(c.getEnvironment());

			if (!isSpecialTable(tableName)) {
				auditEvent("scannerClose", tableName, null, null, null, null, getActiveUser(), accessGrantedFlag);
			}
		}
	}
	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		try {
			User user = getActiveUser();
			if (user != null && user.getShortName() != null) {
				scannerOwners.put(s, user.getShortName());
			}
		} finally {
			byte[] tableName = getTableName(c.getEnvironment());

			if (!isSpecialTable(tableName)) {
				auditEvent("scannerOpen", tableName, null, null, null, null, getActiveUser(), accessGrantedFlag);
			}
		}
		return s;
	}
	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		auditEvent("snapshot", hTableDescriptor.getNameAsString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		if(UpdateXaPoliciesOnGrantRevoke) {
			XaAccessControlLists.init(ctx.getEnvironment().getMasterServices());
		}

		auditEvent("startMaster", (String) null, null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
		auditEvent("unassign", regionInfo.getTable().getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor column) throws IOException {
		requirePermission("addColumn", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
		requirePermission("append", TablePermission.Action.WRITE, c.getEnvironment(), append.getFamilyCellMap());
		return null;
	}
	@Override
	public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		requirePermission("assign", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	@Override
	public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("balance", null, Permission.Action.ADMIN);
	}
	@Override
	public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean newValue) throws IOException {
		requirePermission("balanceSwitch", null, Permission.Action.ADMIN);
		return newValue;
	}
	@Override
	public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
		List<byte[]> cfs = new LinkedList<byte[]>();
		for (Pair<byte[], String> el : familyPaths) {
			cfs.add(el.getFirst());
		}
		requirePermission("bulkLoadHFile", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}
	@Override
	public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		Collection<byte[]> familyMap = Arrays.asList(new byte[][] { family });
		requirePermission("checkAndDelete", TablePermission.Action.READ, c.getEnvironment(), familyMap);
		requirePermission("checkAndDelete", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
		return result;
	}
	@Override
	public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		Collection<byte[]> familyMap = Arrays.asList(new byte[][] { family });
		requirePermission("checkAndPut", TablePermission.Action.READ, c.getEnvironment(), familyMap);
		requirePermission("checkAndPut", TablePermission.Action.WRITE, c.getEnvironment(), familyMap);
		return result;
	}
	@Override
	public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("cloneSnapshot", null, Permission.Action.ADMIN);
	}
	@Override
	public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) throws IOException {
		requirePermission("close", getTableName(e.getEnvironment()), Permission.Action.ADMIN);
	}
	@Override
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner,ScanType scanType) throws IOException {
		requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.ADMIN, Action.CREATE);
		return scanner;
	}
	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> e, Store store, List<StoreFile> candidates) throws IOException {
		requirePermission("compactSelection", getTableName(e.getEnvironment()), null, null, Action.ADMIN, Action.CREATE);
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		requirePermission("createTable", desc.getName(), Permission.Action.CREATE);
	}
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		requirePermission("delete", TablePermission.Action.WRITE, c.getEnvironment(), delete.getFamilyCellMap());
	}
	@Override
	public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, byte[] col) throws IOException {
		requirePermission("deleteColumn", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		requirePermission("deleteSnapshot", null, Permission.Action.ADMIN);
	}
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("deleteTable", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("disableTable", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		requirePermission("enableTable", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		requirePermission("exists", TablePermission.Action.READ, c.getEnvironment(), get.familySet());
		return exists;
	}
	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		requirePermission("flush", getTableName(e.getEnvironment()), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		requirePermission("getClosestRowBefore", TablePermission.Action.READ, c.getEnvironment(), (family != null ? Lists.newArrayList(family) : null));
	}
	@Override
	public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
		requirePermission("increment", TablePermission.Action.WRITE, c.getEnvironment(), increment.getFamilyCellMap().keySet());
		
		return null;
	}
	@Override
	public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
		requirePermission("incrementColumnValue", TablePermission.Action.READ, c.getEnvironment(), Arrays.asList(new byte[][] { family }));
		requirePermission("incrementColumnValue", TablePermission.Action.WRITE, c.getEnvironment(), Arrays.asList(new byte[][] { family }));
		return -1;
	}
	@Override
	public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		requirePermission("modifyColumn", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HTableDescriptor htd) throws IOException {
		requirePermission("modifyTable", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
	}
	@Override
	public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		requirePermission("move", region.getTable().getName() , null, null, Action.ADMIN);
	}
	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		RegionCoprocessorEnvironment env = e.getEnvironment();
		final HRegion region = env.getRegion();
		if (region == null) {
			LOG.error("NULL region from RegionCoprocessorEnvironment in preOpen()");
			return;
		} else {
			HRegionInfo regionInfo = region.getRegionInfo();
			if (isSpecialTable(regionInfo)) {
				isSystemOrSuperUser(regionEnv.getConfiguration());
			} else {
				requirePermission("open", getTableName(e.getEnvironment()), Action.ADMIN);
			}
		}
	}
	@Override
	public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("restoreSnapshot", hTableDescriptor.getName(), Permission.Action.ADMIN);
	}

	@Override
	public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		requireScannerOwner(s);
	}
	@Override
	public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
		requireScannerOwner(s);
		return hasNext;
	}
	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		RegionCoprocessorEnvironment e = c.getEnvironment();
		User user = getActiveUser();
		boolean isGranted = isPermissionGranted("scannerOpen", user, TablePermission.Action.READ, e, scan.getFamilyMap());
		if (!isGranted) {
			if (hasFamilyQualifierPermission(user, TablePermission.Action.READ, e, scan.getFamilyMap())) {
				byte[] table = getTableName(e);
				XaSecureAccessControlFilter filter = new XaSecureAccessControlFilter(user, table);
				if (scan.hasFilter()) {
					FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL, Lists.newArrayList(filter, scan.getFilter()));
					scan.setFilter(wrapper);
				} else {
					scan.setFilter(filter);
				}
			} else {
				throw new AccessDeniedException("Insufficient permissions for user '" + (user != null ? user.getShortName() : "null") + "' for scanner open on table " + Bytes.toString(getTableName(e)));
			}
		}
		return s;
	}
	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("shutdown", null, Permission.Action.ADMIN);
	}
	@Override
	public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		requirePermission("snapshot", hTableDescriptor.getName(), Permission.Action.ADMIN);
	}
	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		requirePermission("split", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
	}
	@Override
	public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		requirePermission("stopMaster", null, Permission.Action.ADMIN);
	}
	@Override
	public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
		requirePermission("stop", null, Permission.Action.ADMIN);
	}
	@Override
	public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
		requirePermission("unassign", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	private String coprocessorType = "unknown";
	private static final String MASTER_COPROCESSOR_TYPE = "master";
	private static final String REGIONAL_COPROCESSOR_TYPE = "regional";
	private static final String REGIONAL_SERVER_COPROCESSOR_TYPE = "regionalServer";
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		AuditProviderFactory.ApplicationType appType = AuditProviderFactory.ApplicationType.Unknown;

		if (env instanceof MasterCoprocessorEnvironment) {
			coprocessorType = MASTER_COPROCESSOR_TYPE;
			appType = AuditProviderFactory.ApplicationType.HBaseMaster;
		} else if (env instanceof RegionServerCoprocessorEnvironment) {
			coprocessorType = REGIONAL_SERVER_COPROCESSOR_TYPE;
			appType = AuditProviderFactory.ApplicationType.HBaseRegionalServer;
		} else if (env instanceof RegionCoprocessorEnvironment) {
			regionEnv = (RegionCoprocessorEnvironment) env;
			coprocessorType = REGIONAL_COPROCESSOR_TYPE;
			appType = AuditProviderFactory.ApplicationType.HBaseRegionalServer;
		}

		XaSecureConfiguration.getInstance().initAudit(appType);

		if (superUserList == null) {
			superUserList = new ArrayList<String>();
			Configuration conf = env.getConfiguration();
			String[] users = conf.getStrings(SUPERUSER_CONFIG_PROP);
			if (users != null) {
				for (String user : users) {
					user = user.trim();
					LOG.info("Start() - Adding Super User(" + user + ")");
					superUserList.add(user);
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Start of Coprocessor: [" + coprocessorType + "] with superUserList [" + superUserList + "]");
		}
	}
	@Override
	public void stop(CoprocessorEnvironment env) {
	}
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
		requirePermission("put", TablePermission.Action.WRITE, c.getEnvironment(), put.getFamilyCellMap());
	}
	
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) {
		byte[] tableName = getTableName(c.getEnvironment());
		if (!isSpecialTable(tableName)) {
			if (accessController.isAudited(tableName)) {
				Map<byte[], List<Cell>> colf2KeyValMap = put.getFamilyCellMap() ;
				for (byte[] colf : colf2KeyValMap.keySet()) {
					if (colf != null) {
						List<Cell> kvList = colf2KeyValMap.get(colf);
						for (Cell kv : kvList) {
							auditEvent("Put", tableName, CellUtil.cloneFamily(kv),   CellUtil.cloneQualifier(kv) , CellUtil.cloneRow(kv), CellUtil.cloneValue(kv), getActiveUser(), accessGrantedFlag);
						}
					}
				}

				
			}
		}
	}

	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> rEnv, final Get get, final List<Cell> result) throws IOException {
		RegionCoprocessorEnvironment e = rEnv.getEnvironment();
		User requestUser = getActiveUser();


		if (LOG.isDebugEnabled())
		{
			StringBuilder fields = new StringBuilder() ;
			Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap() ;
			if (familyMap != null && familyMap.size() > 0) {
				for(byte[] columnfamily : familyMap.keySet() ) {
					if (columnfamily != null && columnfamily.length > 0) {
						NavigableSet<byte[]> columnNameSet = familyMap.get(columnfamily) ;
						if (columnNameSet != null && columnNameSet.size() > 0) {
	 						for(byte[] columnname : columnNameSet) {
								fields.append("Field[" + Bytes.toString(columnfamily) + ":" + Bytes.toString(columnname) + "],") ;
							}
						}
						else {
							fields.append("Field[" + Bytes.toString(columnfamily) + ":null],") ;
						}
					}
				}
			}
			else {
				if (familyMap == null){
					fields.append("{null}") ;
				}
				else {
					fields.append("{empty}") ;
				}
			}
			LOG.debug("preGet is checking permission for the following fields: {" + fields.toString() + "}");
		}
		
		boolean isPermGranted = isPermissionGranted("get", requestUser, TablePermission.Action.READ, e, get.getFamilyMap());
		
		if (!isPermGranted) {
			isPermGranted = hasFamilyQualifierPermission(requestUser, TablePermission.Action.READ, e, get.getFamilyMap());
			if (isPermGranted) {
				byte[] table = getTableName(e);
				XaSecureAccessControlFilter filter = new XaSecureAccessControlFilter(requestUser, table);
				if (get.getFilter() != null) {
					FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL, Lists.newArrayList(filter, get.getFilter()));
					get.setFilter(wrapper);
				} else {
					get.setFilter(filter);
				}
			} else {
				throw new AccessDeniedException("Insufficient permissions (table=" + e.getRegion().getTableDesc().getNameAsString() + ", action=READ)");
			}
		}
	}
	@Override
	public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> env, final Get get, final List<Cell> results) throws IOException {
		HRegionInfo hri = env.getEnvironment().getRegion().getRegionInfo();
		
		byte[] tableName = hri.getTable().getName() ;
		
		if (!isSpecialTable(tableName)) {
			try {
				if (accessController.isAudited(tableName)) {
					for (Cell cell : results) {
						auditEvent("Get", tableName, cell.getFamily(), cell.getQualifier(), cell.getRow(), cell.getValue(), getActiveUser(), accessGrantedFlag);
					}
				}
			} catch (Throwable t) {
			}
		}
	}

	@Override
	public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
	    requirePermission("regionOffline", regionInfo.getTable().getName(), null, null, Action.ADMIN);
	}
	@Override
	public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		auditEvent("regionOffline", regionInfo.getTable().getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		requireGlobalPermission("createNamespace", ns.getName(), Action.ADMIN);
	}
	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		if (accessController.isAudited(WILDCARD_MATCH)) {
			auditEvent("createNamespace", ns.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
		}
	}
	@Override
	public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		requireGlobalPermission("deleteNamespace", namespace, Action.ADMIN);
	}
	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		if (accessController.isAudited(WILDCARD_MATCH)) {
			auditEvent("deleteNamespace", namespace, null, null, null, null, getActiveUser(), accessGrantedFlag);
		}
	}
	@Override
	public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		requireGlobalPermission("modifyNamespace", ns.getName(), Action.ADMIN);
	}
	@Override
	public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		if (accessController.isAudited(WILDCARD_MATCH)) {
			auditEvent("modifyNamespace", ns.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
		}
	}
	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList,  List<HTableDescriptor> descriptors) throws IOException {
		if (tableNamesList == null || tableNamesList.isEmpty()) { // If the list is empty, this is a request for all table descriptors and requires GLOBAL ADMIN privs.
			requireGlobalPermission("getTableDescriptors", WILDCARD, Action.ADMIN);
		} else { // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the request can be granted.
			for (TableName tableName: tableNamesList) {
				requirePermission("getTableDescriptors", tableName.getName(), null, null, Action.ADMIN, Action.CREATE);
			}
		}
	}
	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors) throws IOException {
		if (descriptors == null || descriptors.isEmpty()) { // If the list is empty, this is a request for all table descriptors and requires GLOBAL ADMIN privs.
			auditEvent("getTableDescriptors", WILDCARD, null, null, null, null, getActiveUser(), accessGrantedFlag);
		} else { // Otherwise, if the requestor has ADMIN or CREATE privs for all listed tables, the request can be granted.
			for (HTableDescriptor descriptor : descriptors) {
				auditEvent("getTableDescriptors", descriptor.getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
			}
		}
	}
	@Override
	public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, HRegion regionA, HRegion regionB) throws IOException {
		requirePermission("mergeRegions", regionA.getTableDesc().getTableName().getName(), null, null, Action.ADMIN);
	}

	@Override
	public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, HRegion regionA, HRegion regionB, HRegion mergedRegion) throws IOException {
		auditEvent("mergeRegions", regionA.getTableDesc().getTableName().getName(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}

	public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, PrepareBulkLoadRequest request) throws IOException {
		List<byte[]> cfs = null;

		requirePermission("prePrepareBulkLoad", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}

	public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, CleanupBulkLoadRequest request) throws IOException {
		List<byte[]> cfs = null;

		requirePermission("preCleanupBulkLoad", Permission.Action.WRITE, ctx.getEnvironment(), cfs);
	}
	
	private void auditEvent(String eventName, byte[] tableName, byte[] columnFamilyName, byte[] qualifierName, byte[] row, byte[] value, User user, short accessFlag) {
		auditEvent(eventName, Bytes.toString(tableName), Bytes.toString(columnFamilyName), Bytes.toString(qualifierName), row, value, user, accessFlag);
	}
	
	private void auditEvent(String eventName, String tableName, String columnFamilyName, String qualifierName, byte[] row, byte[] value, User user, short accessFlag) {
		
		if (tableName != null && accessController.isAudited(tableName.getBytes())) {
			
			String resourceType = "table";
			String resourceName = tableName;
			if (columnFamilyName != null && columnFamilyName.length() > 0) {
				resourceName += "/" + columnFamilyName;
				resourceType = "columnFamily";
			}
			if (qualifierName != null && qualifierName.length() > 0) {
				resourceName += "/" + qualifierName;
				resourceType = "column";
			}
			
			HBaseAuditEvent auditEvent = new HBaseAuditEvent();

			auditEvent.setAclEnforcer(XaSecureModuleName);
			auditEvent.setResourceType(resourceType);
			auditEvent.setResourcePath(resourceName);
			auditEvent.setAction(eventName);
			auditEvent.setAccessType(eventName);
			auditEvent.setUser(user == null ? XaSecureHadoopConstants.AUDITLOG_EMPTY_STRING  : user.getShortName());
			auditEvent.setAccessResult(accessFlag);
			auditEvent.setClientIP(getRemoteAddress());
			auditEvent.setEventTime(getUTCDate());
			auditEvent.setRepositoryType(EnumRepositoryType.HBASE);
			auditEvent.setRepositoryName(repositoryName);
			
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Writing audit log [" + auditEvent + "] - START.");
				}
				AuditProviderFactory.getAuditProvider().log(auditEvent);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Writing audit log [" + auditEvent + "] - END.");
				}
			}
			catch(Throwable t) {
				LOG.error("ERROR during audit log [" + auditEvent + "]", t);
			}
			
		}
	}
	
	public static Date getUTCDate() {
		Calendar local=Calendar.getInstance();
	    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
	    GregorianCalendar utc = new GregorianCalendar(gmtTimeZone);
	    utc.setTimeInMillis(local.getTimeInMillis());
	    utc.add(Calendar.MILLISECOND, -offset);
	    return utc.getTime();
	}
	
	@Override
	public void grant(RpcController controller, AccessControlProtos.GrantRequest request, RpcCallback<AccessControlProtos.GrantResponse> done) {
		boolean isSuccess = false;

		if(UpdateXaPoliciesOnGrantRevoke) {
			GrantRevokeData grData = null;
	
			try {
				grData = createGrantData(request);
	
				XaAdminRESTClient xaAdmin = new XaAdminRESTClient();
	
			    xaAdmin.grantPrivilege(grData);
	
			    isSuccess = true;
			} catch(IOException excp) {
				LOG.warn("grant() failed", excp);
	
				ResponseConverter.setControllerException(controller, excp);
			} catch (Exception excp) {
				LOG.warn("grant() failed", excp);
	
				ResponseConverter.setControllerException(controller, new CoprocessorException(excp.getMessage()));
			} finally {
				byte[] tableName = grData == null ? null : StringUtil.getBytes(grData.getTables());
	
				if(accessController.isAudited(tableName)) {
					byte[] colFamily = grData == null ? null : StringUtil.getBytes(grData.getColumnFamilies());
					byte[] qualifier = grData == null ? null : StringUtil.getBytes(grData.getColumns());
	
					// Note: failed return from REST call will be logged as 'DENIED'
					auditEvent("grant", tableName, colFamily, qualifier, null, null, getActiveUser(), isSuccess ? accessGrantedFlag : accessDeniedFlag);
				}
			}
		}

		AccessControlProtos.GrantResponse response = isSuccess ? AccessControlProtos.GrantResponse.getDefaultInstance() : null;

		done.run(response);
	}

	@Override
	public void revoke(RpcController controller, AccessControlProtos.RevokeRequest request, RpcCallback<AccessControlProtos.RevokeResponse> done) {
		boolean isSuccess = false;

		if(UpdateXaPoliciesOnGrantRevoke) {
			GrantRevokeData grData = null;
	
			try {
				grData = createRevokeData(request);
	
				XaAdminRESTClient xaAdmin = new XaAdminRESTClient();
	
			    xaAdmin.revokePrivilege(grData);
	
			    isSuccess = true;
			} catch(IOException excp) {
				LOG.warn("revoke() failed", excp);
	
				ResponseConverter.setControllerException(controller, excp);
			} catch (Exception excp) {
				LOG.warn("revoke() failed", excp);
	
				ResponseConverter.setControllerException(controller, new CoprocessorException(excp.getMessage()));
			} finally {
				byte[] tableName = grData == null ? null : StringUtil.getBytes(grData.getTables());
	
				if(accessController.isAudited(tableName)) {
					byte[] colFamily = grData == null ? null : StringUtil.getBytes(grData.getColumnFamilies());
					byte[] qualifier = grData == null ? null : StringUtil.getBytes(grData.getColumns());
	
					// Note: failed return from REST call will be logged as 'DENIED'
					auditEvent("revoke", tableName, colFamily, qualifier, null, null, getActiveUser(), isSuccess ? accessGrantedFlag : accessDeniedFlag);
				}
			}
		}

		AccessControlProtos.RevokeResponse response = isSuccess ? AccessControlProtos.RevokeResponse.getDefaultInstance() : null;

		done.run(response);
	}

	@Override
	public void checkPermissions(RpcController controller, AccessControlProtos.CheckPermissionsRequest request, RpcCallback<AccessControlProtos.CheckPermissionsResponse> done) {
		LOG.debug("checkPermissions(): ");
	}

	@Override
	public void getUserPermissions(RpcController controller, AccessControlProtos.GetUserPermissionsRequest request, RpcCallback<AccessControlProtos.GetUserPermissionsResponse> done) {
		LOG.debug("getUserPermissions(): ");
	}

	@Override
	public Service getService() {
	    return AccessControlProtos.AccessControlService.newReflectiveService(this);
	}

	private GrantRevokeData createGrantData(AccessControlProtos.GrantRequest request) throws Exception {
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.UserPermission up   = request.getUserPermission();
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.Permission     perm = up == null ? null : up.getPermission();

		UserPermission      userPerm  = up == null ? null : ProtobufUtil.toUserPermission(up);
		Permission.Action[] actions   = userPerm == null ? null : userPerm.getActions();
		String              userName  = userPerm == null ? null : Bytes.toString(userPerm.getUser());
		String              tableName = null;
		String              colFamily = null;
		String              qualifier = null;

		if(perm == null) {
			throw new Exception("grant(): invalid data - permission is null");
		}

		if(StringUtil.isEmpty(userName)) {
			throw new Exception("grant(): invalid data - username empty");
		}

		if ((actions == null) || (actions.length == 0)) {
			throw new Exception("grant(): invalid data - no action specified");
		}

		switch(perm.getType()) {
			case Global:
				tableName = colFamily = qualifier = "*";
			break;

			case Table:
				tableName = Bytes.toString(userPerm.getTableName().getName());
				colFamily = Bytes.toString(userPerm.getFamily());
				qualifier = Bytes.toString(userPerm.getQualifier());
			break;

			case Namespace:
			default:
				LOG.warn("grant(): ignoring type '" + perm.getType().name() + "'");
			break;
		}
		
		if(StringUtil.isEmpty(tableName) && StringUtil.isEmpty(colFamily) && StringUtil.isEmpty(qualifier)) {
			throw new Exception("grant(): table/columnFamily/columnQualifier not specified");
		}

		PermMap permMap = new PermMap();

		if(userName.startsWith(GROUP_PREFIX)) {
			permMap.addGroup(userName.substring(GROUP_PREFIX.length()));
		} else {
			permMap.addUser(userName);
		}

		for (int i = 0; i < actions.length; i++) {
			switch(actions[i].code()) {
				case 'R':
				case 'W':
				case 'C':
				case 'A':
					permMap.addPerm(actions[i].name());
				break;

				default:
					LOG.warn("grant(): ignoring action '" + actions[i].name() + "' for user '" + userName + "'");
			}
		}

		User   activeUser = getActiveUser();
		String grantor    = activeUser != null ? activeUser.getShortName() : null;

		GrantRevokeData grData = new GrantRevokeData();

		grData.setHBaseData(grantor, repositoryName,  tableName,  qualifier, colFamily, permMap);

		return grData;
	}

	private GrantRevokeData createRevokeData(AccessControlProtos.RevokeRequest request) throws Exception {
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.UserPermission up   = request.getUserPermission();
		org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.Permission     perm = up == null ? null : up.getPermission();

		UserPermission      userPerm  = up == null ? null : ProtobufUtil.toUserPermission(up);
		String              userName  = userPerm == null ? null : Bytes.toString(userPerm.getUser());
		String              tableName = null;
		String              colFamily = null;
		String              qualifier = null;

		if(perm == null) {
			throw new Exception("revoke(): invalid data - permission is null");
		}

		if(StringUtil.isEmpty(userName)) {
			throw new Exception("revoke(): invalid data - username empty");
		}

		switch(perm.getType()) {
			case Global :
				tableName = colFamily = qualifier = "*";
			break;

			case Table :
				tableName = Bytes.toString(userPerm.getTableName().getName());
				colFamily = Bytes.toString(userPerm.getFamily());
				qualifier = Bytes.toString(userPerm.getQualifier());
			break;

			case Namespace:
			default:
				LOG.warn("revoke(): ignoring type '" + perm.getType().name() + "'");
			break;
		}
		
		if(StringUtil.isEmpty(tableName) && StringUtil.isEmpty(colFamily) && StringUtil.isEmpty(qualifier)) {
			throw new Exception("revoke(): table/columnFamily/columnQualifier not specified");
		}

		PermMap permMap = new PermMap();

		if(userName.startsWith(GROUP_PREFIX)) {
			permMap.addGroup(userName.substring(GROUP_PREFIX.length()));
		} else {
			permMap.addUser(userName);
		}

		// revoke removes all permissions
		permMap.addPerm(Permission.Action.READ.name());
		permMap.addPerm(Permission.Action.WRITE.name());
		permMap.addPerm(Permission.Action.CREATE.name());
		permMap.addPerm(Permission.Action.ADMIN.name());

		User   activeUser = getActiveUser();
		String grantor    = activeUser != null ? activeUser.getShortName() : null;

		GrantRevokeData grData = new GrantRevokeData();

		grData.setHBaseData(grantor, repositoryName,  tableName,  qualifier, colFamily, permMap);

		return grData;
	}
}
