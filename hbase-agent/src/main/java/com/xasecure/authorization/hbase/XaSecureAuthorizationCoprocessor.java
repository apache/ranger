/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/
 /**
  *
  *	@version: 1.0.004
  *
  */
package com.xasecure.authorization.hbase;
import java.io.IOException;
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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.HBaseAuditEvent;
import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class XaSecureAuthorizationCoprocessor extends BaseRegionObserver implements MasterObserver, RegionServerObserver {
	private static final Log AUDIT = LogFactory.getLog("xaaudit." + XaSecureAuthorizationCoprocessor.class.getName());
	private static final Log LOG = LogFactory.getLog(XaSecureAuthorizationCoprocessor.class.getName());
	private static final String XaSecureModuleName = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_XASECURE_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME) ;
	private static final short  accessGrantedFlag  = 1;
	private static final short  accessDeniedFlag   = 0;
	private static final String repositoryName          = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);

		
	private static final String SUPERUSER_CONFIG_PROP = "hbase.superuser";
	private static final byte[] WILDCARD_MATCH = "*".getBytes();
	
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
		return tableNameStr.equals("-ROOT-") || tableNameStr.equals(".META.");
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
		for (Action action : actions) {
			if (!isPermissionGranted(user, aTableName, aColumnFamily, aQualifier, action)) {
				if (accessController.isAudited(aTableName)) {
					auditEvent(request, aTableName, aColumnFamily, aQualifier, null, null, user, accessDeniedFlag);
				}
				throw new AccessDeniedException("Insufficient permissions for user '" + user + "',action: " + action + ", tableName:" + Bytes.toString(aTableName) + ", family:" + Bytes.toString(aColumnFamily) + ",column: " + Bytes.toString(aQualifier));
			}
		}
	}
	protected void requirePermission(String request, Permission.Action perm, RegionCoprocessorEnvironment env, Collection<byte[]> families) throws IOException {
		HashMap<byte[], Set<byte[]>> familyMap = new HashMap<byte[], Set<byte[]>>();
		for (byte[] family : families) {
			familyMap.put(family, null);
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
			auditEvent("scannerClose", getTableName(c.getEnvironment()), null, null, null, null, getActiveUser(), accessGrantedFlag);
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
			auditEvent("scannerOpen", getTableName(c.getEnvironment()), null, null, null, null, getActiveUser(), accessGrantedFlag);
		}
		return s;
	}
	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		auditEvent("snapshot", hTableDescriptor.getNameAsString(), null, null, null, null, getActiveUser(), accessGrantedFlag);
	}
	@Override
	public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
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
		requirePermission("compact", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
		return scanner;
	}
	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> e, Store store, List<StoreFile> candidates) throws IOException {
		requirePermission("compactSelection", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
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
		requirePermission("flush", getTableName(e.getEnvironment()), null, null, Action.ADMIN);
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
		if (env instanceof MasterCoprocessorEnvironment) {
			coprocessorType = MASTER_COPROCESSOR_TYPE;
		} else if (env instanceof RegionServerCoprocessorEnvironment) {
			coprocessorType = REGIONAL_SERVER_COPROCESSOR_TYPE;
		} else if (env instanceof RegionCoprocessorEnvironment) {
			regionEnv = (RegionCoprocessorEnvironment) env;
			coprocessorType = REGIONAL_COPROCESSOR_TYPE;
		}
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
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> rEnv, Get get, List<KeyValue> keyValList) throws IOException {
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
	public void postGet(final ObserverContext<RegionCoprocessorEnvironment> env, final Get get, final List<KeyValue> result) throws IOException {
		HRegionInfo hri = env.getEnvironment().getRegion().getRegionInfo();
		
		byte[] tableName = hri.getTable().getName() ;
		
		if (!isSpecialTable(tableName)) {
			try {
				if (accessController.isAudited(tableName)) {
					for (KeyValue kv : result) {
						auditEvent("Get", tableName, kv.getFamily(), kv.getQualifier(), kv.getKey(), kv.getValue(), getActiveUser(), accessGrantedFlag);
					}
				}
			} catch (Throwable t) {
			}
		}
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
			auditEvent.setClientIP(null); // TODO:
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
	@Override
	public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName,HColumnDescriptor aHColDesc) throws IOException {
	}
	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx, NamespaceDescriptor aNamespaceDesc) throws IOException {
	}
	@Override
	public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, HTableDescriptor arg1, HRegionInfo[] aRegionInfoList) throws IOException {
	}
	@Override
	public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName, byte[] aColumnFamilyName) throws IOException {
	}
	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx, String arg1) throws IOException {
	}
	@Override
	public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> aMctx, List<HTableDescriptor> aHTableDescList) throws IOException {
	}
	@Override
	public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName,HColumnDescriptor aHColDesc) throws IOException {
	}
	@Override
	public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx,NamespaceDescriptor aNamespaceDesc) throws IOException {
	}
	@Override
	public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName,HTableDescriptor aHTableDesc) throws IOException {
	}
	@Override
	public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> aMctx, HRegionInfo aHRegInfo) throws IOException {
	}
	@Override
	public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName,HColumnDescriptor aHColDesc) throws IOException {
	}
	@Override
	public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx,NamespaceDescriptor aNamespaceDesc) throws IOException {
	}
	@Override
	public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx,HTableDescriptor aHTableDesc, HRegionInfo[] aHRegInfoList) throws IOException {
	}
	@Override
	public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName,byte[] aColumnFamilyName) throws IOException {
	}
	@Override
	public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx, String aNamespaceName) throws IOException {
	}
	@Override
	public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName) throws IOException {
	}
	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> aMctx,List<TableName> aTableNameList, List<HTableDescriptor> aHTableDescList) throws IOException {
	}
	@Override
	public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> aMctx) throws IOException {
	}
	@Override
	public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName, HColumnDescriptor aHColDesc) throws IOException {
	}
	@Override
	public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> aMctx, NamespaceDescriptor aNamespaceDesc) throws IOException {
	}
	@Override
	public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> aMctx, TableName aTableName, HTableDescriptor aHTableDesc) throws IOException {
	}
	@Override
	public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> aMctx, HRegionInfo aHRegInfo) throws IOException {
	}
	
	public static Date getUTCDate() {
		Calendar local=Calendar.getInstance();
	    int offset = local.getTimeZone().getOffset(local.getTimeInMillis());
	    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("GMT+0"));
	    utc.setTimeInMillis(local.getTimeInMillis());
	    utc.add(Calendar.MILLISECOND, -offset);
	    return utc.getTime();
	}
	
	//
	//  Generated to support HBase 0.98.4-hadoop2 version
	//
	
	@Override
	public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> aRctx,HRegion reg1, HRegion reg2, HRegion reg3) throws IOException {
	}
	
	@Override
	public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> aRctx, HRegion reg1, HRegion reg2, HRegion reg3) throws IOException {
	}
	
	@Override
	public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> aRctx, HRegion aReg1, HRegion aReg2) throws IOException {
	}
	
	@Override
	public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> aRctx, HRegion reg1, HRegion aReg2) throws IOException {
	}
	
	@Override
	public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> aRctx, HRegion arg1, HRegion arg2, List<Mutation> arg3) throws IOException {
	}
	
	@Override
	public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> aRctx, HRegion arg1, HRegion arg2) throws IOException {
	}
	
	
	
	
}
