/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.hbase;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
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
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.util.Set;
import com.google.common.net.HostAndPort;

public class RangerAuthorizationCoprocessor implements MasterObserver, RegionObserver, RegionServerObserver, BulkLoadObserver, AccessControlProtos.AccessControlService.Interface, CoprocessorService, Coprocessor {

	public static final Log LOG = LogFactory.getLog(RangerAuthorizationCoprocessor.class);
	private static final String   RANGER_PLUGIN_TYPE                      = "hbase";
	private static final String   RANGER_HBASE_AUTHORIZER_IMPL_CLASSNAME  = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor";
	
	private static RangerPluginClassLoader rangerPluginClassLoader = null;
	
	private        Object 	               						impl	                 = null;
	private MasterObserver      							    implMasterObserver       = null;
	private RegionObserver       							    implRegionObserver       = null;
	private RegionServerObserver 							    implRegionServerObserver = null;
	private BulkLoadObserver                                    implBulkLoadObserver     = null;
	private AccessControlProtos.AccessControlService.Interface  implAccessControlService = null;
	private CoprocessorService   							    implCoprocessorService   = null;

	public RangerAuthorizationCoprocessor() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.RangerAuthorizationCoprocessor()");
		}

		this.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.RangerAuthorizationCoprocessor()");
		}
	}

	private void init(){
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.init()");
		}

		try {
			
			rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
			
			@SuppressWarnings("unchecked")
			Class<?> cls = Class.forName(RANGER_HBASE_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

			activatePluginClassLoader();

			impl 					 = cls.newInstance();
			implMasterObserver       = (MasterObserver)impl;
			implRegionObserver       = (RegionObserver)impl;
			implRegionServerObserver = (RegionServerObserver)impl;
			implBulkLoadObserver     = (BulkLoadObserver)impl;
			implAccessControlService = (AccessControlProtos.AccessControlService.Interface)impl;
			implCoprocessorService   = (CoprocessorService)impl;
			
		} catch (Exception e) {
			// check what need to be done
			LOG.error("Error Enabling RangerHbasePlugin", e);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.init()");
		}
	}

	@Override
	public Service getService() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.getService()");
		}
		final Service ret;
		try {
			activatePluginClassLoader();
			ret = implCoprocessorService.getService();
		} finally  {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.getService()"  + ret);
		}
		
		return ret;
	}
	

	@Override
	public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postScannerClose()");
		}
		
		try {
			activatePluginClassLoader();
			implRegionObserver.postScannerClose(c, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postScannerClose()");
		}
	}

	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
		final RegionScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postScannerOpen()");
		}
		
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postScannerOpen(c, scan, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postScannerOpen()");
		}
		
		return ret;
	}

	@Override
	public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postStartMaster()");
		}
		
		try {
			activatePluginClassLoader();	
			implMasterObserver.postStartMaster(ctx);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postStartMaster()");
		}
		
	}

	@Override
	public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> c,TableName tableName, HColumnDescriptor column) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAddColumn()");
		}
		
		try {
			activatePluginClassLoader();
			implMasterObserver.preAddColumn(c, tableName, column);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAddColumn()");
		}
	}

	@Override
	public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
		final Result ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAppend()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preAppend(c, append);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAppend()");
		}
		
		return ret;
	}

	@Override
	public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAssign()");
		}
		
		try {
			activatePluginClassLoader();
			implMasterObserver.preAssign(c, regionInfo);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAssign()");
		}
	}

	@Override
	public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c)	throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preBalance()");
		}
		
		try {
			activatePluginClassLoader();
			implMasterObserver.preBalance(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preBalance()");
		}	
	}

	@Override
	public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean newValue) 	throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preBalanceSwitch()");
		}
		
		try {
			activatePluginClassLoader();
			ret = implMasterObserver.preBalanceSwitch(c, newValue);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preBalanceSwitch()");
		}	
		
		return ret;
	}

	@Override
	public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preBulkLoadHFile()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preBulkLoadHFile(ctx, familyPaths);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preBulkLoadHFile()");
		}	

	}

	@Override
	public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCheckAndDelete()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCheckAndDelete(c, row, family, qualifier, compareOp,comparator, delete, result);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCheckAndDelete()");
		}	

		return ret;
	}

	@Override
	public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		final boolean ret;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCheckAndPut()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCheckAndPut(c, row, family, qualifier, compareOp, comparator,put, result);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCheckAndPut()");
		}	

		return ret;
	}

	@Override
	public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCloneSnapshot()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preCloneSnapshot(ctx, snapshot, hTableDescriptor);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCloneSnapshot()");
		}
	}

	@Override
	public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,boolean abortRequested) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preClose()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preClose(e, abortRequested);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preClose()");
		}
	}

	@Override
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompact()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCompact(e, store, scanner, scanType);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompact()");
		}
		
		return ret;
	}

	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> e, Store store,List<StoreFile> candidates) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompactSelection()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preCompactSelection(e, store, candidates);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompactSelection()");
		}
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCreateTable()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preCreateTable(c, desc, regions);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCreateTable()");
		}
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDelete()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preDelete(c, delete, edit, durability);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDelete()");
		}
	}

	@Override
	public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c,TableName tableName, byte[] col) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteColumn()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteColumn(c, tableName, col);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteColumn()");
		}
	}

	@Override
	public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteSnapshot()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteSnapshot(ctx, snapshot);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteSnapshot()");
		}
	}

	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteTable()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteTable(c, tableName);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteTable()");
		}
	}

	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDisableTable()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preDisableTable(c, tableName);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDisableTable()");
		}
	}

	@Override
	public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preEnableTable()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preEnableTable(c, tableName);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preEnableTable()");
		}
	}

	@Override
	public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preExists()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preExists(c, get, exists);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preExists()");
		}
		
		return ret;
	}

	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preFlush()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preFlush(e);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preFlush()");
		}
	}

	@Override
	public void preGetClosestRowBefore( ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetClosestRowBefore()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preGetClosestRowBefore(c, row, family, result);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetClosestRowBefore()");
		}
	}

	@Override
	public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c,	Increment increment) throws IOException {
		final Result ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preIncrement()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preIncrement(c, increment);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preIncrement()");
		}
		
		return ret;
	}

	@Override
	public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
		final  long ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preIncrementColumnValue()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preIncrementColumnValue(c, row, family, qualifier, amount,writeToWAL);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preIncrementColumnValue()");
		}
		
		return ret;
	}

	@Override
	public void preModifyColumn( ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preModifyColumn()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preModifyColumn(c, tableName, descriptor);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preModifyColumn()");
		}
	}

	@Override
	public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c,	TableName tableName, HTableDescriptor htd) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preModifyTable()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preModifyTable(c, tableName, htd);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preModifyTable()");
		}
	}

	@Override
	public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preMove()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preMove(c, region, srcServer, destServer);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preMove()");
		}
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preOpen()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preOpen(e);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preOpen()");
		}
	}

	@Override
	public void preRestoreSnapshot(	ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preRestoreSnapshot()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preRestoreSnapshot(ctx, snapshot, hTableDescriptor);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preRestoreSnapshot()");
		}
	}

	@Override
	public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preScannerClose()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preScannerClose(c, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preScannerClose()");
		}
	}

	@Override
	public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preScannerNext()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preScannerNext(c, s, result, limit, hasNext);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preScannerNext()");
		}
		
		return ret;
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,	RegionScanner s) throws IOException {
		final RegionScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preScannerOpen()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preScannerOpen(c, scan, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preScannerOpen()");
		}
		
		return ret;
	}

	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preShutdown()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preShutdown(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preShutdown()");
		}
	}

	@Override
	public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)	throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSnapshot()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSnapshot(ctx, snapshot, hTableDescriptor);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSnapshot()");
		}
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSplit()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preSplit(e);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSplit()");
		}
	}

	@Override
	public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preStopMaster()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preStopMaster(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preStopMaster()");
		}
	}

	@Override
	public void preStopRegionServer( ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preStopRegionServer()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preStopRegionServer(env);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preStopRegionServer()");
		}
	}

	@Override
	public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preUnassign()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preUnassign(c, regionInfo, force);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preUnassign()");
		}
	}

	@Override
	public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,	Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSetUserQuota(ctx, userName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
 	}

	@Override
	public void preSetUserQuota( ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, TableName tableName, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSetUserQuota(ctx, userName, tableName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
	}

	@Override
	public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,	String namespace, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSetUserQuota(ctx, userName, namespace, quotas);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSetUserQuota()");
		}
	}

	@Override
	public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSetTableQuota()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSetTableQuota(ctx, tableName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSetTableQuota()");
		}
	}

	@Override
	public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,	String namespace, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSetNamespaceQuota()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preSetNamespaceQuota(ctx, namespace, quotas);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSetNamespaceQuota()");
		}
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.start()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.start(env);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.start()");
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> c,	Put put, WALEdit edit, Durability durability) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.prePut()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.prePut(c, put, edit, durability);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.prePut()");
		}
	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> rEnv, Get get, List<Cell> result) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetOp()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preGetOp(rEnv, get, result);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetOp()");
		}
	}

	@Override
	public void preRegionOffline( ObserverContext<MasterCoprocessorEnvironment> c,HRegionInfo regionInfo) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preRegionOffline()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preRegionOffline(c, regionInfo);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preRegionOffline()");
		}
	}

	@Override
	public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,NamespaceDescriptor ns) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCreateNamespace()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preCreateNamespace(ctx, ns);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCreateNamespace()");
		}
	}

	@Override
	public void preDeleteNamespace( ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteNamespace()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteNamespace(ctx, namespace);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteNamespace()");
		}
	}

	@Override
	public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preModifyNamespace()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.preModifyNamespace(ctx, ns);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preModifyNamespace()");
		}
	}

	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetTableDescriptors()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.postGetTableDescriptors(ctx, tableNamesList, descriptors, regex);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetTableDescriptors()");
		}
	}

	@Override
	public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preMerge()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preMerge(ctx, regionA, regionB);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preMerge()");
		}
	}

	@Override
	public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, PrepareBulkLoadRequest request) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.prePrepareBulkLoad()");
		}
	
		try {
			activatePluginClassLoader();
			implBulkLoadObserver.prePrepareBulkLoad(ctx, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.prePrepareBulkLoad()");
		}
	}

	@Override
	public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, CleanupBulkLoadRequest request) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCleanupBulkLoad()");
		}
	
		try {
			activatePluginClassLoader();
			implBulkLoadObserver.preCleanupBulkLoad(ctx, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCleanupBulkLoad()");
		}
	}

	@Override
	public void grant(RpcController controller, GrantRequest request, RpcCallback<GrantResponse> done) {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.grant()");
		}
	
		try {
			activatePluginClassLoader();
			implAccessControlService.grant(controller, request, done);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.grant()");
		}
	}

	@Override
 	public void revoke(RpcController controller, RevokeRequest request, RpcCallback<RevokeResponse> done) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.revoke()");
		}
	
		try {
			activatePluginClassLoader();
			implAccessControlService.revoke(controller, request, done);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.revoke()");
		}
	}

	@Override
	public void checkPermissions(RpcController controller, CheckPermissionsRequest request, RpcCallback<CheckPermissionsResponse> done) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.checkPermissions()");
		}
	
		try {
			activatePluginClassLoader();
			implAccessControlService.checkPermissions(controller, request, done);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.checkPermissions()");
		}	
	}

	@Override
	public void getUserPermissions(RpcController controller, GetUserPermissionsRequest request,	RpcCallback<GetUserPermissionsResponse> done) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.getUserPermissions()");
		}
	
		try {
			activatePluginClassLoader();
			implAccessControlService.getUserPermissions(controller, request, done);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.getUserPermissions()");
		}
	}
	
	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.stop()");
		}
	
		try {
			activatePluginClassLoader();
			implMasterObserver.stop(env);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.stop()");
		}
	}

	@Override
	public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA, Region regionB, Region mergedRegion) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postMerge()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.postMerge(c,regionA, regionB, mergedRegion);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postMerge()");
		}
	}

	@Override
	public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB, List<Mutation> metaEntries) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preMergeCommit()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preMergeCommit(ctx ,regionA, regionB, metaEntries);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preMergeCommit()");
		}
	}

	@Override
	public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB, Region mergedRegion) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postMergeCommit()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.postMergeCommit(ctx ,regionA, regionB, mergedRegion);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postMergeCommit()");
		}		
	}

	@Override
	public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,Region regionA, Region regionB) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preRollBackMerge()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preRollBackMerge(ctx ,regionA, regionB);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preRollBackMerge()");
		}	
	}

	@Override
	public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA, Region regionB) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postRollBackMerge()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.postRollBackMerge(ctx ,regionA, regionB);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postRollBackMerge()");
		}	
	}

	@Override
	public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preRollWALWriterRequest()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preRollWALWriterRequest(ctx);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preRollWALWriterRequest()");
		}
	}

	@Override
	public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postRollWALWriterRequest()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.postRollWALWriterRequest(ctx);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postRollWALWriterRequest()");
		}		
	}

	@Override
	public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
		
		final ReplicationEndpoint ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCreateReplicationEndPoint()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionServerObserver.postCreateReplicationEndPoint(ctx,endpoint);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCreateReplicationEndPoint()");
		}	
		
		return ret;
	}

	@Override
	public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,	List<WALEntry> entries, CellScanner cells) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preReplicateLogEntries()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.preReplicateLogEntries(ctx, entries, cells);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preReplicateLogEntries()");
		}	
	}

	@Override
	public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<WALEntry> entries, CellScanner cells) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postReplicateLogEntries()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionServerObserver.postReplicateLogEntries(ctx, entries, cells);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postReplicateLogEntries()");
		}
	}

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postOpen()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postOpen(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postOpen()");
		}
	}

	@Override
	public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> c) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postLogReplay()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postLogReplay(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postLogReplay()");
		}
	}

	@Override
	public InternalScanner preFlushScannerOpen( ObserverContext<RegionCoprocessorEnvironment> c, Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
		
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preFlushScannerOpen()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preFlushScannerOpen(c, store, memstoreScanner, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preFlushScannerOpen()");
		}
		
		return ret;
	}

	@Override
	public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner) throws IOException {
		
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preFlush()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preFlush(c, store, scanner);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preFlush()");
		}
		
		return ret;
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postFlush()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postFlush(c);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postFlush()");
		}
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postFlush()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postFlush(c, store, resultFile);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postFlush()");
		}
	}

	@Override
	public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompactSelection()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preCompactSelection(c, store, candidates, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompactSelection()");
		}
	}

	@Override
	public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected, CompactionRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCompactSelection()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postCompactSelection(c, store, selected, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCompactSelection()");
		}
	}

	@Override
	public void postCompactSelection( ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCompactSelection()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postCompactSelection(c, store, selected);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCompactSelection()");
		}
	}

	@Override
	public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,	InternalScanner scanner, ScanType scanType,	CompactionRequest request) throws IOException {
		
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompact()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCompact(c, store, scanner, scanType, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompact()");
		}
		
		return ret;
	}

	@Override
	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
													long earliestPutTs, InternalScanner s, CompactionRequest request) throws IOException {
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompactScannerOpen()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s,request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompactScannerOpen()");
		}
		
		return ret;
	}

	@Override
	public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
													long earliestPutTs, InternalScanner s) throws IOException {
		final InternalScanner ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCompactScannerOpen()");
		}
	
		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCompactScannerOpen()");
		}
		
		return ret;
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, CompactionRequest request) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCompact()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postCompact(c, store, resultFile, request);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCompact()");
		}
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCompact()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postCompact(c, store, resultFile);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCompact()");
		}
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSplit()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preSplit(c, splitRow);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSplit()");
		}
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c, Region l, Region r) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSplit()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.postSplit(c, l, r);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSplit()");
		}
	}

	@Override
	public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey, List<Mutation> metaEntries) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSplitBeforePONR()");
		}
	
		try {
			activatePluginClassLoader();
			implRegionObserver.preSplitBeforePONR(ctx, splitKey, metaEntries);
		} finally {
			deactivatePluginClassLoader();
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSplitBeforePONR()");
		}
	}

	@Override
	public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preSplitAfterPONR()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.preSplitAfterPONR(ctx);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preSplitAfterPONR()");
		}
	}

	@Override
	public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)	throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preRollBackSplit()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.preRollBackSplit(ctx);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preRollBackSplit()");
		}
	}

	@Override
	public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postRollBackSplit()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postRollBackSplit(ctx);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postRollBackSplit()");
		}
	}

	@Override
	public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCompleteSplit()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postCompleteSplit(ctx);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCompleteSplit()");
		}
	}

	@Override
	public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postClose()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postClose(c,abortRequested);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postClose()");
		}
	}

	@Override
	public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, Result result) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetClosestRowBefore()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postGetClosestRowBefore(c, row, family, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetClosestRowBefore()");
		}
	}

	@Override
	public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetOp()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postGetOp(c, get, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetOp()");
		}
	}

	@Override
	public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
		
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postExists()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postExists(c, get, exists);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postExists()");
		}
		
		return ret;
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postPut()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postPut(c, put, edit, durability);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postPut()");
		}
	}

	@Override
	public void prePrepareTimeStampForDeleteVersion( ObserverContext<RegionCoprocessorEnvironment> c, Mutation mutation, Cell cell, byte[] byteNow, Get get) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.prePrepareTimeStampForDeleteVersion()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.prePrepareTimeStampForDeleteVersion(c, mutation, cell, byteNow, get);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.prePrepareTimeStampForDeleteVersion()");
		}
	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c,	Delete delete, WALEdit edit, Durability durability)	throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDelete()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postDelete(c, delete, edit, durability);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDelete()");
		}
	}

	@Override
	public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,	MiniBatchOperationInProgress<Mutation> miniBatchOp)	throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preBatchMutate()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.preBatchMutate(c, miniBatchOp);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preBatchMutate()");
		}
	}

	@Override
	public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postBatchMutate()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postBatchMutate(c, miniBatchOp);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postBatchMutate()");
		}
	}

	@Override
	public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,	Operation operation) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postStartRegionOperation()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postStartRegionOperation(ctx, operation);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postStartRegionOperation()");
		}
	}

	@Override
	public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, Operation operation) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCloseRegionOperation()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postCloseRegionOperation(ctx, operation);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCloseRegionOperation()");
		}
	}

	@Override
	public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx,	MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postBatchMutateIndispensably()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postBatchMutateIndispensably(ctx, miniBatchOp, success);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postBatchMutateIndispensably()");
		}
	}

	@Override
	public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
												ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCheckAndPutAfterRowLock()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCheckAndPutAfterRowLock(c, row, family, qualifier, compareOp, comparator, put, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCheckAndPutAfterRowLock()");
		}
		return ret;
	}

	@Override
	public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
									ByteArrayComparable comparator, Put put, boolean result) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCheckAndPut()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postCheckAndPut(c, row, family, qualifier, compareOp, comparator, put, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCheckAndPut()");
		}
		return ret;
	}

	@Override
	public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
													ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCheckAndDeleteAfterRowLock()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, compareOp, comparator, delete, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCheckAndDeleteAfterRowLock()");
		}
		return ret;
	}

	@Override
	public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,byte[] family, byte[] qualifier, CompareOp compareOp,
										ByteArrayComparable comparator, Delete delete, boolean result)	throws IOException {
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCheckAndDelete()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postCheckAndDelete(c, row, family, qualifier, compareOp, comparator, delete, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCheckAndDelete()");
		}
		return ret;
	}

	@Override
	public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL, long result) throws IOException {
		final long ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postIncrementColumnValue()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postIncrementColumnValue(c, row, family, qualifier, amount, writeToWAL, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postIncrementColumnValue()");
		}
		return ret;
	}

	@Override
	public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Append append)	throws IOException {
		final Result ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAppendAfterRowLock()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preAppendAfterRowLock(c, append);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAppendAfterRowLock()");
		}
		return ret;
	}

	@Override
	public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result) throws IOException {
		final Result ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postAppend()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postAppend(c, append, result);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postAppend()");
		}
		
		return ret;
	}

	@Override
	public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
		final Result ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preIncrementAfterRowLock()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preIncrementAfterRowLock(c, increment);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preIncrementAfterRowLock()");
		}
		
		return ret;
	}

	@Override
	public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment, Result result) throws IOException {
		final Result ret;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postIncrement()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postIncrement(c, increment, result );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postIncrement()");
		}

		return ret;
	}

	@Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)	throws IOException {
		final KeyValueScanner ret;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preStoreScannerOpen()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preStoreScannerOpen(c, store, scan, targetCols, s);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preStoreScannerOpen()");
		}

		return ret;
	}

	@Override
	public boolean postScannerNext(	ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,	List<Result> result, int limit, boolean hasNext) throws IOException {
		final boolean ret;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postScannerNext()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postScannerNext(c, s, result, limit, hasNext);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postScannerNext()");
		}

		return ret;
	}

	@Override
	public boolean postScannerFilterRow( ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, byte[] currentRow, int offset, short length, boolean hasMore) throws IOException {
		
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postScannerFilterRow()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postScannerFilterRow(c, s, currentRow, offset, length, hasMore);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postScannerFilterRow()");
		}

		return ret;
	}

	@Override
	public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preWALRestore()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.preWALRestore(ctx, info, logKey, logEdit);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preWALRestore()");
		}
	}

	@Override
	public void postWALRestore( ObserverContext<? extends RegionCoprocessorEnvironment> ctx, HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postWALRestore()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postWALRestore(ctx, info, logKey, logEdit);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postWALRestore()");
		}
	}

	@Override
	public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,	List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
		
		final boolean ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postBulkLoadHFile()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postBulkLoadHFile(ctx, familyPaths, hasLoaded);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postBulkLoadHFile()");
		}

		return ret;
	}

	@Override
	public Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size,
											CacheConfig cacheConf, Reference r, Reader reader) throws IOException {
		final Reader ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preStoreFileReaderOpen()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preStoreFileReaderOpen()");
		}

		return ret;
	}

	@Override
	public Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs,	Path p, FSDataInputStreamWrapper in, long size,
											CacheConfig cacheConf, Reference r, Reader reader) throws IOException {
		final Reader ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postStoreFileReaderOpen()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postStoreFileReaderOpen()");
		}

		return ret;
	}

	@Override
	public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx, MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
		final Cell ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postMutationBeforeWAL()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postMutationBeforeWAL(ctx, opType, mutation, oldCell, newCell);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postMutationBeforeWAL()");
		}

		return ret;
	}

	@Override
	public DeleteTracker postInstantiateDeleteTracker( ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) throws IOException {
		final DeleteTracker ret;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postInstantiateDeleteTracker()");
		}

		try {
			activatePluginClassLoader();
			ret = implRegionObserver.postInstantiateDeleteTracker(ctx, delTracker);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postInstantiateDeleteTracker()");
		}

		return ret;
	}

	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCreateTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postCreateTable(ctx, desc, regions);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCreateTable()");
		}
	}

	@Override
	public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preCreateTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preCreateTableHandler(ctx, desc, regions);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preCreateTableHandler()");
		}
	}

	@Override
	public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCreateTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postCreateTableHandler(ctx, desc, regions);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCreateTableHandler()");
		}
	}

	@Override
	public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteTable(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteTable()");
		}
	}

	@Override
	public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteTableHandler()");
		}
	}

	@Override
	public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteTableHandler()");
		}
	}

	@Override
	public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,	TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preTruncateTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preTruncateTable(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preTruncateTable()");
		}
	}

	@Override
	public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postTruncateTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postTruncateTable(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postTruncateTable()");
		}
	}

	@Override
	public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preTruncateTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preTruncateTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preTruncateTableHandler()");
		}
	}

	@Override
	public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postTruncateTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postTruncateTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postTruncateTableHandler()");
		}
	}

	@Override
	public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HTableDescriptor htd) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postModifyTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postModifyTable(ctx, tableName, htd);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postModifyTable()");
		}
	}

	@Override
	public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,TableName tableName, HTableDescriptor htd) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preModifyTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preModifyTableHandler(ctx, tableName, htd);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preModifyTableHandler()");
		}
	}

	@Override
	public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HTableDescriptor htd) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postModifyTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postModifyTableHandler(ctx, tableName, htd);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postModifyTableHandler()");
		}
	}

	@Override
	public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HColumnDescriptor column) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postAddColumn()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postAddColumn(ctx, tableName, column);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postAddColumn()");
		}
	}

	@Override
	public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,	TableName tableName, HColumnDescriptor column) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAddColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preAddColumnHandler(ctx, tableName, column);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAddColumnHandler()");
		}
	}

	@Override
	public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,	TableName tableName, HColumnDescriptor column) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postAddColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postAddColumnHandler(ctx, tableName, column);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postAddColumnHandler()");
		}
	}

	@Override
	public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,	TableName tableName, HColumnDescriptor descriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postModifyColumn()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postModifyColumn(ctx, tableName, descriptor);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postModifyColumn()");
		}
	}

	@Override
	public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preModifyColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preModifyColumnHandler(ctx, tableName, descriptor);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preModifyColumnHandler()");
		}
	}

	@Override
	public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postModifyColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postModifyColumnHandler(ctx, tableName, descriptor);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postModifyColumnHandler()");
		}
	}

	@Override
	public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,	TableName tableName, byte[] c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteColumn()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteColumn(ctx, tableName, c);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteColumn()");
		}
	}

	@Override
	public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDeleteColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preDeleteColumnHandler(ctx, tableName, c);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDeleteColumnHandler()");
		}
	}

	@Override
	public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] c) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteColumnHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteColumnHandler(ctx, tableName, c);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteColumnHandler()");
		}
	}

	@Override
	public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postEnableTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postEnableTable(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postEnableTable()");
		}
	}

	@Override
	public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preEnableTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preEnableTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preEnableTableHandler()");
		}
	}

	@Override
	public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postEnableTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postEnableTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postEnableTableHandler()");
		}
	}

	@Override
	public void postDisableTable( ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDisableTable()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDisableTable(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDisableTable()");
		}
	}

	@Override
	public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preDisableTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preDisableTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preDisableTableHandler()");
		}
	}

	@Override
	public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDisableTableHandler()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDisableTableHandler(ctx, tableName);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDisableTableHandler()");
		}
	}

	@Override
	public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx,	HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postMove()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postMove(ctx, region, srcServer, destServer);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postMove()");
		}
	}

	@Override
	public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext, ProcedureExecutor<MasterProcedureEnv> procEnv, long procId) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preAbortProcedure()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preAbortProcedure(observerContext, procEnv, procId);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preAbortProcedure()");
		}
	}

	@Override
	public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postAbortProcedure()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postAbortProcedure(observerContext);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postAbortProcedure()");
		}
	}

	@Override
	public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preListProcedures()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preListProcedures(observerContext);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preListProcedures()");
		}
	}

	@Override
	public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext, List<ProcedureInfo> procInfoList) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postListProcedures()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postListProcedures(observerContext, procInfoList);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postListProcedures()");
		}
	}

	@Override
	public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postAssign()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postAssign(ctx, regionInfo);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postAssign()");
		}
	}

	@Override
	public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,	HRegionInfo regionInfo, boolean force) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postUnassign()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postUnassign(ctx, regionInfo, force);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postUnassign()");
		}
	}

	@Override
	public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postRegionOffline()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postRegionOffline(ctx, regionInfo);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postRegionOffline()");
		}
	}

	@Override
	public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postBalance()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postBalance(ctx, plans);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postBalance()");
		}
	}

	@Override
	public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean oldValue, boolean newValue) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postBalanceSwitch()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postBalanceSwitch(ctx, oldValue, newValue);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postBalanceSwitch()");
		}
	}

	@Override
	public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preMasterInitialization()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preMasterInitialization(ctx);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preMasterInitialization()");
		}
	}

	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,	SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSnapshot(ctx, snapshot, hTableDescriptor );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSnapshot()");
		}
	}

	@Override
	public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preListSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preListSnapshot(ctx, snapshot);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preListSnapshot()");
		}
	}

	@Override
	public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,	SnapshotDescription snapshot) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postListSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postListSnapshot(ctx, snapshot);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postListSnapshot()");
		}
	}

	@Override
	public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCloneSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postCloneSnapshot(ctx, snapshot, hTableDescriptor);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCloneSnapshot()");
		}
	}

	@Override
	public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,	SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postRestoreSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postRestoreSnapshot(ctx, snapshot, hTableDescriptor);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postRestoreSnapshot()");
		}
	}

	@Override
	public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,SnapshotDescription snapshot) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteSnapshot()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteSnapshot(ctx, snapshot);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteSnapshot()");
		}
	}

	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<HTableDescriptor> descriptors) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetTableDescriptors()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preGetTableDescriptors(ctx, tableNamesList,descriptors );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetTableDescriptors()");
		}
	}

	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetTableDescriptors()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postGetTableDescriptors(ctx, descriptors );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetTableDescriptors()");
		}
	}

	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetTableDescriptors()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preGetTableDescriptors(ctx, tableNamesList, descriptors, regex );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetTableDescriptors()");
		}
	}

	@Override
	public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,	List<HTableDescriptor> descriptors, String regex) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetTableNames()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preGetTableNames(ctx, descriptors, regex );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetTableNames()");
		}
	}

	@Override
	public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors, String regex) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetTableNames()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postGetTableNames(ctx, descriptors, regex );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetTableNames()");
		}
	}

	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,	NamespaceDescriptor ns) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postCreateNamespace()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postCreateNamespace(ctx, ns );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postCreateNamespace()");
		}
	}

	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postDeleteNamespace()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postDeleteNamespace(ctx, namespace );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postDeleteNamespace()");
		}
	}

	@Override
	public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,	NamespaceDescriptor ns) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postModifyNamespace()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postModifyNamespace(ctx, ns );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postModifyNamespace()");
		}
	}

	@Override
	public void preGetNamespaceDescriptor( ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preGetNamespaceDescriptor()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preGetNamespaceDescriptor(ctx, namespace );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preGetNamespaceDescriptor()");
		}
	}

	@Override
	public void postGetNamespaceDescriptor( ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postGetNamespaceDescriptor()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postGetNamespaceDescriptor(ctx, ns );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postGetNamespaceDescriptor()");
		}
	}

	@Override
	public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preListNamespaceDescriptors()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preListNamespaceDescriptors(ctx, descriptors );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preListNamespaceDescriptors()");
		}
	}

	@Override
	public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,	List<NamespaceDescriptor> descriptors) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postListNamespaceDescriptors()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postListNamespaceDescriptors(ctx, descriptors );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postListNamespaceDescriptors()");
		}
	}

	@Override
	public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preTableFlush()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.preTableFlush(ctx, tableName );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preTableFlush()");
		}
	}
	
	@Override
	public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postTableFlush()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postTableFlush(ctx, tableName );
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postTableFlush()");
		}
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSetUserQuota()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSetUserQuota(ctx, userName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSetUserQuota()");
		}
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,TableName tableName, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSetUserQuota()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSetUserQuota(ctx, userName, tableName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSetUserQuota()");
		}
	}

	@Override
	public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, String namespace, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSetUserQuota()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSetUserQuota(ctx, userName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSetUserQuota()");
		}
	}

	@Override
	public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSetTableQuota()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSetTableQuota(ctx, tableName, quotas);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSetTableQuota()");
		}
	}

	@Override
	public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,String namespace, Quotas quotas) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postSetNamespaceQuota()");
		}

		try {
			activatePluginClassLoader();
			implMasterObserver.postSetNamespaceQuota(ctx, namespace, quotas);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postSetNamespaceQuota()");
		}
	}

	@Override
	public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx,HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.preWALRestore()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.preWALRestore(ctx, info, logKey, logEdit);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.preWALRestore()");
		}
	}

	@Override
	public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAuthorizationCoprocessor.postWALRestore()");
		}

		try {
			activatePluginClassLoader();
			implRegionObserver.postWALRestore(ctx, info, logKey, logEdit);
		} finally {
			deactivatePluginClassLoader();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAuthorizationCoprocessor.postWALRestore()");
		}
	}
	
	private void activatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.activate();
		}
	}

	private void deactivatePluginClassLoader() {
		if(rangerPluginClassLoader != null) {
			rangerPluginClassLoader.deactivate();
		}
	}



  // TODO : need override annotations for all of the following methods

  public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort> servers, String targetGroup) throws IOException {}
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort> servers, String targetGroup) throws IOException {}
  public void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}
  public void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}
  public void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
  public void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
  public void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName) throws IOException {}
  public void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName, boolean balancerRan) throws IOException {}
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}
  public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}

  public void postDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> arg0, HRegionInfo arg1,
                                HRegionInfo arg2)
      throws IOException {
      // TODO Auto-generated method stub

  }

  public void postSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> arg0, boolean arg1,
                                         MasterSwitchType arg2)
      throws IOException {
      // TODO Auto-generated method stub

  }
  public void preDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> arg0, HRegionInfo arg1,
                               HRegionInfo arg2)
      throws IOException {
      // TODO Auto-generated method stub

  }

  public boolean preSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> arg0, boolean arg1,
                                           MasterSwitchType arg2)
      throws IOException {
      // TODO Auto-generated method stub
      return false;
  }
}
