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
package com.xasecure.authorization.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;

public class XaSecureAuthorizationCoprocessorBase extends BaseRegionObserver
		implements MasterObserver, RegionServerObserver, BulkLoadObserver {

	@Override
	public void preStopRegionServer(
			ObserverContext<RegionServerCoprocessorEnvironment> env)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			HRegion regionA, HRegion regionB) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> c,
			HRegion regionA, HRegion regionB, HRegion mergedRegion)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preMergeCommit(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			HRegion regionA, HRegion regionB, List<Mutation> metaEntries)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postMergeCommit(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			HRegion regionA, HRegion regionB, HRegion mergedRegion)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preRollBackMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			HRegion regionA, HRegion regionB) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postRollBackMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			HRegion regionA, HRegion regionB) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preCreateTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postCreateTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preCreateTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postCreateTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preModifyTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postModifyTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preModifyTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postModifyTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postAddColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preAddColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postAddColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preModifyColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postModifyColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preModifyColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postModifyColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preEnableTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postEnableTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preEnableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postEnableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDisableTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDisableTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDisableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDisableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo region, ServerName srcServer, ServerName destServer)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo region, ServerName srcServer, ServerName destServer)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo, boolean force) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo, boolean force) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preRegionOffline(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postRegionOffline(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HRegionInfo regionInfo) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx,
			List<RegionPlan> plans) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean preBalanceSwitch(
			ObserverContext<MasterCoprocessorEnvironment> ctx, boolean newValue)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void postBalanceSwitch(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			boolean oldValue, boolean newValue) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postStartMaster(
			ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preMasterInitialization(
			ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preCloneSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postCloneSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preRestoreSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postRestoreSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteSnapshot(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			SnapshotDescription snapshot) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preGetTableDescriptors(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			List<TableName> tableNamesList, List<HTableDescriptor> descriptors)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postGetTableDescriptors(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			List<HTableDescriptor> descriptors) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preCreateNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			NamespaceDescriptor ns) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postCreateNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			NamespaceDescriptor ns) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preDeleteNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postDeleteNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void preModifyNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			NamespaceDescriptor ns) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void postModifyNamespace(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			NamespaceDescriptor ns) throws IOException {
		// TODO Auto-generated method stub

	}

	public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
			// TODO Auto-generated method stub

	}

	public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
			// TODO Auto-generated method stub

	}
	
	public void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public void preTruncateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public void postTruncateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public void preTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public void postTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// TODO Auto-generated method stub

	}

	public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
		return endpoint;
	}

	public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, PrepareBulkLoadRequest request) throws IOException {
	}

	public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx, CleanupBulkLoadRequest request) throws IOException {
	}
}
