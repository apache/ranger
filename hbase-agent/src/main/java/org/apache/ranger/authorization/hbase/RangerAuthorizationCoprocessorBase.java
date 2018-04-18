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
import java.util.Set;

import com.google.common.net.HostAndPort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;


/**
 * This class exists only to prevent the clutter of methods that we don't intend to implement in the main co-processor class.
 * @author alal
 *
 */
public abstract class RangerAuthorizationCoprocessorBase extends BaseRegionObserver
		implements MasterObserver, RegionServerObserver, BulkLoadObserver {

	private static final Log LOG = LogFactory.getLog(RangerAuthorizationCoprocessorBase.class.getName());

	@Override
	public void preMergeCommit(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			Region regionA, Region regionB, List<Mutation> metaEntries)
			throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postMergeCommit(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			Region regionA, Region regionB, Region mergedRegion)
			throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preRollBackMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			Region regionA, Region regionB) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postRollBackMerge(
			ObserverContext<RegionServerCoprocessorEnvironment> ctx,
			Region regionA, Region regionB) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preCreateTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postCreateTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preDeleteTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postDeleteTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preModifyTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postModifyTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HTableDescriptor htd) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preAddColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postAddColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preModifyColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postModifyColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor descriptor)
			throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preDeleteColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postDeleteColumnHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preEnableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postEnableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preDisableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postDisableTableHandler(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void preMasterInitialization(
			ObserverContext<MasterCoprocessorEnvironment> ctx)
			throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
		// Not applicable.  Expected to be empty
	}

    public void preReplicateLogEntries(final ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<WALEntry> entries, CellScanner cells) throws IOException {
    }

    public void postReplicateLogEntries(final ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<WALEntry> entries, CellScanner cells) throws IOException {
    }

	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList,  List<HTableDescriptor> descriptors) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> postGetTableDescriptors(count(tableNamesList)=%s, count(descriptors)=%s)", tableNamesList == null ? 0 : tableNamesList.size(),
					descriptors == null ? 0 : descriptors.size()));
		}

	}
	
	@Override
	public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> postGetTableDescriptors(count(tableNamesList)=%s, count(descriptors)=%s, regex=%s)", tableNamesList == null ? 0 : tableNamesList.size(),
					descriptors == null ? 0 : descriptors.size(), regex));
		}
	}

    public  void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors, String regex) throws IOException {
    }

    public  void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors, String regex) throws IOException {
    }

    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
    }

    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
    }

    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
    }

    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
    }
	
	public void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void preTruncateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void postTruncateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
			// Not applicable.  Expected to be empty
	}

	public void preTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public void postTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}

	public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
		return endpoint;
	}

	@Override
	public void stop(CoprocessorEnvironment env) {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor column) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postAssign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postBalance(ObserverContext<MasterCoprocessorEnvironment> c,List<RegionPlan> aRegPlanList) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean oldValue, boolean newValue) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, byte[] col) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HColumnDescriptor descriptor) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, HTableDescriptor htd) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postMove(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
		// Not applicable.  Expected to be empty
	}
	@Override
	public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) {
		// Not applicable.  Expected to be empty
	}
	
	@Override
	public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> env, final Get get, final List<Cell> results) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
		// Not applicable.  Expected to be empty
	}
	
	@Override
	public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<HTableDescriptor> descriptors) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA, Region regionB, Region mergedRegion) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionInfo, boolean force) throws IOException {
		// Not applicable.  Expected to be empty
	}

	//TODO - add @Override directive when hbase changes to MasterObserver go mainstream
	public void postListSnapshot( ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		// Not applicable.  Expected to be empty
	}

	//TODO - add @Override directive when hbase changes to MasterObserver go mainstream
	public void preListSnapshot( ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
		// Not applicable.  Expected to be empty
	}

	@Override
	public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

	}

	@Override
	public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {

	}

	public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  public void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
  }

  public void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException{
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
