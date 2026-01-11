/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.ranger.plugin.classloader.PluginClassLoaderActivator;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangerAuthorizationCoprocessor implements RegionCoprocessor, MasterCoprocessor, RegionServerCoprocessor, MasterObserver, RegionObserver, RegionServerObserver, EndpointObserver, BulkLoadObserver, AccessControlProtos.AccessControlService.Interface {
    public static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationCoprocessor.class);

    private static final String RANGER_PLUGIN_TYPE                     = "hbase";
    private static final String RANGER_HBASE_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.hbase.RangerAuthorizationCoprocessor";

    private RangerPluginClassLoader                            pluginClassLoader;
    private MasterObserver                                     implMasterObserver;
    private RegionObserver                                     implRegionObserver;
    private RegionServerObserver                               implRegionServerObserver;
    private BulkLoadObserver                                   implBulkLoadObserver;
    private AccessControlProtos.AccessControlService.Interface implAccessControlService;
    private MasterCoprocessor                                  implMasterCoprocessor;
    private RegionCoprocessor                                  implRegionCoprocessor;
    private RegionServerCoprocessor                            implRegionServerCoporcessor;
    // private EndpointObserver                                   implEndpointObserver;

    public RangerAuthorizationCoprocessor() {
        LOG.debug("==> RangerAuthorizationCoprocessor.RangerAuthorizationCoprocessor()");

        this.init();

        LOG.debug("<== RangerAuthorizationCoprocessor.RangerAuthorizationCoprocessor()");
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<EndpointObserver> getEndpointObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<BulkLoadObserver> getBulkLoadObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "start")) {
            if (env instanceof MasterCoprocessorEnvironment) {
                implMasterCoprocessor.start(env);
            } else if (env instanceof RegionServerCoprocessorEnvironment) {
                implRegionServerCoporcessor.start(env);
            } else if (env instanceof RegionCoprocessorEnvironment) {
                implRegionCoprocessor.start(env);
            }
        }
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(AccessControlProtos.AccessControlService.newReflectiveService(this));
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> c, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCreateTable")) {
            implMasterObserver.preCreateTable(c, desc, regions);
        }
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCreateTable")) {
            implMasterObserver.postCreateTable(ctx, desc, regions);
        }
    }

    @Override
    public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCreateTableAction")) {
            implMasterObserver.preCreateTableAction(ctx, desc, regions);
        }
    }

    @Override
    public void postCompletedCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedCreateTableAction")) {
            implMasterObserver.postCompletedCreateTableAction(ctx, desc, regions);
        }
    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDeleteTable")) {
            implMasterObserver.preDeleteTable(c, tableName);
        }
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postDeleteTable")) {
            implMasterObserver.postDeleteTable(ctx, tableName);
        }
    }

    @Override
    public void preDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDeleteTableAction")) {
            implMasterObserver.preDeleteTableAction(ctx, tableName);
        }
    }

    @Override
    public void postCompletedDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedDeleteTableAction")) {
            implMasterObserver.postCompletedDeleteTableAction(ctx, tableName);
        }
    }

    @Override
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preTruncateTable")) {
            implMasterObserver.preTruncateTable(ctx, tableName);
        }
    }

    @Override
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postTruncateTable")) {
            implMasterObserver.postTruncateTable(ctx, tableName);
        }
    }

    @Override
    public void preTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preTruncateTableAction")) {
            implMasterObserver.preTruncateTableAction(ctx, tableName);
        }
    }

    @Override
    public void postCompletedTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedTruncateTableAction")) {
            implMasterObserver.postCompletedTruncateTableAction(ctx, tableName);
        }
    }

    @Override
    public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName, TableDescriptor htd) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preModifyTable")) {
            implMasterObserver.preModifyTable(c, tableName, htd);
        }
    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, TableDescriptor htd) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postModifyTable")) {
            implMasterObserver.postModifyTable(ctx, tableName, htd);
        }
    }

    @Override
    public void preModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, TableDescriptor htd) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preModifyTableAction")) {
            implMasterObserver.preModifyTableAction(ctx, tableName, htd);
        }
    }

    @Override
    public void postCompletedModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, TableDescriptor htd) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedModifyTableAction")) {
            implMasterObserver.postCompletedModifyTableAction(ctx, tableName, htd);
        }
    }

    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preEnableTable")) {
            implMasterObserver.preEnableTable(c, tableName);
        }
    }

    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postEnableTable")) {
            implMasterObserver.postEnableTable(ctx, tableName);
        }
    }

    @Override
    public void preEnableTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preEnableTableAction")) {
            implMasterObserver.preEnableTableAction(ctx, tableName);
        }
    }

    @Override
    public void postCompletedEnableTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedEnableTableAction")) {
            implMasterObserver.postCompletedEnableTableAction(ctx, tableName);
        }
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDisableTable")) {
            implMasterObserver.preDisableTable(c, tableName);
        }
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postDisableTable")) {
            implMasterObserver.postDisableTable(ctx, tableName);
        }
    }

    @Override
    public void preDisableTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDisableTableAction")) {
            implMasterObserver.preDisableTableAction(ctx, tableName);
        }
    }

    @Override
    public void postCompletedDisableTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompletedDisableTableAction")) {
            implMasterObserver.postCompletedDisableTableAction(ctx, tableName);
        }
    }

    @Override
    public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext, long procId) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preAbortProcedure")) {
            implMasterObserver.preAbortProcedure(observerContext, procId);
        }
    }

    @Override
    public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postAbortProcedure")) {
            implMasterObserver.postAbortProcedure(observerContext);
        }
    }

    @Override
    public void preGetProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preGetProcedures")) {
            implMasterObserver.preGetProcedures(observerContext);
        }
    }

    @Override
    public void postGetProcedures(ObserverContext<MasterCoprocessorEnvironment> observerContext) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postGetProcedures")) {
            implMasterObserver.postGetProcedures(observerContext);
        }
    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preMove")) {
            implMasterObserver.preMove(c, region, srcServer, destServer);
        }
    }

    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo region, ServerName srcServer, ServerName destServer) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postMove")) {
            implMasterObserver.postMove(ctx, region, srcServer, destServer);
        }
    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preAssign")) {
            implMasterObserver.preAssign(c, regionInfo);
        }
    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo regionInfo) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postAssign")) {
            implMasterObserver.postAssign(ctx, regionInfo);
        }
    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo, boolean force) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preUnassign")) {
            implMasterObserver.preUnassign(c, regionInfo, force);
        }
    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo regionInfo, boolean force) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postUnassign")) {
            implMasterObserver.postUnassign(ctx, regionInfo, force);
        }
    }

    @Override
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo regionInfo) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preRegionOffline")) {
            implMasterObserver.preRegionOffline(c, regionInfo);
        }
    }

    @Override
    public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo regionInfo) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postRegionOffline")) {
            implMasterObserver.postRegionOffline(ctx, regionInfo);
        }
    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c, BalanceRequest request) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preBalance")) {
            implMasterObserver.preBalance(c, request);
        }
    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, BalanceRequest request, List<RegionPlan> plans) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postBalance")) {
            implMasterObserver.postBalance(ctx, request, plans);
        }
    }

    public void preSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> arg0, boolean arg1, MasterSwitchType arg2) throws IOException {
        // TODO Auto-generated method stub
    }

    public void postSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> arg0, boolean arg1, MasterSwitchType arg2) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> c, boolean newValue) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preBalanceSwitch")) {
            implMasterObserver.preBalanceSwitch(c, newValue);
        }
    }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean oldValue, boolean newValue) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postBalanceSwitch")) {
            implMasterObserver.postBalanceSwitch(ctx, oldValue, newValue);
        }
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preShutdown")) {
            implMasterObserver.preShutdown(c);
        }
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> c) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preStopMaster")) {
            implMasterObserver.preStopMaster(c);
        }
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postStartMaster")) {
            implMasterObserver.postStartMaster(ctx);
        }
    }

    @Override
    public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preMasterInitialization")) {
            implMasterObserver.preMasterInitialization(ctx);
        }
    }

    @Override
    public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSnapshot")) {
            implMasterObserver.preSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSnapshot")) {
            implMasterObserver.postSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preListSnapshot")) {
            implMasterObserver.preListSnapshot(ctx, snapshot);
        }
    }

    @Override
    public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postListSnapshot")) {
            implMasterObserver.postListSnapshot(ctx, snapshot);
        }
    }

    @Override
    public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCloneSnapshot")) {
            implMasterObserver.preCloneSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCloneSnapshot")) {
            implMasterObserver.postCloneSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preRestoreSnapshot")) {
            implMasterObserver.preRestoreSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot, TableDescriptor hTableDescriptor) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postRestoreSnapshot")) {
            implMasterObserver.postRestoreSnapshot(ctx, snapshot, hTableDescriptor);
        }
    }

    @Override
    public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDeleteSnapshot")) {
            implMasterObserver.preDeleteSnapshot(ctx, snapshot);
        }
    }

    @Override
    public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription snapshot) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postDeleteSnapshot")) {
            implMasterObserver.postDeleteSnapshot(ctx, snapshot);
        }
    }

    @Override
    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<TableDescriptor> descriptors, String regex) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preGetTableDescriptors")) {
            implMasterObserver.preGetTableDescriptors(ctx, tableNamesList, descriptors, regex);
        }
    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<TableDescriptor> descriptors, String regex) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postGetTableDescriptors")) {
            implMasterObserver.postGetTableDescriptors(ctx, tableNamesList, descriptors, regex);
        }
    }

    @Override
    public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableDescriptor> descriptors, String regex) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preGetTableNames")) {
            implMasterObserver.preGetTableNames(ctx, descriptors, regex);
        }
    }

    @Override
    public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableDescriptor> descriptors, String regex) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postGetTableNames")) {
            implMasterObserver.postGetTableNames(ctx, descriptors, regex);
        }
    }

    @Override
    public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCreateNamespace")) {
            implMasterObserver.preCreateNamespace(ctx, ns);
        }
    }

    @Override
    public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCreateNamespace")) {
            implMasterObserver.postCreateNamespace(ctx, ns);
        }
    }

    @Override
    public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDeleteNamespace")) {
            implMasterObserver.preDeleteNamespace(ctx, namespace);
        }
    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postDeleteNamespace")) {
            implMasterObserver.postDeleteNamespace(ctx, namespace);
        }
    }

    @Override
    public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preModifyNamespace")) {
            implMasterObserver.preModifyNamespace(ctx, ns);
        }
    }

    @Override
    public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postModifyNamespace")) {
            implMasterObserver.postModifyNamespace(ctx, ns);
        }
    }

    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preGetNamespaceDescriptor")) {
            implMasterObserver.preGetNamespaceDescriptor(ctx, namespace);
        }
    }

    @Override
    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postGetNamespaceDescriptor")) {
            implMasterObserver.postGetNamespaceDescriptor(ctx, ns);
        }
    }

    @Override
    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preListNamespaceDescriptors")) {
            implMasterObserver.preListNamespaceDescriptors(ctx, descriptors);
        }
    }

    @Override
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<NamespaceDescriptor> descriptors) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postListNamespaceDescriptors")) {
            implMasterObserver.postListNamespaceDescriptors(ctx, descriptors);
        }
    }

    @Override
    public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preTableFlush")) {
            implMasterObserver.preTableFlush(ctx, tableName);
        }
    }

    @Override
    public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postTableFlush")) {
            implMasterObserver.postTableFlush(ctx, tableName);
        }
    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSetUserQuota")) {
            implMasterObserver.preSetUserQuota(ctx, userName, quotas);
        }
    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSetUserQuota")) {
            implMasterObserver.postSetUserQuota(ctx, userName, quotas);
        }
    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, TableName tableName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSetUserQuota")) {
            implMasterObserver.preSetUserQuota(ctx, userName, tableName, quotas);
        }
    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, TableName tableName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSetUserQuota")) {
            implMasterObserver.postSetUserQuota(ctx, userName, tableName, quotas);
        }
    }

    @Override
    public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, String namespace, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSetUserQuota")) {
            implMasterObserver.preSetUserQuota(ctx, userName, namespace, quotas);
        }
    }

    @Override
    public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName, String namespace, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSetUserQuota")) {
            implMasterObserver.postSetUserQuota(ctx, userName, quotas);
        }
    }

    @Override
    public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSetTableQuota")) {
            implMasterObserver.preSetTableQuota(ctx, tableName, quotas);
        }
    }

    @Override
    public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSetTableQuota")) {
            implMasterObserver.postSetTableQuota(ctx, tableName, quotas);
        }
    }

    @Override
    public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preSetNamespaceQuota")) {
            implMasterObserver.preSetNamespaceQuota(ctx, namespace, quotas);
        }
    }

    @Override
    public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace, GlobalQuotaSettings quotas) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postSetNamespaceQuota")) {
            implMasterObserver.postSetNamespaceQuota(ctx, namespace, quotas);
        }
    }

    public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<Address> servers, String targetGroup) throws IOException {}

    public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<Address> servers, String targetGroup) throws IOException {}

    public void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}

    public void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName> tables, String targetGroup) throws IOException {}

    public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}

    public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}

    public void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}

    public void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name) throws IOException {}

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preOpen")) {
            implRegionObserver.preOpen(e);
        }
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postOpen")) {
            implRegionObserver.postOpen(c);
        }
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e, FlushLifeCycleTracker tracker) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preFlush")) {
            implRegionObserver.preFlush(e, tracker);
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preFlush")) {
            return implRegionObserver.preFlush(c, store, scanner, tracker);
        }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, FlushLifeCycleTracker tracker) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postFlush")) {
            implRegionObserver.postFlush(c, tracker);
        }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, FlushLifeCycleTracker tracker) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postFlush")) {
            implRegionObserver.postFlush(c, store, resultFile, tracker);
        }
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends StoreFile> candidates, CompactionLifeCycleTracker request) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCompactSelection")) {
            implRegionObserver.preCompactSelection(c, store, candidates, request);
        }
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends StoreFile> selected, CompactionLifeCycleTracker tracker, CompactionRequest request) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompactSelection")) {
            implRegionObserver.postCompactSelection(c, store, selected, tracker, request);
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner,
            ScanType scanType, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCompact")) {
            return implRegionObserver.preCompact(c, store, scanner, scanType, tracker, request);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile,
            CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCompact")) {
            implRegionObserver.postCompact(c, store, resultFile, tracker, request);
        }
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preClose")) {
            implRegionObserver.preClose(e, abortRequested);
        }
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postClose")) {
            implRegionObserver.postClose(c, abortRequested);
        }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> rEnv, Get get, List<Cell> result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preGetOp")) {
            implRegionObserver.preGetOp(rEnv, get, result);
        }
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postGetOp")) {
            implRegionObserver.postGetOp(c, get, result);
        }
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preExists")) {
            return implRegionObserver.preExists(c, get, exists);
        }
    }

    @Override
    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postExists")) {
            return implRegionObserver.postExists(c, get, exists);
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "prePut")) {
            implRegionObserver.prePut(c, put, edit, durability);
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postPut")) {
            implRegionObserver.postPut(c, put, edit, durability);
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preDelete")) {
            implRegionObserver.preDelete(c, delete, edit, durability);
        }
    }

    @Override
    public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> c, Mutation mutation, Cell cell, byte[] byteNow, Get get) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "prePrepareTimeStampForDeleteVersion")) {
            implRegionObserver.prePrepareTimeStampForDeleteVersion(c, mutation, cell, byteNow, get);
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postDelete")) {
            implRegionObserver.postDelete(c, delete, edit, durability);
        }
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preBatchMutate")) {
            implRegionObserver.preBatchMutate(c, miniBatchOp);
        }
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postBatchMutate")) {
            implRegionObserver.postBatchMutate(c, miniBatchOp);
        }
    }

    @Override
    public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, Operation operation) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postStartRegionOperation")) {
            implRegionObserver.postStartRegionOperation(ctx, operation);
        }
    }

    @Override
    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, Operation operation) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCloseRegionOperation")) {
            implRegionObserver.postCloseRegionOperation(ctx, operation);
        }
    }

    @Override
    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx, MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postBatchMutateIndispensably")) {
            implRegionObserver.postBatchMutateIndispensably(ctx, miniBatchOp, success);
        }
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCheckAndPut")) {
            return implRegionObserver.preCheckAndPut(c, row, family, qualifier, compareOp, comparator, put, result);
        }
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCheckAndPutAfterRowLock")) {
            return implRegionObserver.preCheckAndPutAfterRowLock(c, row, family, qualifier, compareOp, comparator, put, result);
        }
    }

    @Override
    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCheckAndPut")) {
            return implRegionObserver.postCheckAndPut(c, row, family, qualifier, compareOp, comparator, put, result);
        }
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCheckAndDelete")) {
            return implRegionObserver.preCheckAndDelete(c, row, family, qualifier, compareOp, comparator, delete, result);
        }
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCheckAndDeleteAfterRowLock")) {
            return implRegionObserver.preCheckAndDeleteAfterRowLock(c, row, family, qualifier, compareOp, comparator, delete, result);
        }
    }

    @Override
    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
            ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCheckAndDelete")) {
            return implRegionObserver.postCheckAndDelete(c, row, family, qualifier, compareOp, comparator, delete, result);
        }
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preAppend")) {
            return implRegionObserver.preAppend(c, append);
        }
    }

    @Override
    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preAppendAfterRowLock")) {
            return implRegionObserver.preAppendAfterRowLock(c, append);
        }
    }

    @Override
    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postAppend")) {
            return implRegionObserver.postAppend(c, append, result);
        }
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preIncrement")) {
            return implRegionObserver.preIncrement(c, increment);
        }
    }

    @Override
    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preIncrementAfterRowLock")) {
            return implRegionObserver.preIncrementAfterRowLock(c, increment);
        }
    }

    @Override
    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment, Result result) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postIncrement")) {
            return implRegionObserver.postIncrement(c, increment, result);
        }
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preScannerOpen")) {
            implRegionObserver.preScannerOpen(c, scan);
        }
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postScannerOpen")) {
            return implRegionObserver.postScannerOpen(c, scan, s);
        }
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preScannerNext")) {
            return implRegionObserver.preScannerNext(c, s, result, limit, hasNext);
        }
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postScannerNext")) {
            return implRegionObserver.postScannerNext(c, s, result, limit, hasNext);
        }
    }

    @Override
    public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s, Cell currentCell, boolean hasMore) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postScannerFilterRow")) {
            return implRegionObserver.postScannerFilterRow(c, s, currentCell, hasMore);
        }
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preScannerClose")) {
            implRegionObserver.preScannerClose(c, s);
        }
    }

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postScannerClose")) {
            implRegionObserver.postScannerClose(c, s);
        }
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preBulkLoadHFile")) {
            implRegionObserver.preBulkLoadHFile(ctx, familyPaths);
        }
    }

    @Override
    public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths, Map<byte[], List<Path>> finalPaths) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postBulkLoadHFile")) {
            implRegionObserver.postBulkLoadHFile(ctx, familyPaths, finalPaths);
        }
    }

    @Override
    public StoreFileReader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size,
            CacheConfig cacheConf, Reference r, StoreFileReader reader) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preStoreFileReaderOpen")) {
            return implRegionObserver.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
        }
    }

    @Override
    public StoreFileReader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size,
            CacheConfig cacheConf, Reference r, StoreFileReader reader) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postStoreFileReaderOpen")) {
            return implRegionObserver.postStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
        }
    }

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx, MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postMutationBeforeWAL")) {
            return implRegionObserver.postMutationBeforeWAL(ctx, opType, mutation, oldCell, newCell);
        }
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postInstantiateDeleteTracker")) {
            return implRegionObserver.postInstantiateDeleteTracker(ctx, delTracker);
        }
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preStopRegionServer")) {
            implRegionServerObserver.preStopRegionServer(env);
        }
    }

    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preRollWALWriterRequest")) {
            implRegionServerObserver.preRollWALWriterRequest(ctx);
        }
    }

    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postRollWALWriterRequest")) {
            implRegionServerObserver.postRollWALWriterRequest(ctx);
        }
    }

    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "postCreateReplicationEndPoint")) {
            return implRegionServerObserver.postCreateReplicationEndPoint(ctx, endpoint);
        }
    }

    // TODO : need override annotations for all of the following methods

    @Override
    public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "prePrepareBulkLoad")) {
            implBulkLoadObserver.prePrepareBulkLoad(ctx);
        }
    }

    @Override
    public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "preCleanupBulkLoad")) {
            implBulkLoadObserver.preCleanupBulkLoad(ctx);
        }
    }

    @Override
    public void grant(RpcController controller, GrantRequest request, RpcCallback<GrantResponse> done) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "grant")) {
            implAccessControlService.grant(controller, request, done);
        }
    }

    @Override
    public void revoke(RpcController controller, RevokeRequest request, RpcCallback<RevokeResponse> done) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "revoke")) {
            implAccessControlService.revoke(controller, request, done);
        }
    }

    @Override
    public void getUserPermissions(RpcController controller, GetUserPermissionsRequest request, RpcCallback<GetUserPermissionsResponse> done) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "getUserPermissions")) {
            implAccessControlService.getUserPermissions(controller, request, done);
        }
    }

    @Override
    public void checkPermissions(RpcController controller, CheckPermissionsRequest request, RpcCallback<CheckPermissionsResponse> done) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "checkPermissions")) {
            implAccessControlService.checkPermissions(controller, request, done);
        }
    }

    @Override
    public void hasPermission(RpcController controller, AccessControlProtos.HasPermissionRequest request, RpcCallback<AccessControlProtos.HasPermissionResponse> done) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "hasPermission")) {
            implAccessControlService.hasPermission(controller, request, done);
        }
    }

    public void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName) throws IOException {
    }

    public void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName, boolean balancerRan) throws IOException {
    }

    public void postDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> arg0, HRegionInfo arg1, HRegionInfo arg2) throws IOException {
        // TODO Auto-generated method stub
    }

    public void preDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> arg0, HRegionInfo arg1, HRegionInfo arg2) throws IOException {
        // TODO Auto-generated method stub
    }

    private void init() {
        LOG.debug("==> RangerAuthorizationCoprocessor.init()");

        try {
            pluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<?> cls = Class.forName(RANGER_HBASE_AUTHORIZER_IMPL_CLASSNAME, true, pluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator(pluginClassLoader, "init")) {
                Object impl = cls.newInstance();

                implAccessControlService    = (AccessControlProtos.AccessControlService.Interface) impl;
                implMasterCoprocessor       = (MasterCoprocessor) impl;
                implRegionCoprocessor       = (RegionCoprocessor) impl;
                implRegionServerCoporcessor = (RegionServerCoprocessor) impl;
                implMasterObserver          = (MasterObserver) impl;
                implRegionObserver          = (RegionObserver) impl;
                implRegionServerObserver    = (RegionServerObserver) impl;
                implBulkLoadObserver        = (BulkLoadObserver) impl;
                // implEndpointObserver        = (EndpointObserver)impl;
            }
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerHbasePlugin", e);
        }

        LOG.debug("<== RangerAuthorizationCoprocessor.init()");
    }
}
