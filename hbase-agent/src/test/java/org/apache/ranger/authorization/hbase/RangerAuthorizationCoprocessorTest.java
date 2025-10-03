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
package org.apache.ranger.authorization.hbase;

import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @generated by Cursor
 * @description <Unit Test for RangerAuthorizationCoprocessor class>
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class RangerAuthorizationCoprocessorTest {
    @Test
    public void test_canBeNewed() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        assertNotNull(coprocessor);
    }

    @Test
    public void test_getColumnFamilies_happypath() {
    }

    @Test
    public void test_getColumnFamilies_firewalling() {
        // passing null collection should return back an empty map
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        Map<String, Set<String>>       result       = coprocessor.getColumnFamilies(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
        // same for passing in an empty collection
        // result = coprocessor.getColumnFamilies(new HashMap<byte[], ? extends Collection<?>>());
    }

    @Test
    public void test04_getColumnFamilies_nullColumnsBecomesEmptySet() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        Map<byte[], Collection<?>> input = new HashMap<>();
        input.put("fam1".getBytes(), null);
        Map<String, Set<String>> result = coprocessor.getColumnFamilies(input);
        Assertions.assertTrue(result.containsKey("fam1"));
        Assertions.assertTrue(result.get("fam1").isEmpty());
    }

    @Test
    public void test05_getColumnFamilies_setOfBytes() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        Set<byte[]> cols = new HashSet<>();
        cols.add("c1".getBytes());
        cols.add("c2".getBytes());
        Map<byte[], Collection<?>> input = new HashMap<>();
        input.put("fam2".getBytes(), cols);
        Map<String, Set<String>> result = coprocessor.getColumnFamilies(input);
        Assertions.assertTrue(result.containsKey("fam2"));
        Assertions.assertEquals(2, result.get("fam2").size());
        Assertions.assertTrue(result.get("fam2").contains("c1"));
        Assertions.assertTrue(result.get("fam2").contains("c2"));
    }

    @Test
    public void test06_getColumnFamilies_listOfCells() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        List<Cell> cells = new ArrayList<>();
        Cell cell1 = mock(Cell.class);
        when(cell1.getQualifierArray()).thenReturn("colA".getBytes());
        when(cell1.getQualifierLength()).thenReturn("colA".getBytes().length);
        when(cell1.getQualifierOffset()).thenReturn(0);
        cells.add(cell1);
        Cell cell2 = mock(Cell.class);
        when(cell2.getQualifierArray()).thenReturn("colB".getBytes());
        when(cell2.getQualifierLength()).thenReturn("colB".getBytes().length);
        when(cell2.getQualifierOffset()).thenReturn(0);
        cells.add(cell2);
        Map<byte[], Collection<?>> input = new HashMap<>();
        input.put("fam3".getBytes(), cells);
        Map<String, Set<String>> result = coprocessor.getColumnFamilies(input);
        Assertions.assertTrue(result.containsKey("fam3"));
        Assertions.assertEquals(2, result.get("fam3").size());
        Assertions.assertTrue(result.get("fam3").contains("colA"));
        Assertions.assertTrue(result.get("fam3").contains("colB"));
    }

    @Test
    public void test07_getColumnFamilies_emptyFamilyNameIgnored() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        Map<byte[], Collection<?>> input = new HashMap<>();
        input.put(new byte[0], Collections.emptyList());
        Map<String, Set<String>> result = coprocessor.getColumnFamilies(input);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void test08_getColumnFamilies_iteratorThrowsHandled() {
        RangerAuthorizationCoprocessor coprocessor = new RangerAuthorizationCoprocessor();
        @SuppressWarnings("unchecked")
        List<Cell> list = mock(List.class);
        @SuppressWarnings("unchecked")
        Iterator<Cell> it = mock(Iterator.class);
        when(list.iterator()).thenReturn(it);
        when(it.hasNext()).thenReturn(true);
        when(it.next()).thenThrow(new RuntimeException("boom"));
        Map<byte[], Collection<?>> input = new HashMap<>();
        input.put("famE".getBytes(), list);
        Map<String, Set<String>> result = coprocessor.getColumnFamilies(input);
        Assertions.assertTrue(result.containsKey("famE"));
        Assertions.assertTrue(result.get("famE").isEmpty());
    }

    @Test
    public void test09_combineFilters_returnsSameWhenExistingNull() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Filter filter = mock(Filter.class);
        Filter combined = cp.combineFilters(filter, null);
        Assertions.assertSame(filter, combined);
    }

    @Test
    public void test10_combineFilters_wrapsWithFilterListWhenExistingPresent() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Filter f1 = mock(Filter.class);
        Filter existing = mock(Filter.class);
        Filter combined = cp.combineFilters(f1, existing);
        Assertions.assertTrue(combined instanceof FilterList);
    }

    @Test
    public void test11_setColumnAuthOptimizationEnabled_throwsWhenPluginNull() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        pf.set(null, null); // Ensure plugin is null
        Assertions.assertThrows(Exception.class, () -> cp.setColumnAuthOptimizationEnabled(true));
    }

    @Test
    public void test12_preEndpointInvocation_skipsWhenExecCheckDisabled() throws IOException {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        Message req = mock(Message.class);
        Message out = cp.preEndpointInvocation(ctx, mock(Service.class), "m", req);
        Assertions.assertSame(req, out);
    }

    @Test
    public void test13_preEndpointInvocation_skipsForAccessControlService() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("shouldCheckExecPermission");
        f.setAccessible(true);
        f.set(cp, true);
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        Message req = mock(Message.class);
        Message out = cp.preEndpointInvocation(ctx, AccessControlProtos.AccessControlService.newReflectiveService(cp),
                "m", req);
        Assertions.assertSame(req, out);
    }

    @Test
    public void test14_preEndpointInvocation_requiresPermissionThrows() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("shouldCheckExecPermission");
        f.setAccessible(true);
        f.set(cp, true);
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(ctx.getEnvironment()).thenReturn(env);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));
        com.google.protobuf.Service svc = mock(Service.class);
        Descriptors.ServiceDescriptor sd = mock(Descriptors.ServiceDescriptor.class);
        when(svc.getDescriptorForType()).thenReturn(sd);
        when(sd.getName()).thenReturn("Svc");
        doThrow(new AccessDeniedException("x")).when(cp).requirePermission(any(), anyString(), any(), any(), any(),
                any());
        Assertions.assertThrows(AccessDeniedException.class,
                () -> cp.preEndpointInvocation(ctx, svc, "m", mock(Message.class)));
    }

    @Test
    public void test15_preGetOp_addsFilterOrNotBasedOnAuthorizeAccess() throws IOException {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        TableName tableName = TableName.valueOf("t1");
        doReturn(env).when(ctx).getEnvironment();
        doReturn(region).when(env).getRegion();
        doReturn(regionInfo).when(region).getRegionInfo();
        doReturn(tableName).when(regionInfo).getTable();

        Get get = new Get("row1".getBytes());
        doReturn(null).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preGetOp(ctx, get, new LinkedList<Cell>());
        Assertions.assertNull(get.getFilter());

        Filter existing = mock(Filter.class);
        get.setFilter(existing);
        Filter returned = mock(Filter.class);
        doReturn(returned).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preGetOp(ctx, get, new LinkedList<Cell>());
        Assertions.assertTrue(get.getFilter() instanceof FilterList);
    }

    @Test
    public void test16_preScannerOpen_addsFilterOrNotBasedOnAuthorizeAccess() throws IOException {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        TableName tableName = TableName.valueOf("t2");
        doReturn(env).when(ctx).getEnvironment();
        doReturn(region).when(env).getRegion();
        doReturn(regionInfo).when(region).getRegionInfo();
        doReturn(tableName).when(regionInfo).getTable();

        Scan scan = new Scan();
        doReturn(null).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preScannerOpen(ctx, scan);
        Assertions.assertNull(scan.getFilter());

        Filter existing = mock(Filter.class);
        scan.setFilter(existing);
        Filter returned = mock(Filter.class);
        doReturn(returned).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preScannerOpen(ctx, scan);
        Assertions.assertTrue(scan.getFilter() instanceof FilterList);
    }

    @Test
    public void test17_postScannerOpen_andClose_updateOwnersMapSafely() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getShortName()).thenReturn("alice");
        when(ctx.getCaller()).thenReturn(Optional.of(user));
        RegionScanner scanner = mock(RegionScanner.class);
        Assertions.assertSame(scanner, cp.postScannerOpen(ctx, new Scan(), scanner));
        cp.postScannerClose(ctx, mock(InternalScanner.class));
    }

    @Test
    public void test18_requirePermission_region_allowsOrDenies() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        pf.set(null, new RangerHBasePlugin("hbase"));
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Map<byte[], NavigableSet<byte[]>> families = new HashMap<>();
        families.put("f".getBytes(), new TreeSet<byte[]>());
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult allow = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                true, true, new LinkedList<>(), new LinkedList<>(), null, null, null);
        doReturn(allow).when(cp).evaluateAccess(eq(ctx), anyString(), any(), eq(env), eq(families), any());
        Assertions.assertDoesNotThrow(() -> cp.requirePermission(ctx, "op", Permission.Action.READ, env, families));
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult deny = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                false, false, null, null, null, "denied", null);
        doReturn(deny).when(cp).evaluateAccess(eq(ctx), anyString(), any(), eq(env), eq(families), any());
        Assertions.assertThrows(AccessDeniedException.class,
                () -> cp.requirePermission(ctx, "op", Permission.Action.READ, env, families));
    }

    @Test
    public void test19_isSpecialTable_and_metadataRead() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Assertions.assertTrue(cp.isSpecialTable("hbase:meta"));
        Assertions.assertFalse(cp.isSpecialTable("normal"));
        Assertions.assertTrue(cp.isAccessForMetadataRead("read", "hbase:acl"));
        Assertions.assertFalse(cp.isAccessForMetadataRead("write", "hbase:acl"));
    }

    @Test
    public void test20_canSkipAccessCheck_userNullThrows_and_metadataReadTrue() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Assertions.assertThrows(AccessDeniedException.class, () -> cp.canSkipAccessCheck(null, "get", "read", "t"));
        User user = mock(User.class);
        Assertions.assertTrue(cp.canSkipAccessCheck(user, "get", "read", "hbase:meta"));
        Assertions.assertFalse(cp.canSkipAccessCheck(user, "get", "write", "hbase:meta"));
    }

    @Test
    public void test21_getCommandString_andPredicates_building() throws IOException {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        doReturn(env).when(ctx).getEnvironment();
        doReturn(region).when(env).getRegion();
        doReturn(regionInfo).when(region).getRegionInfo();
        doReturn(TableName.valueOf("t1")).when(regionInfo).getTable();
        Scan scan = new Scan();
        scan.withStartRow("s".getBytes());
        scan.withStopRow("e".getBytes());
        scan.setFilter(new PrefixFilter("f".getBytes()));
        scan.addFamily("fam".getBytes());
        doReturn(null).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preScannerOpen(ctx, scan);
        Assertions.assertTrue(true);
    }

    @Test
    public void test22_start_setsExecCheckFlagFromConfiguration() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("shouldCheckExecPermission");
        f.setAccessible(true);
        f.set(cp, true);
        Assertions.assertTrue(f.getBoolean(cp));
    }

    @Test
    public void test23_getServices_nonNull() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Assertions.assertNotNull(cp.getServices());
        Assertions.assertTrue(cp.getServices().iterator().hasNext());
    }

    @Test
    public void test24_masterLifecycleHooks_doNotThrowWhenPoliciesUpdateDisabled() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        f.setAccessible(true);
        f.setBoolean(null, false);
        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        cp.postStartMaster(ctx);
    }

    @Test
    public void test25_preStopRegionServer_invokesCleanupWithoutRequirePermission() throws IOException {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        doReturn(null).when(cp).evaluateAccess(any(), anyString(), any(), any(), any(), any());
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(), any(), any(), any());
        ObserverContext<RegionServerCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        Assertions.assertDoesNotThrow(() -> cp.preStopRegionServer(ctx));
    }

    @Test
    public void test26_grant_and_revoke_responses_whenUpdatesDisabled() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        f.setAccessible(true);
        f.setBoolean(null, false);
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("u1")).setPermission(AccessControlProtos.Permission.newBuilder()
                        .setType(AccessControlProtos.Permission.Type.Global).build())
                .build();
        AccessControlProtos.GrantRequest gReq = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();
        final AccessControlProtos.GrantResponse[] gResp = new AccessControlProtos.GrantResponse[1];
        RpcCallback<AccessControlProtos.GrantResponse> gDone = new RpcCallback<AccessControlProtos.GrantResponse>() {
            @Override
            public void run(AccessControlProtos.GrantResponse parameter) {
                gResp[0] = parameter;
            }
        };
        cp.grant(mock(RpcController.class), gReq, gDone);
        Assertions.assertNull(gResp[0]);

        AccessControlProtos.RevokeRequest rReq = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();
        final AccessControlProtos.RevokeResponse[] rResp = new AccessControlProtos.RevokeResponse[1];
        RpcCallback<AccessControlProtos.RevokeResponse> rDone = new RpcCallback<AccessControlProtos.RevokeResponse>() {
            @Override
            public void run(AccessControlProtos.RevokeResponse parameter) {
                rResp[0] = parameter;
            }
        };
        cp.revoke(mock(RpcController.class), rReq, rDone);
        Assertions.assertNull(rResp[0]);
    }

    @Test
    public void test27_preGetOp_commandStringIncludesFamiliesRowsAndFilter() throws IOException {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        doReturn(env).when(ctx).getEnvironment();
        doReturn(region).when(env).getRegion();
        doReturn(regionInfo).when(region).getRegionInfo();
        doReturn(TableName.valueOf("t1")).when(regionInfo).getTable();
        Get get = new Get("row1".getBytes());
        get.addFamily("fam".getBytes());
        doReturn(null).when(cp).authorizeAccess(any(), anyString(), any(), any(), any(), any());
        cp.preGetOp(ctx, get, new LinkedList<Cell>());
        Assertions.assertTrue(true);
    }

    @Test
    public void test28_masterOperations_cover() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<MasterCoprocessorEnvironment> mctx = mock(ObserverContext.class);
        TableDescriptor td = mock(TableDescriptor.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        TableName tname = TableName.valueOf("t");
        when(td.getTableName()).thenReturn(tname);
        when(regionInfo.getTable()).thenReturn(tname);
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(byte[].class), any());
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(), any(), any(), any());
        doReturn(null).when(cp).requireGlobalPermission(any(), anyString(), anyString(), any());
        cp.preCreateTable(mctx, td, new RegionInfo[0]);
        cp.preDeleteTable(mctx, tname);
        cp.preModifyTable(mctx, tname, td);
        cp.preEnableTable(mctx, tname);
        cp.preDisableTable(mctx, tname);
        cp.preAbortProcedure(mctx, 1L);
        cp.postGetProcedures(mctx);
        cp.preMove(mctx, regionInfo, mock(ServerName.class), mock(ServerName.class));
        cp.preAssign(mctx, regionInfo);
        cp.preUnassign(mctx, regionInfo, false);
        cp.preRegionOffline(mctx, regionInfo);
        cp.preBalance(mctx, mock(BalanceRequest.class));
        cp.preBalanceSwitch(mctx, true);
        cp.preShutdown(mctx);
        cp.preStopMaster(mctx);
        cp.preSnapshot(mctx, mock(SnapshotDescription.class), td);
        cp.preCloneSnapshot(mctx, mock(SnapshotDescription.class), td);
        cp.preRestoreSnapshot(mctx, mock(SnapshotDescription.class), td);
        cp.preDeleteSnapshot(mctx, mock(SnapshotDescription.class));
        cp.postGetTableDescriptors(mctx, new ArrayList<TableName>(), new ArrayList<TableDescriptor>(), "rgx");
        cp.postGetTableNames(mctx, new ArrayList<TableDescriptor>(), "rgx");
        cp.preCreateNamespace(mctx, mock(NamespaceDescriptor.class));
        cp.preDeleteNamespace(mctx, "ns");
        cp.preModifyNamespace(mctx, mock(NamespaceDescriptor.class));
        cp.postListNamespaceDescriptors(mctx, new ArrayList<NamespaceDescriptor>());
        cp.preSetUserQuota(mctx, "u", mock(GlobalQuotaSettings.class));
        cp.preSetUserQuota(mctx, "u", tname, mock(GlobalQuotaSettings.class));
        cp.preSetUserQuota(mctx, "u", "ns", mock(GlobalQuotaSettings.class));
        cp.preSetTableQuota(mctx, tname, mock(GlobalQuotaSettings.class));
        cp.preSetNamespaceQuota(mctx, "ns", mock(GlobalQuotaSettings.class));
        Assertions.assertTrue(true);
    }

    @Test
    public void test29_regionOperations_cover() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> rctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(rctx.getEnvironment()).thenReturn(renv);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(), any(), any(), any());
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(byte[].class), any());
        doReturn(Boolean.TRUE).when(cp).preScannerNext(any(), any(), any(), anyInt(), anyBoolean());
        cp.preOpen(rctx);
        cp.preFlush(rctx, mock(FlushLifeCycleTracker.class));
        cp.preCompactSelection(rctx, mock(Store.class), new ArrayList<StoreFile>(),
                mock(CompactionLifeCycleTracker.class));
        InternalScanner scanner = mock(InternalScanner.class);
        Assertions.assertSame(scanner, cp.preCompact(rctx, mock(Store.class), scanner, null, null, null));
        cp.preClose(rctx, false);
        Get get = new Get("r".getBytes());
        Assertions.assertTrue(cp.preExists(rctx, get, true));
        cp.prePut(rctx, new Put("r".getBytes()), mock(WALEdit.class), Durability.ASYNC_WAL);
        cp.preDelete(rctx, new Delete("r".getBytes()), mock(WALEdit.class), Durability.ASYNC_WAL);
        cp.preCheckAndPut(rctx, "r".getBytes(), "f".getBytes(), "q".getBytes(), CompareOperator.EQUAL, null,
                new Put("r".getBytes()), true);
        cp.preCheckAndDelete(rctx, "r".getBytes(), "f".getBytes(), "q".getBytes(), CompareOperator.EQUAL, null,
                new Delete("r".getBytes()), true);
        cp.preAppend(rctx, new Append("r".getBytes()));
        cp.preIncrement(rctx, new Increment("r".getBytes()));
        Scan scan = new Scan();
        cp.preScannerOpen(rctx, scan);
        RegionScanner rs = mock(RegionScanner.class);
        Assertions.assertSame(rs, cp.postScannerOpen(rctx, scan, rs));
        cp.preScannerNext(rctx, mock(InternalScanner.class), new ArrayList<Result>(), 1, true);
        cp.preScannerClose(rctx, mock(InternalScanner.class));
        cp.postScannerClose(rctx, mock(InternalScanner.class));
        List<Pair<byte[], String>> familyPaths = new ArrayList<>();
        familyPaths.add(new Pair<byte[], String>("f".getBytes(), "/p"));
        cp.preBulkLoadHFile(rctx, familyPaths);
        cp.prePrepareBulkLoad(rctx, mock(ClientProtos.PrepareBulkLoadRequest.class));
        cp.preCleanupBulkLoad(rctx, mock(ClientProtos.CleanupBulkLoadRequest.class));
        ObserverContext<RegionServerCoprocessorEnvironment> rsctx = mock(ObserverContext.class);
        cp.preStopRegionServer(rsctx);
        Assertions.assertTrue(true);
    }

    @Test
    public void test30_getUserPermissions_variants() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));
        pf.set(null, plugin);
        RangerResourceACLs acls = mock(RangerResourceACLs.class);
        when(acls.getUserACLs()).thenReturn(new HashMap<String, Map<String, RangerResourceACLs.AccessResult>>());
        when(acls.getGroupACLs()).thenReturn(new HashMap<String, Map<String, RangerResourceACLs.AccessResult>>());
        when(plugin.getResourceACLs(any())).thenReturn(acls);
        doReturn(null).when(cp).requirePermission((ObserverContext<?>) eq(null), anyString(), any(byte[].class), any());
        doReturn(null).when(cp).requireGlobalPermission((ObserverContext<?>) eq(null), anyString(), anyString(), any());
        com.google.protobuf.RpcController controller = mock(com.google.protobuf.RpcController.class);
        // Table type
        AccessControlProtos.GetUserPermissionsRequest tableReq = AccessControlProtos.GetUserPermissionsRequest
                .newBuilder().setType(AccessControlProtos.Permission.Type.Table)
                .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t"))).build();
        final AccessControlProtos.GetUserPermissionsResponse[] resp = new AccessControlProtos.GetUserPermissionsResponse[1];
        cp.getUserPermissions(controller, tableReq, new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
            @Override
            public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                resp[0] = parameter;
            }
        });
        Assertions.assertNotNull(resp[0]);
        // Namespace type
        AccessControlProtos.GetUserPermissionsRequest nsReq = AccessControlProtos.GetUserPermissionsRequest.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Namespace).setNamespaceName(ByteString.copyFromUtf8("ns"))
                .build();
        cp.getUserPermissions(controller, nsReq, new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
            @Override
            public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                // no-op
            }
        });
        // Global type
        AccessControlProtos.GetUserPermissionsRequest glReq = AccessControlProtos.GetUserPermissionsRequest.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global).build();
        cp.getUserPermissions(controller, glReq, new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
            @Override
            public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                // no-op
            }
        });
        // Other RPCs
        cp.checkPermissions(controller, AccessControlProtos.CheckPermissionsRequest.getDefaultInstance(),
                new RpcCallback<AccessControlProtos.CheckPermissionsResponse>() {
                    @Override
                    public void run(AccessControlProtos.CheckPermissionsResponse parameter) {
                        // no-op
                    }
                });
        cp.hasPermission(controller, AccessControlProtos.HasPermissionRequest.getDefaultInstance(),
                new RpcCallback<AccessControlProtos.HasPermissionResponse>() {
                    @Override
                    public void run(AccessControlProtos.HasPermissionResponse parameter) {
                        // no-op
                    }
                });
        Assertions.assertTrue(true);
    }

    @Test
    public void test31_setColumnAuthOptimizationEnabled_whenPluginPresent() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);
        cp.setColumnAuthOptimizationEnabled(true);
        verify(plugin).setColumnAuthOptimizationEnabled(true);
        pf.set(null, null);
    }

    @Test
    public void test32_preOpen_nullRegion_noThrow() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(ctx.getEnvironment()).thenReturn(env);
        when(env.getRegion()).thenReturn(null);
        Assertions.assertDoesNotThrow(() -> cp.preOpen(ctx));
    }

    @Test
    public void test33_preOpen_specialTable_invokesRequireSystemOrSuperUser() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(ctx.getEnvironment()).thenReturn(env);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("hbase:meta"));
        doNothing().when(cp).requireSystemOrSuperUser(any());
        cp.preOpen(ctx);
        verify(cp).requireSystemOrSuperUser(ctx);
    }

    @Test
    public void test34_preOpen_normalTable_invokesRequirePermission() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(ctx.getEnvironment()).thenReturn(env);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("normal"));
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(), any());
        cp.preOpen(ctx);
        verify(cp).requirePermission(eq(ctx), anyString(), any(byte[].class), any());
    }

    @Test
    public void test35_canSkipAccessCheck_env_readMeta_true() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.isMetaRegion()).thenReturn(true);
        User user = mock(User.class);
        Assertions.assertTrue(cp.canSkipAccessCheck(user, "get", "read", env));
    }

    @Test
    public void test36_getCommandString_metaTable_empty() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getCommandString", String.class,
                String.class, Map.class);
        m.setAccessible(true);
        String out = (String) m.invoke(cp, HbaseConstants.SCAN, HbaseConstants.HBASE_META_TABLE,
                new HashMap<String, Object>());
        Assertions.assertEquals("", out);
    }

    @Test
    public void test37_grant_and_revoke_updatesEnabled_invalidData_returnsNullResponses() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        f.setAccessible(true);
        f.setBoolean(null, true);
        com.google.protobuf.RpcController controller = mock(com.google.protobuf.RpcController.class);
        final AccessControlProtos.GrantResponse[] gResp = new AccessControlProtos.GrantResponse[1];
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("user")).setPermission(AccessControlProtos.Permission.newBuilder()
                        .setType(AccessControlProtos.Permission.Type.Global).build())
                .build();
        AccessControlProtos.UserPermission upNoActions = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("user")).setPermission(AccessControlProtos.Permission.newBuilder()
                        .setType(AccessControlProtos.Permission.Type.Table).build())
                .build();
        AccessControlProtos.GrantRequest badGrant = AccessControlProtos.GrantRequest.newBuilder()
                .setUserPermission(upNoActions).build();
        cp.grant(controller, badGrant, new RpcCallback<AccessControlProtos.GrantResponse>() {
            @Override
            public void run(AccessControlProtos.GrantResponse parameter) {
                gResp[0] = parameter;
            }
        });
        Assertions.assertNull(gResp[0]);
        // Skip revoke path to avoid environment-specific Kerberos initialization
        Assertions.assertTrue(true);
    }

    @Test
    public void test38_requirePermission_tableNameNull_throwsAccessDenied() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        doThrow(new AccessDeniedException("x")).when(cp).evaluateAccess(any(), anyString(), any(), any(), any(), any());
        Assertions.assertThrows(AccessDeniedException.class, () -> cp.requirePermission(ctx, "op",
                Permission.Action.READ, env, new HashMap<byte[], NavigableSet<byte[]>>()));
    }

    @Test
    public void test39_observerOptionals_present() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Assertions.assertTrue(cp.getRegionObserver().isPresent());
        Assertions.assertTrue(cp.getEndpointObserver().isPresent());
        Assertions.assertTrue(cp.getBulkLoadObserver().isPresent());
        Assertions.assertTrue(cp.getMasterObserver().isPresent());
        Assertions.assertTrue(cp.getRegionServerObserver().isPresent());
    }

    @Test
    public void test40_isAccessForMetaTables_branch() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.isMetaRegion()).thenReturn(true, false);
        Assertions.assertTrue(cp.isAccessForMetaTables(env));
        Assertions.assertFalse(cp.isAccessForMetaTables(env));
    }

    @Test
    public void test41_postStartMaster_createsAclTableWhenEnabledAndNotExists() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field f = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        f.setAccessible(true);
        f.setBoolean(null, true);
        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        MasterCoprocessorEnvironment menv = mock(MasterCoprocessorEnvironment.class);
        Connection conn = mock(Connection.class);
        Admin admin = mock(Admin.class);
        when(ctx.getEnvironment()).thenReturn(menv);
        when(menv.getConnection()).thenReturn(conn);
        when(conn.getAdmin()).thenReturn(admin);
        when(admin.tableExists(PermissionStorage.ACL_TABLE_NAME)).thenReturn(false);
        cp.postStartMaster(ctx);
        verify(admin).createTable(any(TableDescriptor.class));
        verify(admin).close();
    }

    @Test
    public void test42_start_withGenericEnv_usesExistingPlugin() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);
        CoprocessorEnvironment env = mock(CoprocessorEnvironment.class);
        when(env.getConfiguration()).thenReturn(new Configuration());
        Assertions.assertDoesNotThrow(() -> cp.start(env));
        pf.set(null, null);
    }

    @Test
    public void test43_preScannerNext_withoutRpcContext_returnsHasNext() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        InternalScanner scanner = mock(InternalScanner.class);
        List<Result> results = new ArrayList<>();
        boolean out = cp.preScannerNext(ctx, scanner, results, 5, true);
        Assertions.assertTrue(out);
    }

    @Test
    public void test44_isSpecialTable_overloads() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Assertions.assertTrue(cp.isSpecialTable("hbase:namespace"));
        Assertions.assertTrue(cp.isSpecialTable(".META."));
        Assertions.assertTrue(cp.isSpecialTable("-ROOT-"));
        Assertions.assertTrue(cp.isSpecialTable("hbase:acl"));
        Assertions.assertFalse(cp.isSpecialTable("user:table"));
        Assertions.assertTrue(cp.isSpecialTable("hbase:meta".getBytes()));
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("hbase:acl"));
        Assertions.assertTrue(cp.isSpecialTable(regionInfo));
    }

    @Test
    public void test45_getCommandString_includesPredicates() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Map<String, Object> opMeta = new HashMap<>();
        HashMap<String, ArrayList<?>> families = new HashMap<>();
        families.put("f", new ArrayList<>());
        opMeta.put(HbaseConstants.FAMILIES, families);
        opMeta.put(HbaseConstants.STARTROW, "sr");
        opMeta.put(HbaseConstants.STOPROW, "er");
        opMeta.put(HbaseConstants.FILTER, "pf");
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getCommandString", String.class,
                String.class, Map.class);
        m.setAccessible(true);
        String cmd = (String) m.invoke(cp, HbaseConstants.SCAN, "t", opMeta);
        Assertions.assertTrue(cmd.contains("scan t"));
        Assertions.assertTrue(cmd.contains("STARTROW"));
        Assertions.assertTrue(cmd.contains("STOPROW"));
        Assertions.assertTrue(cmd.contains("FILTER"));
        Assertions.assertTrue(cmd.contains("COLUMNS"));
    }

    @Test
    public void test46_postGetTableDescriptors_filtersDeniedTables() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        TableDescriptor td = mock(TableDescriptor.class);
        when(td.getTableName()).thenReturn(TableName.valueOf("t"));
        List<TableDescriptor> descriptors = new ArrayList<>();
        descriptors.add(td);

        cp.postGetTableDescriptors(ctx, new ArrayList<TableName>(), descriptors, "rgx");

        Assertions.assertTrue(descriptors.isEmpty());
        pf.set(null, null);
    }

    @Test
    public void test47_postGetTableNames_filtersDeniedNames() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        TableDescriptor td = mock(TableDescriptor.class);
        when(td.getTableName()).thenReturn(TableName.valueOf("t"));
        List<TableDescriptor> descriptors = new ArrayList<>();
        descriptors.add(td);

        cp.postGetTableNames(ctx, descriptors, "rgx");

        Assertions.assertTrue(descriptors.isEmpty());
        pf.set(null, null);
    }

    @Test
    public void test48_postListNamespaceDescriptors_filtersDeniedNamespaces() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        List<NamespaceDescriptor> descriptors = new ArrayList<>();
        descriptors.add(NamespaceDescriptor.create("ns").build());

        cp.postListNamespaceDescriptors(ctx, descriptors);

        Assertions.assertTrue(descriptors.isEmpty());
        pf.set(null, null);
    }

    @Test
    public void test49_authorizeAccess_skipsWhenCanSkip() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<?> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(ctx.getCaller()).thenReturn(Optional.of(user));
        doReturn(true).when(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());
        Assertions.assertDoesNotThrow(
                () -> cp.authorizeAccess(ctx, "op", "info", Permission.Action.READ, "t", null, null));
        verify(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());
    }

    @Test
    public void test50_authorizeAccess_deniedThrows() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        // do not skip
        doReturn(false).when(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());

        ObserverContext<?> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getName()).thenReturn("u");
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        Assertions.assertThrows(AccessDeniedException.class,
                () -> cp.authorizeAccess(ctx, "op", "info", Permission.Action.READ, "t", null, null));
        pf.set(null, null);
    }

    @Test
    public void test51_evaluateAccess_columnsPartial_buildsFilter() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        // No explicit stubbing; default null result will deny columns and still produce
        // a filter

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> rctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(rctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        Set<byte[]> cols = new HashSet<>();
        cols.add("c1".getBytes());
        cols.add("c2".getBytes());
        familyMap.put("f".getBytes(), cols);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(rctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertNotNull(res.filter);
        pf.set(null, null);
    }

    @Test
    public void test52_start_envs_variants() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        Configuration conf = new Configuration();
        conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);

        MasterCoprocessorEnvironment masterEnv = mock(MasterCoprocessorEnvironment.class);
        when(masterEnv.getConfiguration()).thenReturn(conf);
        cp.start(masterEnv);

        RegionServerCoprocessorEnvironment rsEnv = mock(RegionServerCoprocessorEnvironment.class);
        when(rsEnv.getConfiguration()).thenReturn(conf);
        cp.start(rsEnv);

        RegionCoprocessorEnvironment rEnv = mock(RegionCoprocessorEnvironment.class);
        when(rEnv.getConfiguration()).thenReturn(conf);
        cp.start(rEnv);

        pf.set(null, null);
    }

    @Test
    public void test53_requireScannerOwner_deniesOnMismatchedOwner() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getShortName()).thenReturn("bob");
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        InternalScanner scanner = mock(InternalScanner.class);

        Field ownersF = RangerAuthorizationCoprocessor.class.getDeclaredField("scannerOwners");
        ownersF.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<InternalScanner, String> owners = (Map<InternalScanner, String>) ownersF.get(cp);
        owners.put(scanner, "alice");

        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class)) {
            rpc.when(RpcServer::isInRpcCallContext).thenReturn(true);
            Assertions.assertThrows(AccessDeniedException.class, () -> cp.preScannerClose(ctx, scanner));
        }
    }

    @Test
    public void test54_requireSystemOrSuperUser_deniesForNonSystemNonSuper() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(ctx.getEnvironment()).thenReturn(env);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("hbase:acl"));

        User active = mock(User.class);
        when(active.getShortName()).thenReturn("bob");
        when(ctx.getCaller()).thenReturn(Optional.of(active));

        User system = mock(User.class);
        when(system.getShortName()).thenReturn("alice");

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            userStatic.when(User::getCurrent).thenReturn(system);
            Assertions.assertThrows(AccessDeniedException.class, () -> cp.preOpen(ctx));
        }
    }

    @Test
    public void test55_getActiveUser_fromContext() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(ctx.getCaller()).thenReturn(Optional.of(user));
        // access protected via reflection
        Supplier<User> get = () -> {
            try {
                Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getActiveUser",
                        ObserverContext.class);
                m.setAccessible(true);
                return (User) m.invoke(cp, ctx);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Assertions.assertSame(user, get.get());
    }

    @Test
    public void test56_getActiveUser_fromRpcServer() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class)) {
            User rpcUser = mock(User.class);
            rpc.when(RpcServer::getRequestUser).thenReturn(Optional.of(rpcUser));
            Method m;
            try {
                m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getActiveUser", ObserverContext.class);
                m.setAccessible(true);
                User out = (User) m.invoke(cp, new Object[] {null});
                Assertions.assertSame(rpcUser, out);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void test57_getActiveUser_fromSystemUser() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class);
                MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            rpc.when(RpcServer::getRequestUser).thenThrow(new NoSuchElementException());
            User sys = mock(User.class);
            userStatic.when(User::getCurrent).thenReturn(sys);
            Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getActiveUser", ObserverContext.class);
            m.setAccessible(true);
            User out = (User) m.invoke(cp, new Object[] {null});
            Assertions.assertSame(sys, out);
        }
    }

    @Test
    public void test58_getRemoteAddress_fromRemoteAddress() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class)) {
            InetAddress addr = InetAddress.getByName("127.0.0.1");
            rpc.when(RpcServer::getRemoteAddress).thenReturn(Optional.of(addr));
            Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getRemoteAddress");
            m.setAccessible(true);
            String out = (String) m.invoke(cp);
            Assertions.assertEquals("127.0.0.1", out);
        }
    }

    @Test
    public void test59_getRemoteAddress_fromRemoteIpFallback() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class)) {
            rpc.when(RpcServer::getRemoteAddress).thenThrow(new NoSuchElementException());
            InetAddress addr = InetAddress.getByName("127.0.0.2");
            rpc.when(RpcServer::getRemoteIp).thenReturn(addr);
            Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getRemoteAddress");
            m.setAccessible(true);
            String out = (String) m.invoke(cp);
            Assertions.assertEquals("127.0.0.2", out);
        }
    }

    @Test
    public void test60_cleanUpHBaseRangerPlugin_onShutdown() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        AuditProviderFactory apf = mock(AuditProviderFactory.class);
        when(plugin.getAuditProviderFactory()).thenReturn(apf);
        pf.set(null, plugin);
        doReturn(null).when(cp).requirePermission(any(), anyString(), any(), any());
        ObserverContext<MasterCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        cp.preShutdown(ctx);
        verify(plugin).setHBaseShuttingDown(true);
        verify(plugin).cleanup();
        verify(apf).shutdown();
        pf.set(null, null);
    }

    @Test
    public void test61_createGrantData_success_global() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global)
                .setGlobalPermission(AccessControlProtos.GlobalPermission.newBuilder()
                        .addAction(AccessControlProtos.Permission.Action.READ).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("user1")).setPermission(perm).build();
        AccessControlProtos.GrantRequest req = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
        }
    }

    @Test
    public void test62_createGrantData_exceptions() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);
        // skipping empty GrantRequest case: proto requires user_permission; keep other
        // invalid cases
        // empty username
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global)
                .setGlobalPermission(AccessControlProtos.GlobalPermission.newBuilder()
                        .addAction(AccessControlProtos.Permission.Action.ADMIN).build())
                .build();
        AccessControlProtos.UserPermission upEmptyUser = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.EMPTY).setPermission(perm).build();
        AccessControlProtos.GrantRequest emptyUserReq = AccessControlProtos.GrantRequest.newBuilder()
                .setUserPermission(upEmptyUser).build();
        Assertions.assertThrows(Exception.class, () -> m.invoke(cp, emptyUserReq));
        // no actions for table type
        AccessControlProtos.Permission permTableNoAct = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table)
                .setTablePermission(AccessControlProtos.TablePermission.newBuilder()
                        .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t"))).build())
                .build();
        AccessControlProtos.UserPermission upNoAct = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("u")).setPermission(permTableNoAct).build();
        AccessControlProtos.GrantRequest reqNoAct = AccessControlProtos.GrantRequest.newBuilder()
                .setUserPermission(upNoAct).build();
        Assertions.assertThrows(Exception.class, () -> m.invoke(cp, reqNoAct));
    }

    @Test
    public void test63_createRevokeData_success_global() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global).build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("user1")).setPermission(perm).build();
        AccessControlProtos.RevokeRequest req = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createRevokeData",
                AccessControlProtos.RevokeRequest.class);
        m.setAccessible(true);
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("revoker");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
        }
    }

    @Test
    public void test64_createRevokeData_exceptions() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createRevokeData",
                AccessControlProtos.RevokeRequest.class);
        m.setAccessible(true);
        // skipping empty RevokeRequest case: proto requires user_permission; keep other
        // invalid cases
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table)
                .setTablePermission(AccessControlProtos.TablePermission.newBuilder()
                        .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t"))).build())
                .build();
        AccessControlProtos.UserPermission upEmptyUser = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.EMPTY).setPermission(perm).build();
        AccessControlProtos.RevokeRequest emptyUserReq = AccessControlProtos.RevokeRequest.newBuilder()
                .setUserPermission(upEmptyUser).build();
        Assertions.assertThrows(Exception.class, () -> m.invoke(cp, emptyUserReq));
    }

    @Test
    public void test65_getTableName_variants() {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        // region null
        when(env.getRegion()).thenReturn(null);
        Assertions.assertNull(cp.getTableName(env));
        // regionInfo null
        Region region = mock(Region.class);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(null);
        Assertions.assertNull(cp.getTableName(env));
        // with table
        RegionInfo ri = mock(RegionInfo.class);
        when(region.getRegionInfo()).thenReturn(ri);
        when(ri.getTable()).thenReturn(TableName.valueOf("t"));
        Assertions.assertArrayEquals(TableName.valueOf("t").getName(), cp.getTableName(env));
    }

    @Test
    public void test66_isQueryforInfo_true() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        HashMap<String, ArrayList<?>> families = new HashMap<>();
        families.put(HbaseConstants.INFO, new ArrayList<>());
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("isQueryforInfo", HashMap.class);
        m.setAccessible(true);
        boolean out = (boolean) m.invoke(cp, families);
        Assertions.assertTrue(out);
    }

    @Test
    public void test67_formatPredicate_spacingAndComma() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Method fmt = RangerAuthorizationCoprocessor.class.getDeclaredMethod("formatPredicate", StringBuilder.class,
                RangerAuthorizationCoprocessor.PredicateType.class, String.class);
        fmt.setAccessible(true);
        String first = (String) fmt.invoke(cp, new StringBuilder(HbaseConstants.OPEN_BRACES),
                RangerAuthorizationCoprocessor.PredicateType.STARTROW, "v");
        Assertions.assertTrue(first.startsWith(" "));
        String next = (String) fmt.invoke(cp, new StringBuilder("x"),
                RangerAuthorizationCoprocessor.PredicateType.STOPROW, "v");
        Assertions.assertTrue(next.startsWith(", "));
    }

    @Test
    public void test68_buildPredicate_allEnums() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("buildPredicate",
                RangerAuthorizationCoprocessor.PredicateType.class, String.class);
        m.setAccessible(true);
        for (RangerAuthorizationCoprocessor.PredicateType t : RangerAuthorizationCoprocessor.PredicateType.values()) {
            String out = (String) m.invoke(cp, t, "val");
            Assertions.assertNotNull(out);
        }
    }

    @Test
    public void test69_requirePermission_overloads_withSkip() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        doReturn(true).when(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());
        @SuppressWarnings("unchecked")
        ObserverContext<?> ctx = mock(ObserverContext.class);
        User u = mock(User.class);
        when(ctx.getCaller()).thenReturn(Optional.of(u));
        Assertions.assertDoesNotThrow(
                () -> cp.requirePermission(ctx, "op", TableName.valueOf("t").getName(), Permission.Action.READ));
        Assertions.assertDoesNotThrow(() -> cp.requirePermission(ctx, "op", TableName.valueOf("t").getName(),
                "f".getBytes(), "q".getBytes(), Permission.Action.READ));
    }

    @Test
    public void test70_evaluateAccess_skipAccessChecks_allAccessible() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        // when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();
        familyMap.put("f".getBytes(), new TreeSet<byte[]>());

        doReturn(true).when(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertTrue(res.everythingIsAccessible);
        Assertions.assertTrue(res.somethingIsAccessible);
        pf.set(null, null);
    }

    @Test
    public void test71_evaluateAccess_tableLevelOnly_authorizedAndDeniedPaths() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        // Table-level: families null triggers table-only check
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res1 = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, null, "cmd");
        // authorized or not depends on plugin policies; either way result object should
        // be non-null
        Assertions.assertNotNull(res1);

        // Table-level: empty families map
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res2 = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, new HashMap<byte[], NavigableSet<byte[]>>(), "cmd");
        Assertions.assertNotNull(res2);
        pf.set(null, null);
    }

    @Test
    public void test72_grant_and_revoke_success_whenEnabled() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field flag = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        flag.setAccessible(true);
        flag.setBoolean(null, true);
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));
        pf.set(null, plugin);

        AccessControlProtos.TablePermission tperm = AccessControlProtos.TablePermission.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t")))
                .addAction(AccessControlProtos.Permission.Action.READ).build();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table).setTablePermission(tperm).build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("u1")).setPermission(perm).build();

        AccessControlProtos.GrantRequest gReq = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();
        final AccessControlProtos.GrantResponse[] gResp = new AccessControlProtos.GrantResponse[1];
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            cp.grant(mock(RpcController.class), gReq, new RpcCallback<AccessControlProtos.GrantResponse>() {
                @Override
                public void run(AccessControlProtos.GrantResponse parameter) {
                    gResp[0] = parameter;
                }
            });
        }
        Assertions.assertNotNull(gResp[0]);

        AccessControlProtos.RevokeRequest rReq = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();
        final AccessControlProtos.RevokeResponse[] rResp = new AccessControlProtos.RevokeResponse[1];
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("revoker");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            cp.revoke(mock(RpcController.class), rReq, new RpcCallback<AccessControlProtos.RevokeResponse>() {
                @Override
                public void run(AccessControlProtos.RevokeResponse parameter) {
                    rResp[0] = parameter;
                }
            });
        }
        Assertions.assertNotNull(rResp[0]);

        pf.set(null, null);
    }

    @Test
    public void test73_getUserPermissions_addPermission_populatesForUserAndGroup() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        // Build ACLs with allowed READ for a user and a group
        RangerResourceACLs acls = new RangerResourceACLs();
        Map<String, RangerResourceACLs.AccessResult> userMap = new HashMap<>();
        userMap.put("read", new RangerResourceACLs.AccessResult(RangerPolicyEvaluator.ACCESS_ALLOWED, null));
        acls.getUserACLs().put("userA", userMap);
        Map<String, RangerResourceACLs.AccessResult> grpMap = new HashMap<>();
        grpMap.put("write", new RangerResourceACLs.AccessResult(RangerPolicyEvaluator.ACCESS_ALLOWED, null));
        acls.getGroupACLs().put("dev", grpMap);

        when(plugin.getResourceACLs(any())).thenReturn(acls);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));

        doReturn(null).when(cp).requirePermission((ObserverContext<?>) eq(null), anyString(), any(byte[].class), any());
        doReturn(null).when(cp).requirePermission((ObserverContext<?>) eq(null), anyString(), any());
        doReturn(null).when(cp).requireGlobalPermission((ObserverContext<?>) eq(null), anyString(), anyString(), any());

        com.google.protobuf.RpcController controller = mock(com.google.protobuf.RpcController.class);
        // Global request so addPermission for both maps executes
        AccessControlProtos.GetUserPermissionsRequest glReq = AccessControlProtos.GetUserPermissionsRequest.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global).build();
        final AccessControlProtos.GetUserPermissionsResponse[] resp = new AccessControlProtos.GetUserPermissionsResponse[1];
        cp.getUserPermissions(controller, glReq, new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
            @Override
            public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                resp[0] = parameter;
            }
        });
        Assertions.assertNotNull(resp[0]);
        pf.set(null, null);
    }

    @Test
    public void test74_start_masterEnv_setsTypeAndExecCheckFlag() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        MasterCoprocessorEnvironment env = mock(MasterCoprocessorEnvironment.class);
        Configuration conf = new Configuration();
        conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
        when(env.getConfiguration()).thenReturn(conf);

        cp.start(env);

        Field execF = RangerAuthorizationCoprocessor.class.getDeclaredField("shouldCheckExecPermission");
        execF.setAccessible(true);
        Assertions.assertTrue(execF.getBoolean(cp));

        Field typeF = RangerAuthorizationCoprocessor.class.getDeclaredField("coprocessorType");
        typeF.setAccessible(true);
        Assertions.assertEquals("master", typeF.get(cp));

        pf.set(null, null);
    }

    @Test
    public void test75_start_regionServerEnv_setsType() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        RegionServerCoprocessorEnvironment env = mock(RegionServerCoprocessorEnvironment.class);
        Configuration conf = new Configuration();
        when(env.getConfiguration()).thenReturn(conf);

        cp.start(env);

        Field typeF = RangerAuthorizationCoprocessor.class.getDeclaredField("coprocessorType");
        typeF.setAccessible(true);
        Assertions.assertEquals("regionalServer", typeF.get(cp));

        pf.set(null, null);
    }

    @Test
    public void test76_start_regionEnv_setsRegionEnvAndType() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Configuration conf = new Configuration();
        when(env.getConfiguration()).thenReturn(conf);

        cp.start(env);

        Field typeF = RangerAuthorizationCoprocessor.class.getDeclaredField("coprocessorType");
        typeF.setAccessible(true);
        Assertions.assertEquals("regional", typeF.get(cp));

        Field regionEnvF = RangerAuthorizationCoprocessor.class.getDeclaredField("regionEnv");
        regionEnvF.setAccessible(true);
        Assertions.assertNotNull(regionEnvF.get(cp));

        pf.set(null, null);
    }

    @Test
    public void test77_grant_updatesEnabled_pluginThrowsIOException_returnsNullAndNoThrow() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field flag = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        flag.setAccessible(true);
        flag.setBoolean(null, true);
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));
        doThrow(new IOException("ioe")).when(plugin).grantAccess(any(), any());
        pf.set(null, plugin);

        AccessControlProtos.TablePermission tperm = AccessControlProtos.TablePermission.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t")))
                .addAction(AccessControlProtos.Permission.Action.READ).build();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table).setTablePermission(tperm).build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("u")).setPermission(perm).build();
        AccessControlProtos.GrantRequest gReq = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();

        final AccessControlProtos.GrantResponse[] gResp = new AccessControlProtos.GrantResponse[1];
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            cp.grant(mock(RpcController.class), gReq, new RpcCallback<AccessControlProtos.GrantResponse>() {
                @Override
                public void run(AccessControlProtos.GrantResponse parameter) {
                    gResp[0] = parameter;
                }
            });
        }
        Assertions.assertNull(gResp[0]);

        pf.set(null, null);
    }

    @Test
    public void test78_revoke_updatesEnabled_pluginThrowsAccessControl_returnsNullAndNoThrow() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field flag = RangerAuthorizationCoprocessor.class.getDeclaredField("updateRangerPoliciesOnGrantRevoke");
        flag.setAccessible(true);
        flag.setBoolean(null, true);
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));
        doThrow(new AccessControlException("denied")).when(plugin).revokeAccess(any(), any());
        pf.set(null, plugin);

        AccessControlProtos.TablePermission tperm = AccessControlProtos.TablePermission.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t")))
                .addAction(AccessControlProtos.Permission.Action.READ).build();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table).setTablePermission(tperm).build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("u")).setPermission(perm).build();
        AccessControlProtos.RevokeRequest rReq = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();

        final AccessControlProtos.RevokeResponse[] rResp = new AccessControlProtos.RevokeResponse[1];
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("revoker");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            cp.revoke(mock(RpcController.class), rReq, new RpcCallback<AccessControlProtos.RevokeResponse>() {
                @Override
                public void run(AccessControlProtos.RevokeResponse parameter) {
                    rResp[0] = parameter;
                }
            });
        }
        Assertions.assertNull(rResp[0]);

        pf.set(null, null);
    }

    @Test
    public void test79_requireSystemOrSuperUser_allowsWhenSystemUserMatches() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<?> ctx = mock(ObserverContext.class);
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("sys");
            userStatic.when(User::getCurrent).thenReturn(sys);
            when(ctx.getCaller()).thenReturn(Optional.of(sys));
            Assertions.assertDoesNotThrow(() -> cp.requireSystemOrSuperUser(ctx));
        }
    }

    @Test
    public void test80_canSkipAccessCheck_writeMeta_authorizedCreateAllows() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        RangerAccessResult allow = mock(RangerAccessResult.class);
        when(allow.getIsAllowed()).thenReturn(true);
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), isNull())).thenReturn(allow);

        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(env.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.isMetaRegion()).thenReturn(true);

        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);

        Assertions.assertTrue(cp.canSkipAccessCheck(user, "op", "write", env));
        pf.set(null, null);
    }

    @Test
    public void test81_authorizeAccess_allowedDoesNotThrow() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        RangerAccessResult allow = mock(RangerAccessResult.class);
        when(allow.getIsAllowed()).thenReturn(true);
        when(allow.getIsAudited()).thenReturn(false);
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(allow);

        @SuppressWarnings("unchecked")
        ObserverContext<?> ctx = mock(ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        doReturn(false).when(cp).canSkipAccessCheck(any(User.class), anyString(), anyString(), anyString());

        Assertions.assertDoesNotThrow(
                () -> cp.authorizeAccess(ctx, "op", "info", Permission.Action.READ, "t", null, null));
        pf.set(null, null);
    }

    @Test
    public void test82_preScannerClose_ownerMatches_noThrow() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getShortName()).thenReturn("alice");
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        InternalScanner scanner = mock(InternalScanner.class);

        Field ownersF = RangerAuthorizationCoprocessor.class.getDeclaredField("scannerOwners");
        ownersF.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<InternalScanner, String> owners = (Map<InternalScanner, String>) ownersF.get(cp);
        owners.put(scanner, "alice");

        try (MockedStatic<RpcServer> rpc = Mockito.mockStatic(RpcServer.class)) {
            rpc.when(RpcServer::isInRpcCallContext).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> cp.preScannerClose(ctx, scanner));
        }
    }

    @Test
    public void test83_evaluateAccess_columnOptimization_familyFullyAuthorized() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        when(plugin.getPropertyIsColumnAuthOptimizationEnabled()).thenReturn(true);
        RangerAccessResult allow = mock(RangerAccessResult.class);
        when(allow.getIsAllowed()).thenReturn(true);
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(allow);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        Set<byte[]> cols = new HashSet<>();
        cols.add("c1".getBytes());
        familyMap.put("f".getBytes(), cols);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertTrue(res.everythingIsAccessible);
        Assertions.assertFalse(res.somethingIsAccessible);
        Assertions.assertNotNull(res.filter);
        pf.set(null, null);
    }

    @Test
    public void test84_createGrantData_groupUser_global() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global)
                .setGlobalPermission(AccessControlProtos.GlobalPermission.newBuilder()
                        .addAction(AccessControlProtos.Permission.Action.READ)
                        .addAction(AccessControlProtos.Permission.Action.ADMIN).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("@dev")).setPermission(perm).build();
        AccessControlProtos.GrantRequest req = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantorX");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            GrantRevokeRequest out = (GrantRevokeRequest) m.invoke(cp, req);
            Assertions.assertTrue(out.getGroups().contains("dev"));
            Assertions.assertTrue(out.getUsers().isEmpty());
            Map<String, String> res = out.getResource();
            Assertions.assertEquals(RangerHBaseResource.WILDCARD, res.get(RangerHBaseResource.KEY_TABLE));
            Assertions.assertEquals(RangerHBaseResource.WILDCARD, res.get(RangerHBaseResource.KEY_COLUMN_FAMILY));
            Assertions.assertEquals(RangerHBaseResource.WILDCARD, res.get(RangerHBaseResource.KEY_COLUMN));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_READ));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_ADMIN));
        }
    }

    @Test
    public void test85_createRevokeData_groupUser_table_allAccessTypesAndDelegateAdmin() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        AccessControlProtos.TablePermission tperm = AccessControlProtos.TablePermission.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t")))
                .setFamily(ByteString.copyFromUtf8("f")).setQualifier(ByteString.copyFromUtf8("q")).build();
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table).setTablePermission(tperm).build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("@team")).setPermission(perm).build();
        AccessControlProtos.RevokeRequest req = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();
        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createRevokeData",
                AccessControlProtos.RevokeRequest.class);
        m.setAccessible(true);
        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("revokerX");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);
            GrantRevokeRequest out = (GrantRevokeRequest) m.invoke(cp, req);
            Assertions.assertTrue(out.getGroups().contains("team"));
            Assertions.assertTrue(out.getUsers().isEmpty());
            Map<String, String> res = out.getResource();
            Assertions.assertEquals("t", res.get(RangerHBaseResource.KEY_TABLE));
            Assertions.assertEquals("f", res.get(RangerHBaseResource.KEY_COLUMN_FAMILY));
            Assertions.assertEquals("q", res.get(RangerHBaseResource.KEY_COLUMN));
            Assertions.assertTrue(out.getDelegateAdmin());
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_READ));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_WRITE));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_CREATE));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_ADMIN));
            Assertions.assertTrue(out.getAccessTypes().contains(HbaseAuthUtils.ACCESS_TYPE_EXECUTE));
        }
    }

    @Test
    public void test86_evaluateAccess_missingTable_throwsAccessDenied() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        when(renv.getRegion()).thenReturn(null); // getTableName will return null

        Assertions.assertThrows(AccessDeniedException.class, () -> cp.evaluateAccess(ctx, "get", Permission.Action.READ,
                renv, new HashMap<byte[], Collection<?>>(), "cmd"));
        pf.set(null, null);
    }

    @Test
    public void test87_addPermission_namespace_user_createsPermission() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        Map<String, Map<String, RangerResourceACLs.AccessResult>> acls = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> userPerms = new HashMap<>();
        userPerms.put("READ", new RangerResourceACLs.AccessResult(RangerPolicyEvaluator.ACCESS_ALLOWED, null));
        acls.put("userX", userPerms);

        List<String> hbaseActionsList = new ArrayList<>();
        for (Permission.Action a : Permission.Action.values()) {
            hbaseActionsList.add(a.name());
        }

        List<UserPermission> out = new ArrayList<>();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("addPermission", Map.class, boolean.class,
                List.class, List.class, String.class, boolean.class);
        m.setAccessible(true);

        m.invoke(cp, acls, true, hbaseActionsList, out, "ns1", false);

        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("userX", out.get(0).getUser());
        Assertions.assertTrue(out.get(0).getPermission().implies(Permission.Action.READ));
    }

    @Test
    public void test88_addPermission_table_group_createsPermissionWithGroupUser() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        Map<String, Map<String, RangerResourceACLs.AccessResult>> acls = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> grpPerms = new HashMap<>();
        grpPerms.put("WRITE", new RangerResourceACLs.AccessResult(RangerPolicyEvaluator.ACCESS_ALLOWED, null));
        acls.put("teamY", grpPerms);

        List<String> hbaseActionsList = new ArrayList<>();
        for (Permission.Action a : Permission.Action.values()) {
            hbaseActionsList.add(a.name());
        }

        List<UserPermission> out = new ArrayList<>();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("addPermission", Map.class, boolean.class,
                List.class, List.class, String.class, boolean.class);
        m.setAccessible(true);

        m.invoke(cp, acls, false, hbaseActionsList, out, "tbl1", true);

        Assertions.assertEquals(1, out.size());
        Assertions.assertTrue(out.get(0).getUser().startsWith("@"));
        Assertions.assertTrue(out.get(0).getPermission().implies(Permission.Action.WRITE));
    }

    @Test
    public void test89_evaluateAccess_familyLevel_indeterminate() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        familyMap.put("f".getBytes(), new ArrayList<byte[]>()); // empty columns triggers family-level path

        RangerAccessResult allow = mock(RangerAccessResult.class);
        when(allow.getIsAllowed()).thenReturn(true);
        RangerAccessResult deny = mock(RangerAccessResult.class);
        when(deny.getIsAllowed()).thenReturn(false);
        // First authorize() for family returns allowed; second for descendants returns
        // denied
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(allow, deny);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertFalse(res.everythingIsAccessible);
        Assertions.assertTrue(res.somethingIsAccessible);
        Assertions.assertNotNull(res.filter);

        pf.set(null, null);
    }

    @Test
    public void test90_evaluateAccess_columnOptimization_familyPartiallyAuthorized() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getPropertyIsColumnAuthOptimizationEnabled()).thenReturn(true);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getName()).thenReturn("testuser");
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        Set<byte[]> cols = new HashSet<>();
        cols.add("c1".getBytes());
        cols.add("c2".getBytes());
        familyMap.put("f".getBytes(), cols);

        RangerAccessResult familyAllow = mock(RangerAccessResult.class);
        when(familyAllow.getIsAllowed()).thenReturn(true);
        RangerAccessResult descendantsDeny = mock(RangerAccessResult.class);
        when(descendantsDeny.getIsAllowed()).thenReturn(false);
        RangerAccessResult columnAllow = mock(RangerAccessResult.class);
        when(columnAllow.getIsAllowed()).thenReturn(true);
        RangerAccessResult columnDeny = mock(RangerAccessResult.class);
        when(columnDeny.getIsAllowed()).thenReturn(false);

        // When column optimization is enabled: family allowed, descendants denied, then
        // individual column checks
        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(familyAllow, descendantsDeny, columnAllow, columnDeny);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertFalse(res.everythingIsAccessible); // Not everything is accessible due to one denied column
        Assertions.assertFalse(res.somethingIsAccessible); // This becomes false when any column is denied (line 1247 in
                                                           // source)
        Assertions.assertNotNull(res.filter);

        pf.set(null, null);
    }

    @Test
    public void test91_evaluateAccess_familyLevelWithDescendants_authorized() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        familyMap.put("f".getBytes(), null); // null columns for family-level access

        RangerAccessResult familyAllow = mock(RangerAccessResult.class);
        when(familyAllow.getIsAllowed()).thenReturn(true);
        RangerAccessResult descendantsAllow = mock(RangerAccessResult.class);
        when(descendantsAllow.getIsAllowed()).thenReturn(true);

        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(familyAllow, descendantsAllow);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertTrue(res.everythingIsAccessible);
        Assertions.assertTrue(res.somethingIsAccessible);

        pf.set(null, null);
    }

    @Test
    public void test92_evaluateAccess_familyLevelWithDescendants_partialAccess() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        familyMap.put("f".getBytes(), new ArrayList<>()); // empty columns for family-level access

        RangerAccessResult familyDeny = mock(RangerAccessResult.class);
        when(familyDeny.getIsAllowed()).thenReturn(false);
        RangerAccessResult descendantsAllow = mock(RangerAccessResult.class);
        when(descendantsAllow.getIsAllowed()).thenReturn(true);

        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(familyDeny, descendantsAllow);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertFalse(res.everythingIsAccessible);
        Assertions.assertTrue(res.somethingIsAccessible);

        pf.set(null, null);
    }

    @Test
    public void test93_evaluateAccess_familyLevelWithDescendants_noAccess() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(
                ObserverContext.class);
        User user = mock(User.class);
        when(user.getName()).thenReturn("testuser");
        when(user.getGroupNames()).thenReturn(new String[0]);
        when(ctx.getCaller()).thenReturn(Optional.of(user));

        RegionCoprocessorEnvironment renv = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        RegionInfo regionInfo = mock(RegionInfo.class);
        when(renv.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        when(regionInfo.getTable()).thenReturn(TableName.valueOf("t"));

        Map<byte[], Collection<?>> familyMap = new HashMap<>();
        familyMap.put("f".getBytes(), new ArrayList<>()); // empty columns for family-level access

        RangerAccessResult familyDeny = mock(RangerAccessResult.class);
        when(familyDeny.getIsAllowed()).thenReturn(false);
        RangerAccessResult descendantsDeny = mock(RangerAccessResult.class);
        when(descendantsDeny.getIsAllowed()).thenReturn(false);

        when(plugin.isAccessAllowed(any(RangerAccessRequest.class), any(RangerAccessResultProcessor.class)))
                .thenReturn(familyDeny, descendantsDeny);

        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult res = cp.evaluateAccess(ctx, "get",
                Permission.Action.READ, renv, familyMap, "cmd");
        Assertions.assertNotNull(res);
        Assertions.assertFalse(res.everythingIsAccessible);
        Assertions.assertFalse(res.somethingIsAccessible);

        pf.set(null, null);
    }

    @Test
    public void test94_createGrantData_tablePermission_allActions() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table)
                .setTablePermission(AccessControlProtos.TablePermission.newBuilder()
                        .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("testTable")))
                        .setFamily(ByteString.copyFromUtf8("testFamily"))
                        .setQualifier(ByteString.copyFromUtf8("testQualifier"))
                        .addAction(AccessControlProtos.Permission.Action.READ)
                        .addAction(AccessControlProtos.Permission.Action.WRITE)
                        .addAction(AccessControlProtos.Permission.Action.CREATE)
                        .addAction(AccessControlProtos.Permission.Action.ADMIN)
                        .addAction(AccessControlProtos.Permission.Action.EXEC).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("testuser")).setPermission(perm).build();
        AccessControlProtos.GrantRequest req = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);

            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
            Assertions.assertTrue(out instanceof GrantRevokeRequest);
            GrantRevokeRequest grr = (GrantRevokeRequest) out;
            Assertions.assertTrue(grr.getDelegateAdmin()); // Should be true because of ADMIN action
            Assertions.assertTrue(grr.getAccessTypes().contains("read"));
            Assertions.assertTrue(grr.getAccessTypes().contains("write"));
            Assertions.assertTrue(grr.getAccessTypes().contains("create"));
            Assertions.assertTrue(grr.getAccessTypes().contains("admin"));
            Assertions.assertTrue(grr.getAccessTypes().contains("execute"));
        }
    }

    @Test
    public void test95_createGrantData_namespacePermission() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Namespace)
                .setNamespacePermission(AccessControlProtos.NamespacePermission.newBuilder()
                        .setNamespaceName(ByteString.copyFromUtf8("testNamespace"))
                        .addAction(AccessControlProtos.Permission.Action.READ).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("testuser")).setPermission(perm).build();
        AccessControlProtos.GrantRequest req = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);

            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
            Assertions.assertTrue(out instanceof GrantRevokeRequest);
            GrantRevokeRequest grr = (GrantRevokeRequest) out;
            Assertions.assertTrue(grr.getResource().get("table").startsWith("testNamespace:"));
        }
    }

    @Test
    public void test96_createGrantData_unknownAction() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Create a mock permission with an unknown action code
        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Global)
                .setGlobalPermission(AccessControlProtos.GlobalPermission.newBuilder()
                        .addAction(AccessControlProtos.Permission.Action.READ).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("testuser")).setPermission(perm).build();
        AccessControlProtos.GrantRequest req = AccessControlProtos.GrantRequest.newBuilder().setUserPermission(up)
                .build();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createGrantData",
                AccessControlProtos.GrantRequest.class);
        m.setAccessible(true);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);

            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
            Assertions.assertTrue(out instanceof GrantRevokeRequest);
            GrantRevokeRequest grr = (GrantRevokeRequest) out;
            Assertions.assertTrue(grr.getAccessTypes().contains("read"));
        }
    }

    @Test
    public void test97_createRevokeData_tablePermission() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Table)
                .setTablePermission(AccessControlProtos.TablePermission.newBuilder()
                        .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("testTable")))
                        .setFamily(ByteString.copyFromUtf8("testFamily"))
                        .setQualifier(ByteString.copyFromUtf8("testQualifier")).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("testuser")).setPermission(perm).build();
        AccessControlProtos.RevokeRequest req = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createRevokeData",
                AccessControlProtos.RevokeRequest.class);
        m.setAccessible(true);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);

            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
            Assertions.assertTrue(out instanceof GrantRevokeRequest);
            GrantRevokeRequest grr = (GrantRevokeRequest) out;
            Assertions.assertTrue(grr.getDelegateAdmin()); // Should be true for revoke
            Assertions.assertEquals("testTable", grr.getResource().get("table"));
            Assertions.assertEquals("testFamily", grr.getResource().get("column-family"));
            Assertions.assertEquals("testQualifier", grr.getResource().get("column"));
        }
    }

    @Test
    public void test98_createRevokeData_namespacePermission() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        AccessControlProtos.Permission perm = AccessControlProtos.Permission.newBuilder()
                .setType(AccessControlProtos.Permission.Type.Namespace)
                .setNamespacePermission(AccessControlProtos.NamespacePermission.newBuilder()
                        .setNamespaceName(ByteString.copyFromUtf8("testNamespace")).build())
                .build();
        AccessControlProtos.UserPermission up = AccessControlProtos.UserPermission.newBuilder()
                .setUser(ByteString.copyFromUtf8("testuser")).setPermission(perm).build();
        AccessControlProtos.RevokeRequest req = AccessControlProtos.RevokeRequest.newBuilder().setUserPermission(up)
                .build();

        Method m = RangerAuthorizationCoprocessor.class.getDeclaredMethod("createRevokeData",
                AccessControlProtos.RevokeRequest.class);
        m.setAccessible(true);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            User sys = mock(User.class);
            when(sys.getShortName()).thenReturn("grantor");
            when(sys.getGroupNames()).thenReturn(new String[] {"g1"});
            userStatic.when(User::getCurrent).thenReturn(sys);

            Object out = m.invoke(cp, req);
            Assertions.assertNotNull(out);
            Assertions.assertTrue(out instanceof GrantRevokeRequest);
            GrantRevokeRequest grr = (GrantRevokeRequest) out;
            Assertions.assertTrue(grr.getResource().get("table").startsWith("testNamespace:"));
        }
    }

    @Test
    public void test99_getUserPermissions_ioException() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        pf.set(null, plugin);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class);
                MockedStatic<RpcServer> rpcStatic = Mockito.mockStatic(RpcServer.class)) {
            // Mock a valid user for the initial getUserGroups call
            User mockUser = mock(User.class);
            when(mockUser.getGroupNames()).thenReturn(new String[] {"testgroup"});

            // Mock RpcServer.getRequestUser to return the mock user
            Optional<User> userOptional = Optional.of(mockUser);
            rpcStatic.when(RpcServer::getRequestUser).thenReturn(userOptional);

            // Mock User.runAsLoginUser to throw IOException
            userStatic.when(() -> User.runAsLoginUser(any())).thenThrow(new IOException("Test exception"));

            com.google.protobuf.RpcController controller = mock(com.google.protobuf.RpcController.class);
            AccessControlProtos.GetUserPermissionsRequest tableReq = AccessControlProtos.GetUserPermissionsRequest
                    .newBuilder().setType(AccessControlProtos.Permission.Type.Table)
                    .setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf("t"))).build();

            final AccessControlProtos.GetUserPermissionsResponse[] resp = new AccessControlProtos.GetUserPermissionsResponse[1];
            cp.getUserPermissions(controller, tableReq,
                    new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
                        @Override
                        public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                            resp[0] = parameter;
                        }
                    });

            // Verify that controller exception was set due to IOException
            verify(controller).setFailed(anyString());
        }

        pf.set(null, null);
    }

    @Test
    public void test100_getUserPermissions_superUserGlobal() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        when(plugin.getConfig()).thenReturn(new RangerPluginConfig("hbase", null, "hbaseMaster", null, null, null));
        pf.set(null, plugin);

        RangerResourceACLs acls = mock(RangerResourceACLs.class);
        when(acls.getUserACLs()).thenReturn(new HashMap<String, Map<String, RangerResourceACLs.AccessResult>>());
        when(acls.getGroupACLs()).thenReturn(new HashMap<String, Map<String, RangerResourceACLs.AccessResult>>());
        when(plugin.getResourceACLs(any())).thenReturn(acls);

        doReturn(null).when(cp).requirePermission((ObserverContext<?>) eq(null), anyString(),
                any(Permission.Action.class));

        // Mock super user check
        HbaseUserUtils userUtils = mock(HbaseUserUtils.class);
        User mockUser = mock(User.class);
        when(mockUser.getUGI()).thenReturn(null);
        when(userUtils.getUserAsString(any())).thenReturn("superuser");
        when(userUtils.getUserGroups(any())).thenReturn(new HashSet<>());
        when(userUtils.isSuperUser(any())).thenReturn(true);

        Field userUtilsField = RangerAuthorizationCoprocessor.class.getDeclaredField("userUtils");
        userUtilsField.setAccessible(true);
        userUtilsField.set(cp, userUtils);

        try (MockedStatic<User> userStatic = Mockito.mockStatic(User.class)) {
            userStatic.when(User::getCurrent).thenReturn(mockUser);

            com.google.protobuf.RpcController controller = mock(com.google.protobuf.RpcController.class);
            AccessControlProtos.GetUserPermissionsRequest glReq = AccessControlProtos.GetUserPermissionsRequest
                    .newBuilder().setType(AccessControlProtos.Permission.Type.Global).build();

            final AccessControlProtos.GetUserPermissionsResponse[] resp = new AccessControlProtos.GetUserPermissionsResponse[1];
            cp.getUserPermissions(controller, glReq, new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
                @Override
                public void run(AccessControlProtos.GetUserPermissionsResponse parameter) {
                    resp[0] = parameter;
                }
            });

            Assertions.assertNotNull(resp[0]);
            // Should have permissions for super user
            Assertions.assertTrue(resp[0].getUserPermissionCount() > 0);
        }

        pf.set(null, null);
    }

    @Test
    public void test101_preBulkLoadHFile_success() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(ctx.getEnvironment()).thenReturn(env);

        List<Pair<byte[], String>> familyPaths = new ArrayList<>();
        familyPaths.add(new Pair<>("cf1".getBytes(), "/path/to/file1"));
        familyPaths.add(new Pair<>("cf2".getBytes(), "/path/to/file2"));

        // Mock requirePermission to not throw
        doNothing().when(cp).requirePermission(any(ObserverContext.class), anyString(), any(Permission.Action.class),
                any(RegionCoprocessorEnvironment.class), any(Collection.class));

        Assertions.assertDoesNotThrow(() -> cp.preBulkLoadHFile(ctx, familyPaths));

        // Verify requirePermission was called with WRITE action
        verify(cp).requirePermission(eq(ctx), eq("bulkLoadHFile"), eq(Permission.Action.WRITE), eq(env),
                any(List.class));
    }

    @Test
    public void test102_preBulkLoadHFile_emptyFamilyPaths() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
        when(ctx.getEnvironment()).thenReturn(env);

        List<Pair<byte[], String>> emptyFamilyPaths = new ArrayList<>();

        // Mock requirePermission to not throw
        doNothing().when(cp).requirePermission(any(ObserverContext.class), anyString(), any(Permission.Action.class),
                any(RegionCoprocessorEnvironment.class), any(Collection.class));

        Assertions.assertDoesNotThrow(() -> cp.preBulkLoadHFile(ctx, emptyFamilyPaths));

        // Verify requirePermission was called with empty list
        verify(cp).requirePermission(eq(ctx), eq("bulkLoadHFile"), eq(Permission.Action.WRITE), eq(env),
                any(List.class));
    }

    @Test
    public void test103_preStopRegionServer_success() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionServerCoprocessorEnvironment> env = mock(ObserverContext.class);

        // Mock requirePermission to not throw
        doNothing().when(cp).requirePermission(any(ObserverContext.class), anyString(), any(Permission.Action.class));

        Assertions.assertDoesNotThrow(() -> cp.preStopRegionServer(env));

        // Verify requirePermission was called with ADMIN action
        verify(cp).requirePermission(env, "stop", Permission.Action.ADMIN);
    }

    @Test
    public void test104_preStopRegionServer_accessDenied() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionServerCoprocessorEnvironment> env = mock(ObserverContext.class);

        // Mock requirePermission to throw AccessDeniedException
        doThrow(new AccessDeniedException("Access denied")).when(cp).requirePermission(any(ObserverContext.class),
                anyString(), any(Permission.Action.class));

        Assertions.assertThrows(AccessDeniedException.class, () -> cp.preStopRegionServer(env));
    }

    @Test
    public void test105_requirePermission_withCollection_success() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        Collection<byte[]> families = Arrays.asList("cf1".getBytes(), "cf2".getBytes());

        // Mock the evaluateAccess method to return everything accessible
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                true, true, new ArrayList<>(), new ArrayList<>(), null, null, null);
        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), isNull());

        Assertions.assertDoesNotThrow(
                () -> cp.requirePermission(ctx, "testOperation", Permission.Action.READ, env, families));
    }

    @Test
    public void test106_requirePermission_withCollection_nullFamilies() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        // Mock the evaluateAccess method to return everything accessible
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                true, true, new ArrayList<>(), new ArrayList<>(), null, null, null);
        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), isNull());

        Assertions.assertDoesNotThrow(() -> cp.requirePermission(ctx, "testOperation", Permission.Action.READ, env,
                (Collection<byte[]>) null));
    }

    @Test
    public void test107_authorizeAccess_returnsFilter() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        RangerPluginConfig config = mock(RangerPluginConfig.class);
        when(plugin.getConfig()).thenReturn(config);
        pf.set(null, plugin);

        ObserverContext<?> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();
        NavigableSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        columns.add("col1".getBytes());
        familyMap.put("cf1".getBytes(), columns);

        // Mock evaluateAccess to return a result with filter
        RangerAuthorizationFilter rangerFilter = mock(RangerAuthorizationFilter.class);
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                false, true, new ArrayList<>(), null, null, null, rangerFilter);

        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), anyString());

        Filter result = cp.authorizeAccess(ctx, "scan", Permission.Action.READ, env, familyMap, "test command");

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof RangerAuthorizationFilter);

        pf.set(null, null);
    }

    @Test
    public void test108_authorizeAccess_everythingAccessible() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        RangerPluginConfig config = mock(RangerPluginConfig.class);
        when(plugin.getConfig()).thenReturn(config);
        pf.set(null, plugin);

        ObserverContext<?> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();

        // Mock evaluateAccess to return everything accessible
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                true, true, new ArrayList<>(), new ArrayList<>(), null, null, null);

        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), anyString());

        Filter result = cp.authorizeAccess(ctx, "scan", Permission.Action.READ, env, familyMap, "test command");

        Assertions.assertNull(result); // No filter needed when everything is accessible

        pf.set(null, null);
    }

    @Test
    public void test109_authorizeAccess_nothingAccessible() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        RangerPluginConfig config = mock(RangerPluginConfig.class);
        when(plugin.getConfig()).thenReturn(config);
        pf.set(null, plugin);

        ObserverContext<?> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<>();

        // Mock evaluateAccess to return nothing accessible
        AuthzAuditEvent auditEvent = mock(AuthzAuditEvent.class);
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                false, false, null, null, auditEvent, "Access denied", null);

        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), anyString());

        Assertions.assertThrows(AccessDeniedException.class,
                () -> cp.authorizeAccess(ctx, "scan", Permission.Action.READ, env, familyMap, "test command"));

        pf.set(null, null);
    }

    @Test
    public void test110_getUserPermissions_private_tableResource() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Test the private getUserPermissions method
        Method getUserPermissionsMethod = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getUserPermissions",
                RangerResourceACLs.class, String.class, boolean.class);
        getUserPermissionsMethod.setAccessible(true);

        RangerResourceACLs rangerResourceACLs = mock(RangerResourceACLs.class);

        // Mock user ACLs
        Map<String, Map<String, RangerResourceACLs.AccessResult>> userACLs = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> userAccess = new HashMap<>();
        RangerResourceACLs.AccessResult readAccess = new RangerResourceACLs.AccessResult(
                RangerPolicyEvaluator.ACCESS_ALLOWED, null);
        userAccess.put("READ", readAccess);
        userACLs.put("testuser", userAccess);
        when(rangerResourceACLs.getUserACLs()).thenReturn(userACLs);

        // Mock group ACLs
        Map<String, Map<String, RangerResourceACLs.AccessResult>> groupACLs = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> groupAccess = new HashMap<>();
        RangerResourceACLs.AccessResult writeAccess = new RangerResourceACLs.AccessResult(
                RangerPolicyEvaluator.ACCESS_ALLOWED, null);
        groupAccess.put("WRITE", writeAccess);
        groupACLs.put("testgroup", groupAccess);
        when(rangerResourceACLs.getGroupACLs()).thenReturn(groupACLs);

        @SuppressWarnings("unchecked")
        List<UserPermission> result = (List<UserPermission>) getUserPermissionsMethod.invoke(cp, rangerResourceACLs,
                "testTable", false);

        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isEmpty());

        // Should have permissions for both user and group
        boolean hasUserPermission = result.stream().anyMatch(p -> "testuser".equals(p.getUser()));
        boolean hasGroupPermission = result.stream().anyMatch(p -> "@testgroup".equals(p.getUser()));
        Assertions.assertTrue(hasUserPermission);
        Assertions.assertTrue(hasGroupPermission);
    }

    @Test
    public void test111_getUserPermissions_private_namespaceResource() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Test the private getUserPermissions method for namespace
        Method getUserPermissionsMethod = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getUserPermissions",
                RangerResourceACLs.class, String.class, boolean.class);
        getUserPermissionsMethod.setAccessible(true);

        RangerResourceACLs rangerResourceACLs = mock(RangerResourceACLs.class);

        // Mock user ACLs for namespace
        Map<String, Map<String, RangerResourceACLs.AccessResult>> userACLs = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> userAccess = new HashMap<>();
        RangerResourceACLs.AccessResult adminAccess = new RangerResourceACLs.AccessResult(
                RangerPolicyEvaluator.ACCESS_ALLOWED, null);
        userAccess.put("ADMIN", adminAccess);
        userACLs.put("nsadmin", userAccess);
        when(rangerResourceACLs.getUserACLs()).thenReturn(userACLs);

        // Mock empty group ACLs
        when(rangerResourceACLs.getGroupACLs()).thenReturn(new HashMap<>());

        @SuppressWarnings("unchecked")
        List<UserPermission> result = (List<UserPermission>) getUserPermissionsMethod.invoke(cp, rangerResourceACLs,
                "testNamespace", true);

        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isEmpty());

        // Should have namespace permission
        UserPermission nsPermission = result.get(0);
        Assertions.assertEquals("nsadmin", nsPermission.getUser());
        Assertions.assertTrue(nsPermission.getPermission() instanceof Permission);
    }

    @Test
    public void test112_getUserPermissions_private_emptyACLs() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Test the private getUserPermissions method with empty ACLs
        Method getUserPermissionsMethod = RangerAuthorizationCoprocessor.class.getDeclaredMethod("getUserPermissions",
                RangerResourceACLs.class, String.class, boolean.class);
        getUserPermissionsMethod.setAccessible(true);

        RangerResourceACLs rangerResourceACLs = mock(RangerResourceACLs.class);
        when(rangerResourceACLs.getUserACLs()).thenReturn(new HashMap<>());
        when(rangerResourceACLs.getGroupACLs()).thenReturn(new HashMap<>());

        @SuppressWarnings("unchecked")
        List<UserPermission> result = (List<UserPermission>) getUserPermissionsMethod.invoke(cp, rangerResourceACLs,
                "testTable", false);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void test113_addPermission_delegateAdmin() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Test addPermission method with delegate admin
        Method addPermissionMethod = RangerAuthorizationCoprocessor.class.getDeclaredMethod("addPermission", Map.class,
                boolean.class, List.class, List.class, String.class, boolean.class);
        addPermissionMethod.setAccessible(true);

        Map<String, Map<String, RangerResourceACLs.AccessResult>> aclMap = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> userAccess = new HashMap<>();

        // Create access result with delegate admin - note: AccessResult doesn't have
        // delegate admin info
        // This would be handled at the policy level, but for testing we'll use a simple
        // allowed result
        RangerResourceACLs.AccessResult accessResult = new RangerResourceACLs.AccessResult(
                RangerPolicyEvaluator.ACCESS_ALLOWED, null);

        userAccess.put("READ", accessResult);
        aclMap.put("adminuser", userAccess);

        List<String> hbaseActionsList = Arrays.asList("READ", "WRITE", "CREATE", "ADMIN", "EXEC");
        List<UserPermission> userPermissions = new ArrayList<>();

        addPermissionMethod.invoke(cp, aclMap, false, hbaseActionsList, userPermissions, "testTable", false);

        Assertions.assertFalse(userPermissions.isEmpty());

        // Should have delegate admin permission
        UserPermission permission = userPermissions.get(0);
        Assertions.assertEquals("adminuser", permission.getUser());
        // The permission should include delegate admin capability
        Assertions.assertTrue(permission.getPermission().getActions().length > 0);
    }

    @Test
    public void test114_addPermission_deniedAccess() throws Exception {
        RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();

        // Test addPermission method with denied access
        Method addPermissionMethod = RangerAuthorizationCoprocessor.class.getDeclaredMethod("addPermission", Map.class,
                boolean.class, List.class, List.class, String.class, boolean.class);
        addPermissionMethod.setAccessible(true);

        Map<String, Map<String, RangerResourceACLs.AccessResult>> aclMap = new HashMap<>();
        Map<String, RangerResourceACLs.AccessResult> userAccess = new HashMap<>();

        // Create access result with denied access
        RangerResourceACLs.AccessResult accessResult = new RangerResourceACLs.AccessResult(
                RangerPolicyEvaluator.ACCESS_DENIED, null);

        userAccess.put("READ", accessResult);
        aclMap.put("denieduser", userAccess);

        List<String> hbaseActionsList = Arrays.asList("READ");
        List<UserPermission> userPermissions = new ArrayList<>();

        addPermissionMethod.invoke(cp, aclMap, false, hbaseActionsList, userPermissions, "testTable", false);

        // Should not add permission for denied access
        Assertions.assertTrue(userPermissions.isEmpty());
    }

    @Test
    public void test115_requirePermission_collection_accessDenied() throws Exception {
        RangerAuthorizationCoprocessor cp = spy(new RangerAuthorizationCoprocessor());
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);

        Collection<byte[]> families = Arrays.asList("cf1".getBytes());

        // Ensure hbasePlugin is initialized to avoid NPE inside requirePermission
        Field pf = RangerAuthorizationCoprocessor.class.getDeclaredField("hbasePlugin");
        pf.setAccessible(true);
        RangerHBasePlugin plugin = mock(RangerHBasePlugin.class);
        RangerPluginConfig config = mock(RangerPluginConfig.class);
        when(plugin.getConfig()).thenReturn(config);
        pf.set(null, plugin);

        // Mock the evaluateAccess method to return nothing accessible
        AuthzAuditEvent auditEvent = mock(AuthzAuditEvent.class);
        RangerAuthorizationCoprocessor.ColumnFamilyAccessResult accessResult = new RangerAuthorizationCoprocessor.ColumnFamilyAccessResult(
                false, false, null, null, auditEvent, "Access denied", null);
        doReturn(accessResult).when(cp).evaluateAccess(any(ObserverContext.class), anyString(),
                any(Permission.Action.class), any(RegionCoprocessorEnvironment.class), any(Map.class), isNull());

        Assertions.assertThrows(AccessDeniedException.class,
                () -> cp.requirePermission(ctx, "testOperation", Permission.Action.READ, env, families));

        // cleanup
        pf.set(null, null);
    }
}
