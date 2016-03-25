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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

import org.apache.ranger.plugin.util.RangerRequestedResources;

public class RangerHiveMetastoreAuthorizer extends MetaStorePreEventListener {
    private static final Log LOG = LogFactory.getLog(RangerHiveMetastoreAuthorizer.class);

    private static volatile RangerHivePlugin hivePlugin = null;

    private String appType = "unknown";

    public RangerHiveMetastoreAuthorizer(Configuration config) {
        super(config);

        LOG.debug("RangerHiveMetastoreAuthorizer()");
    }


    /**
     * Check privileges on Metastore preevents
     *
     * @param context the pre-event context
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws InvalidOperationException
     */
    @Override
    public void onEvent(PreEventContext context)
            throws MetaException, NoSuchObjectException, InvalidOperationException {
        RangerHivePlugin plugin = hivePlugin;
        UserGroupInformation ugi;
        SessionState ss = null;
        HiveAuthzSessionContext.CLIENT_TYPE ctype = null;
        if (plugin == null) {
            ss = SessionState.get();
            synchronized (RangerHiveMetastoreAuthorizer.class) {
                plugin = hivePlugin;
                 if (plugin == null) {
                     if (ss != null) {
                         if (ss.isHiveServerQuery()) {
                             appType = "hiveServer2 - MetaStore";
                             ctype = HiveAuthzSessionContext.CLIENT_TYPE.HIVESERVER2;
                         } else {
                             appType = "hiveCLI - MetaStore";
                             ctype = HiveAuthzSessionContext.CLIENT_TYPE.HIVECLI;
                         }
                     }
                     plugin = new RangerHivePlugin(appType);
                     plugin.init();
                     hivePlugin = plugin;
                }
            }
        }
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();
        RangerAccessResult result = null;
        RangerHiveResource resource = null;
        RangerHiveAccessRequest request = null;
        String user = null;
        try {
            if (ss == null) ss = SessionState.get();
            String hoptName = null;
            HiveAccessType hact = null;
            HiveAuthzContext.Builder authbuilder = new HiveAuthzContext.Builder();
            if (ss != null) {
                authbuilder.setCommandString(ss.getLastCommand());
                authbuilder.setUserIpAddress(ss.getUserIpAddress());
                user = ss.getUserName();
                if (user == null)
                    user = ss.getAuthenticator().getUserName();
            }
            HiveAuthzContext authzContext = authbuilder.build();
            HiveAuthzSessionContext.Builder authssbuilder = new HiveAuthzSessionContext.Builder();
            authssbuilder.setClientType(ctype);
            HiveAuthzSessionContext sessionContext = authssbuilder.build();
            RangerRequestedResources requestedResources = new RangerRequestedResources();
            Table table;
            switch (context.getEventType()) {
                case CREATE_TABLE:
                    hoptName = HiveOperationType.CREATETABLE.name();
                    hact = HiveAccessType.CREATE;
                    table = ((PreCreateTableEvent) context).getTable();
                    resource = new RangerHiveResource(HiveObjectType.TABLE, table.getDbName(), table.getTableName());
                    break;
                case DROP_TABLE:
                    hoptName = HiveOperationType.DROPTABLE.name();
                    hact = HiveAccessType.DROP;
                    table = ((PreDropTableEvent) context).getTable();
                    resource = new RangerHiveResource(HiveObjectType.TABLE, table.getDbName(), table.getTableName());
                    break;
                case ALTER_TABLE:
                    hoptName = "ALTERTABLE";
                    hact = HiveAccessType.ALTER;
                    table = ((PreAlterTableEvent) context).getOldTable();
                    resource = new RangerHiveResource(HiveObjectType.TABLE, table.getDbName(), table.getTableName());
                    break;
                case READ_TABLE:
                    hoptName = HiveOperationType.QUERY.name();
                    hact = HiveAccessType.SELECT;
                    table = ((PreReadTableEvent) context).getTable();
                    resource = new RangerHiveResource(HiveObjectType.TABLE, table.getDbName(), table.getTableName());
                    break;
                case READ_DATABASE:
                    hoptName = HiveOperationType.QUERY.name();
                    hact = HiveAccessType.SELECT;
                    Database db = ((PreReadDatabaseEvent) context).getDatabase();
                    resource = new RangerHiveResource(HiveObjectType.DATABASE, db.getName());
                    break;
                case ADD_PARTITION:
                    hoptName = "ADDPARTITION";
                    hact = HiveAccessType.ALTER;
                    table = ((PreAddPartitionEvent) context).getTable();
                    resource = new RangerHiveResource(HiveObjectType.PARTITION, table.getDbName(), table.getTableName());
                    break;
                case DROP_PARTITION:
                    hoptName = "DROPPARTITION";
                    hact = HiveAccessType.ALTER;
                    table = ((PreDropPartitionEvent) context).getTable();
                    resource = new RangerHiveResource(HiveObjectType.PARTITION, table.getDbName(), table.getTableName());
                    break;
                case ALTER_PARTITION:
                    hoptName = "ALTERPARTITION";
                    hact = HiveAccessType.ALTER;
                    String databaseName = ((PreAlterPartitionEvent) context).getDbName();
                    String tableName = ((PreAlterPartitionEvent) context).getTableName();
                    resource = new RangerHiveResource(HiveObjectType.PARTITION, databaseName, tableName);
                    break;
                case ADD_INDEX:
                    hoptName = "ADDINDEX";
                    hact = HiveAccessType.CREATE;
                    databaseName = ((PreAddIndexEvent) context).getIndex().getDbName();
                    tableName = ((PreAddIndexEvent) context).getIndex().getOrigTableName();
                    resource = new RangerHiveResource(HiveObjectType.INDEX, databaseName, tableName);
                    break;
                case DROP_INDEX:
                    hoptName = "DROPINDEX";
                    hact = HiveAccessType.DROP;
                    databaseName = ((PreDropIndexEvent) context).getIndex().getDbName();
                    tableName = ((PreDropIndexEvent) context).getIndex().getOrigTableName();
                    resource = new RangerHiveResource(HiveObjectType.INDEX, databaseName, tableName);
                    break;
                case ALTER_INDEX:
                    hoptName = "ALTERINDEX";
                    hact = HiveAccessType.ALTER;
                    databaseName = ((PreAlterIndexEvent) context).getOldIndex().getDbName();
                    tableName = ((PreAlterIndexEvent) context).getOldIndex().getOrigTableName();
                    resource = new RangerHiveResource(HiveObjectType.INDEX, databaseName, tableName);
                    break;
                case CREATE_DATABASE:
                    hoptName = HiveOperationType.CREATEDATABASE.name();
                    hact = HiveAccessType.CREATE;
                    db = ((PreCreateDatabaseEvent) context).getDatabase();
                    resource = new RangerHiveResource(HiveObjectType.DATABASE, db.getName());
                    break;
                case DROP_DATABASE:
                    hoptName = HiveOperationType.DROPDATABASE.name();
                    hact = HiveAccessType.DROP;
                    db = ((PreDropDatabaseEvent) context).getDatabase();
                    resource = new RangerHiveResource(HiveObjectType.DATABASE, db.getName());
                    break;
                case LOAD_PARTITION_DONE:
                    // noop for now
                    break;
                case AUTHORIZATION_API_CALL:
                    // noop for now
                default:
                    break;
            }
            if (resource != null) {
                resource.setServiceDef(hivePlugin.getServiceDef());
                request = new RangerHiveAccessRequest(resource, user, null,
                        hoptName, hact, authzContext, sessionContext);
                RangerAccessRequestUtil.setRequestedResourcesInContext(request.getContext(), requestedResources);
                result = hivePlugin.isAccessAllowed(request, auditHandler);
            }
        } catch (Exception e) {
            throw new InvalidOperationException("MetastorePreEventListener.onEvent exception: " + e.getMessage());
        } finally {
            auditHandler.flushAudit();
        }
        if (result != null && !result.getIsAllowed()) {
            String path = resource.getAsString();

            throw new InvalidOperationException(String.format("Permission denied: user [%s] does not have [%s] privilege on [%s]",
                    user, request.getHiveAccessType().name(), path));
        }
    }

    private String toString(PreEventContext context, SessionState ss, UserGroupInformation ugi) {
        StringBuilder sb = new StringBuilder();
        Iterator<Partition> pitor;

        sb.append("'Metastore onEvent':{");
        sb.append("'Event Type':").append(context.getEventType());

        switch (context.getEventType()) {
            case CREATE_TABLE:
                sb.append(", 'Table':[");
                sb.append(((PreCreateTableEvent) context).getTable().getDbName());
                sb.append(".");
                sb.append(((PreCreateTableEvent) context).getTable().getTableName());
                sb.append("]");
                break;
            case DROP_TABLE:
                sb.append(", 'Table':[");
                sb.append(((PreDropTableEvent) context).getTable().getDbName());
                sb.append(".");
                sb.append(((PreDropTableEvent) context).getTable().getTableName());
                sb.append("]");
                break;
            case ALTER_TABLE:
                sb.append(", 'Old Table':[");
                sb.append(((PreAlterTableEvent) context).getOldTable().getDbName());
                sb.append(".");
                sb.append(((PreAlterTableEvent) context).getOldTable().getTableName());
                sb.append("]");
                sb.append(", 'New Table':[");
                sb.append(((PreAlterTableEvent) context).getNewTable().getDbName());
                sb.append(".");
                sb.append(((PreAlterTableEvent) context).getNewTable().getTableName());
                sb.append("]");
                break;
            case READ_TABLE:
                sb.append(", 'Table':[");
                sb.append(((PreReadTableEvent) context).getTable().getDbName());
                sb.append(".");
                sb.append(((PreReadTableEvent) context).getTable().getTableName());
                sb.append("]");
                break;
            case READ_DATABASE:
                sb.append(", 'Database':[");
                sb.append(((PreReadDatabaseEvent) context).getDatabase().getName());
                sb.append("]");
                break;
            case ADD_PARTITION:
                pitor = ((PreAddPartitionEvent) context).getPartitionIterator();
                partitionsToString(pitor, sb);
                break;
            case DROP_PARTITION:
                pitor = ((PreDropPartitionEvent) context).getPartitionIterator();
                partitionsToString(pitor, sb);
                break;
            case ALTER_PARTITION:
                Partition p = ((PreAlterPartitionEvent) context).getNewPartition();
                sb.append(", 'Table':[");
                sb.append(((PreAlterPartitionEvent) context).getDbName());
                sb.append(".");
                sb.append(((PreAlterPartitionEvent) context).getTableName());
                sb.append("]");
                partitionToString(p, sb);
                break;
            case CREATE_DATABASE:
                sb.append(", 'Database':[");
                sb.append(((PreCreateDatabaseEvent) context).getDatabase().getName());
                sb.append("]");
                break;
            case DROP_DATABASE:
                sb.append(", 'Database':[");
                sb.append(((PreDropDatabaseEvent) context).getDatabase().getName());
                sb.append("]");
                break;
            case LOAD_PARTITION_DONE:
                // noop for now
                break;
            case AUTHORIZATION_API_CALL:
            default:
                break;
        }
        sb.append(", 'context':{");
        sb.append("'clientType':").append(appType);
        sb.append(", 'commandString':").append(ss == null ? null : ss.getLastCommand());
        sb.append(", 'ipAddress':").append(ss == null ? null : ss.getUserIpAddress());
        sb.append(", 'sessionString':").append(ss == null ? null : ss.getSessionId());
        sb.append("}");

        sb.append(", 'user':").append(ugi.getShortUserName());
        sb.append(", 'groups':[").append(ugi.getGroupNames()).append("]");

        sb.append("}");

        return sb.toString();
    }

    private void partitionsToString(Iterator<Partition> pitor, StringBuilder sb) {
        while (pitor.hasNext()) {
            partitionToString(pitor.next(), sb);
        }
    }

    private void partitionToString(Partition partition, StringBuilder sb) {
        Map<String, String> paras = partition.getParameters();
        sb.append(", 'Partition':[");
        for (Map.Entry<String, String> entry : paras.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append(",");
        }
        sb.append(" Location:");
        sb.append(partition.getSd().getLocation());
        sb.append("]");
    }
}
