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
package org.apache.ranger.authorization.presto.authorizer;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaRoutineName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangerSystemAccessControl implements SystemAccessControl {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSystemAccessControl.class);

    public static final String RANGER_CONFIG_KEYTAB              = "ranger.keytab";
    public static final String RANGER_CONFIG_PRINCIPAL           = "ranger.principal";
    public static final String RANGER_CONFIG_USE_UGI             = "ranger.use_ugi";
    public static final String RANGER_CONFIG_HADOOP_CONFIG       = "ranger.hadoop_config";
    public static final String RANGER_PRESTO_DEFAULT_HADOOP_CONF = "presto-ranger-site.xml";
    public static final String RANGER_PRESTO_SERVICETYPE         = "presto";
    public static final String RANGER_PRESTO_APPID               = "presto";

    private final RangerBasePlugin rangerPlugin;
    private       boolean          useUgi;

    public RangerSystemAccessControl(Map<String, String> config) {
        super();

        Configuration hadoopConf = new Configuration();

        if (config.get(RANGER_CONFIG_HADOOP_CONFIG) != null) {
            URL url = hadoopConf.getResource(config.get(RANGER_CONFIG_HADOOP_CONFIG));

            if (url == null) {
                LOG.warn("Hadoop config {} not found", config.get(RANGER_CONFIG_HADOOP_CONFIG));
            } else {
                hadoopConf.addResource(url);
            }
        } else {
            URL url = hadoopConf.getResource(RANGER_PRESTO_DEFAULT_HADOOP_CONF);

            LOG.debug("Trying to load Hadoop config from {} (can be null)", url);

            if (url != null) {
                hadoopConf.addResource(url);
            }
        }

        UserGroupInformation.setConfiguration(hadoopConf);

        if (config.get(RANGER_CONFIG_KEYTAB) != null && config.get(RANGER_CONFIG_PRINCIPAL) != null) {
            String keytab    = config.get(RANGER_CONFIG_KEYTAB);
            String principal = config.get(RANGER_CONFIG_PRINCIPAL);

            LOG.info("Performing kerberos login with principal {} and keytab {}", principal, keytab);

            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            } catch (IOException ioe) {
                LOG.error("Kerberos login failed", ioe);

                throw new RuntimeException(ioe);
            }
        }

        if (config.getOrDefault(RANGER_CONFIG_USE_UGI, "false").equalsIgnoreCase("true")) {
            useUgi = true;
        }

        rangerPlugin = new RangerBasePlugin(RANGER_PRESTO_SERVICETYPE, RANGER_PRESTO_APPID);

        rangerPlugin.init();
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName) {
        if (!hasPermission(createUserResource(userName), context, PrestoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanImpersonateUser( {}) denied", userName);
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), userName);
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {
        // pass as it is deprecated
    }

    /**
     * This is a NOOP. Everyone can execute a query
     *
     * @param context
     */
    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        if (!hasPermission(createUserResource(queryOwner), context, PrestoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanViewQueryOwnedBy({}) denied", queryOwner);
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), queryOwner);
        }
    }

    /**
     * This is a NOOP, no filtering is applied
     */
    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners) {
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        if (!hasPermission(createUserResource(queryOwner), context, PrestoAccessType.IMPERSONATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanKillQueryOwnedBy({}) denied", queryOwner);
            AccessDeniedException.denyImpersonateUser(context.getIdentity().getUser(), queryOwner);
        }
    }

    /**
     * SYSTEM
     **/

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {
        if (!hasPermission(createSystemPropertyResource(propertyName), context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetSystemSessionProperty denied");
            AccessDeniedException.denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {
        if (!hasPermission(createResource(catalogName), context, PrestoAccessType.USE)) {
            LOG.debug("RangerSystemAccessControl.checkCanAccessCatalog({}) denied", catalogName);
            AccessDeniedException.denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        LOG.debug("==> RangerSystemAccessControl.filterCatalogs{}", catalogs);

        Set<String> filteredCatalogs = new HashSet<>(catalogs.size());

        for (String catalog : catalogs) {
            if (hasPermission(createResource(catalog), context, PrestoAccessType.SELECT)) {
                filteredCatalogs.add(catalog);
            }
        }

        return filteredCatalogs;
    }

    /** PERMISSION CHECKS ORDERED BY SYSTEM, CATALOG, SCHEMA, TABLE, VIEW, COLUMN, QUERY, FUNCTIONS, PROCEDURES **/

    /**
     * Create schema is evaluated on the level of the Catalog. This means that it is assumed you have permission
     * to create a schema when you have create rights on the catalog level
     */
    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        if (!hasPermission(createResource(schema.getCatalogName()), context, PrestoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateSchema({}) denied", schema.getSchemaName());
            AccessDeniedException.denyCreateSchema(schema.getSchemaName());
        }
    }

    /**
     * This is evaluated against the schema name as ownership information is not available
     */
    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, PrestoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropSchema({}) denied", schema.getSchemaName());
            AccessDeniedException.denyDropSchema(schema.getSchemaName());
        }
    }

    /**
     * This is evaluated against the schema name as ownership information is not available
     */
    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {
        RangerPrestoResource res = createResource(schema.getCatalogName(), schema.getSchemaName());
        if (!hasPermission(res, context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameSchema({}) denied", schema.getSchemaName());
            AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    /**
     * SCHEMA
     **/

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, PrestoPrincipal principal) {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, PrestoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetSchemaAuthorization({}) denied", schema.getSchemaName());
            AccessDeniedException.denySetSchemaAuthorization(schema.getSchemaName(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {
        if (!hasPermission(createResource(catalogName), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowSchemas({}) denied", catalogName);
            AccessDeniedException.denyShowSchemas(catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
        LOG.debug("==> RangerSystemAccessControl.filterSchemas({}) denied", catalogName);

        Set<String> filteredSchemaNames = new HashSet<>(schemaNames.size());

        for (String schemaName : schemaNames) {
            if (hasPermission(createResource(catalogName, schemaName), context, PrestoAccessType.SELECT)) {
                filteredSchemaNames.add(schemaName);
            }
        }

        return filteredSchemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        if (!hasPermission(createResource(schema.getCatalogName(), schema.getSchemaName()), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowCreateSchema({}) denied", schema.getSchemaName());
            AccessDeniedException.denyShowCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables({}) denied", table);
            AccessDeniedException.denyShowCreateTable(table.toString());
        }
    }

    /**
     * Create table is verified on schema level
     */
    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName()), context, PrestoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateTable({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropTable({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {
        RangerPrestoResource res = createResource(table);
        if (!hasPermission(res, context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameTable({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetTableComment({}) denied", table);
            AccessDeniedException.denyCommentTable(table.toString());
        }
    }

    /**
     * TABLE
     **/

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {
        if (!hasPermission(createResource(schema), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables({}) denied", schema);
            AccessDeniedException.denyShowTables(schema.toString());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
        LOG.debug("==> RangerSystemAccessControl.filterTables({})", catalogName);

        Set<SchemaTableName> filteredTableNames = new HashSet<>(tableNames.size());

        for (SchemaTableName tableName : tableNames) {
            RangerPrestoResource res = createResource(catalogName, tableName.getSchemaName(), tableName.getTableName());

            if (hasPermission(res, context, PrestoAccessType.SELECT)) {
                filteredTableNames.add(tableName);
            }
        }

        return filteredTableNames;
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowTables({}) denied", table);
            AccessDeniedException.denyShowColumns(table.toString());
        }
    }

    /**
     * This is a NOOP, no filtering is applied
     */
    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns) {
        return columns;
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        RangerPrestoResource res = createResource(table);

        if (!hasPermission(res, context, PrestoAccessType.ALTER)) {
            AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        RangerPrestoResource res = createResource(table);

        if (!hasPermission(res, context, PrestoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropColumn({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated on table level
     */
    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        RangerPrestoResource res = createResource(table);

        if (!hasPermission(res, context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameColumn({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        for (RangerPrestoResource res : createResource(table, columns)) {
            if (!hasPermission(res, context, PrestoAccessType.SELECT)) {
                LOG.debug("RangerSystemAccessControl.checkCanSelectFromColumns({}) denied", table.getSchemaTableName().getTableName());
                AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), columns);
            }
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        RangerPrestoResource res = createResource(table);

        if (!hasPermission(res, context, PrestoAccessType.INSERT)) {
            LOG.debug("RangerSystemAccessControl.checkCanInsertIntoTable({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.DELETE)) {
            LOG.debug("RangerSystemAccessControl.checkCanDeleteFromTable({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    /**
     * Create view is verified on schema level
     */
    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {
        if (!hasPermission(createResource(view.getCatalogName(), view.getSchemaTableName().getSchemaName()), context, PrestoAccessType.CREATE)) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateView({}) denied", view.getSchemaTableName().getTableName());
            AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {
        if (!hasPermission(createResource(view), context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanRenameView({}) denied", view);
            AccessDeniedException.denyRenameView(view.toString(), newView.toString());
        }
    }

    /**
     * This is evaluated against the table name as ownership information is not available
     */
    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {
        if (!hasPermission(createResource(view), context, PrestoAccessType.DROP)) {
            LOG.debug("RangerSystemAccessControl.checkCanDropView({}) denied", view.getSchemaTableName().getTableName());
            AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    /**
     * This check equals the check for checkCanCreateView
     */
    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        try {
            checkCanCreateView(context, table);
        } catch (AccessDeniedException ade) {
            LOG.debug("RangerSystemAccessControl.checkCanCreateViewWithSelectFromColumns({}) denied", table.getSchemaTableName().getTableName());
            AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
        }
    }

    /** COLUMN **/

    /**
     * FUNCTIONS
     **/
    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String function, PrestoPrincipal grantee, boolean grantOption) {
        if (!hasPermission(createFunctionResource(function), context, PrestoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanGrantExecuteFunctionPrivilege({}) denied", function);
            AccessDeniedException.denyGrantExecuteFunctionPrivilege(function, context.getIdentity(), grantee.getName());
        }
    }

    /**
     * CATALOG
     **/
    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {
        if (!hasPermission(createCatalogSessionResource(catalogName, propertyName), context, PrestoAccessType.ALTER)) {
            LOG.debug("RangerSystemAccessControl.checkCanSetCatalogSessionProperty({}) denied", catalogName);
            AccessDeniedException.denySetCatalogSessionProperty(catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.GRANT)) {
            LOG.debug("RangerSystemAccessControl.checkCanGrantTablePrivilege({}) denied", table);
            AccessDeniedException.denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor) {
        if (!hasPermission(createResource(table), context, PrestoAccessType.REVOKE)) {
            LOG.debug("RangerSystemAccessControl.checkCanRevokeTablePrivilege({}) denied", table);
            AccessDeniedException.denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {
        if (!hasPermission(createResource(catalogName), context, PrestoAccessType.SHOW)) {
            LOG.debug("RangerSystemAccessControl.checkCanShowRoles({}) denied", catalogName);
            AccessDeniedException.denyShowRoles(catalogName);
        }
    }

    /**
     * PROCEDURES
     **/
    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure) {
        if (!hasPermission(createProcedureResource(procedure), context, PrestoAccessType.EXECUTE)) {
            LOG.debug("RangerSystemAccessControl.checkCanExecuteFunction({}) denied", procedure.getSchemaRoutineName().getRoutineName());
            AccessDeniedException.denyExecuteProcedure(procedure.getSchemaRoutineName().getRoutineName());
        }
    }

    /**
     * QUERY
     **/

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, String function) {
        if (!hasPermission(createFunctionResource(function), context, PrestoAccessType.EXECUTE)) {
            LOG.debug("RangerSystemAccessControl.checkCanExecuteFunction({}) denied", function);
            AccessDeniedException.denyExecuteFunction(function);
        }
    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        RangerPrestoAccessRequest request        = createAccessRequest(createResource(tableName), context, PrestoAccessType.SELECT);
        RangerAccessResult        result         = getRowFilterResult(request);
        ViewExpression            viewExpression = null;

        if (isRowFilterEnabled(result)) {
            String filter = result.getFilterExpr();

            viewExpression = new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getSchemaName()), filter);
        }

        return Optional.ofNullable(viewExpression);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {
        RangerPrestoAccessRequest request        = createAccessRequest(createResource(tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), tableName.getSchemaTableName().getTableName(), Optional.of(columnName)), context, PrestoAccessType.SELECT);
        RangerAccessResult        result         = getDataMaskResult(request);
        ViewExpression            viewExpression = null;

        if (isDataMaskEnabled(result)) {
            String                                 maskType    = result.getMaskType();
            RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
            String                                 transformer = null;

            if (maskTypeDef != null) {
                transformer = maskTypeDef.getTransformer();
            }

            if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
                transformer = "NULL";
            } else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
                String maskedValue = result.getMaskedValue();

                if (maskedValue == null) {
                    transformer = "NULL";
                } else {
                    transformer = maskedValue;
                }
            }

            if (StringUtils.isNotEmpty(transformer)) {
                transformer = transformer.replace("{col}", columnName).replace("{type}", type.getDisplayName());
            }

            viewExpression = new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getSchemaName()), transformer);

            LOG.debug("getColumnMask: user: {}, catalog: {}, schema: {}, transformer: {}", context.getIdentity().getUser(), tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(), transformer);
        }

        return Optional.ofNullable(viewExpression);
    }

    /**
     * FILTERING AND DATA MASKING
     **/

    private RangerAccessResult getDataMaskResult(RangerPrestoAccessRequest request) {
        LOG.debug("==> getDataMaskResult(request={})", request);

        RangerAccessResult ret = rangerPlugin.evalDataMaskPolicies(request, null);

        LOG.debug("<== getDataMaskResult(request={}): ret={}", request, ret);

        return ret;
    }

    private RangerAccessResult getRowFilterResult(RangerPrestoAccessRequest request) {
        LOG.debug("==> getRowFilterResult(request={})", request);

        RangerAccessResult ret = rangerPlugin.evalRowFilterPolicies(request, null);

        LOG.debug("<== getRowFilterResult(request={}): ret={}", request, ret);

        return ret;
    }

    private boolean isDataMaskEnabled(RangerAccessResult result) {
        return result != null && result.isMaskEnabled();
    }

    private boolean isRowFilterEnabled(RangerAccessResult result) {
        return result != null && result.isRowFilterEnabled();
    }

    /**
     * HELPER FUNCTIONS
     **/

    private RangerPrestoAccessRequest createAccessRequest(RangerPrestoResource resource, SystemSecurityContext context, PrestoAccessType accessType) {
        String      userName;
        Set<String> userGroups = null;

        if (useUgi) {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(context.getIdentity().getUser());

            userName = ugi.getShortUserName();

            String[] groups = ugi.getGroupNames();

            if (groups != null && groups.length > 0) {
                userGroups = new HashSet<>(Arrays.asList(groups));
            }
        } else {
            userName   = context.getIdentity().getUser();
            userGroups = context.getIdentity().getGroups();
        }

        return new RangerPrestoAccessRequest(resource, userName, userGroups, accessType);
    }

    private boolean hasPermission(RangerPrestoResource resource, SystemSecurityContext context, PrestoAccessType accessType) {
        boolean                   ret     = false;
        RangerPrestoAccessRequest request = createAccessRequest(resource, context, accessType);
        RangerAccessResult        result  = rangerPlugin.isAccessAllowed(request);

        if (result != null && result.getIsAllowed()) {
            ret = true;
        }

        return ret;
    }

    private static RangerPrestoResource createUserResource(String userName) {
        RangerPrestoResource res = new RangerPrestoResource();

        res.setValue(RangerPrestoResource.KEY_USER, userName);

        return res;
    }

    private static RangerPrestoResource createFunctionResource(String function) {
        RangerPrestoResource res = new RangerPrestoResource();

        res.setValue(RangerPrestoResource.KEY_FUNCTION, function);

        return res;
    }

    private static RangerPrestoResource createProcedureResource(CatalogSchemaRoutineName procedure) {
        RangerPrestoResource res = new RangerPrestoResource();

        res.setValue(RangerPrestoResource.KEY_CATALOG, procedure.getCatalogName());
        res.setValue(RangerPrestoResource.KEY_SCHEMA, procedure.getSchemaRoutineName().getSchemaName());
        res.setValue(RangerPrestoResource.KEY_PROCEDURE, procedure.getSchemaRoutineName().getRoutineName());

        return res;
    }

    private static RangerPrestoResource createCatalogSessionResource(String catalogName, String propertyName) {
        RangerPrestoResource res = new RangerPrestoResource();

        res.setValue(RangerPrestoResource.KEY_CATALOG, catalogName);
        res.setValue(RangerPrestoResource.KEY_SESSION_PROPERTY, propertyName);

        return res;
    }

    private static RangerPrestoResource createSystemPropertyResource(String property) {
        RangerPrestoResource res = new RangerPrestoResource();

        res.setValue(RangerPrestoResource.KEY_SYSTEM_PROPERTY, property);

        return res;
    }

    private static RangerPrestoResource createResource(CatalogSchemaName catalogSchemaName) {
        return createResource(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName());
    }

    private static RangerPrestoResource createResource(CatalogSchemaTableName catalogSchemaTableName) {
        return createResource(catalogSchemaTableName.getCatalogName(), catalogSchemaTableName.getSchemaTableName().getSchemaName(), catalogSchemaTableName.getSchemaTableName().getTableName());
    }

    private static RangerPrestoResource createResource(String catalogName) {
        return new RangerPrestoResource(catalogName, Optional.empty(), Optional.empty());
    }

    private static RangerPrestoResource createResource(String catalogName, String schemaName) {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.empty());
    }

    private static RangerPrestoResource createResource(String catalogName, String schemaName, final String tableName) {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.of(tableName));
    }

    private static RangerPrestoResource createResource(String catalogName, String schemaName, final String tableName, final Optional<String> column) {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.of(tableName), column);
    }

    private static List<RangerPrestoResource> createResource(CatalogSchemaTableName table, Set<String> columns) {
        List<RangerPrestoResource> colRequests = new ArrayList<>();

        if (!columns.isEmpty()) {
            for (String column : columns) {
                RangerPrestoResource rangerPrestoResource = createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), Optional.of(column));

                colRequests.add(rangerPrestoResource);
            }
        } else {
            colRequests.add(createResource(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), Optional.empty()));
        }

        return colRequests;
    }
}
