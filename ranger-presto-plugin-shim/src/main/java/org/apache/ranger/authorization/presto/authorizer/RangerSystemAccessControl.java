/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.presto.authorizer;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaRoutineName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import javax.inject.Inject;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangerSystemAccessControl implements SystemAccessControl {
    private static final String RANGER_PLUGIN_TYPE                      = "presto";
    private static final String RANGER_PRESTO_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.presto.authorizer.RangerSystemAccessControl";

    private final RangerPluginClassLoader rangerPluginClassLoader;
    private final SystemAccessControl     systemAccessControlImpl;

    @Inject
    public RangerSystemAccessControl(RangerConfig config) {
        try {
            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<SystemAccessControl> cls = (Class<SystemAccessControl>) Class.forName(RANGER_PRESTO_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("RangerSystemAccessControl")) {
                Map<String, String> configMap = new HashMap<>();

                if (config.getKeytab() != null && config.getPrincipal() != null) {
                    configMap.put("ranger.keytab", config.getKeytab());
                    configMap.put("ranger.principal", config.getPrincipal());
                }

                configMap.put("ranger.use_ugi", Boolean.toString(config.isUseUgi()));

                if (config.getHadoopConfigPath() != null) {
                    configMap.put("ranger.hadoop_config", config.getHadoopConfigPath());
                }

                systemAccessControlImpl = cls.getDeclaredConstructor(Map.class).newInstance(configMap);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanImpersonateUser")) {
            systemAccessControlImpl.checkCanImpersonateUser(context, userName);
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSetUser")) {
            systemAccessControlImpl.checkCanSetUser(principal, userName);
        }
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanExecuteQuery")) {
            systemAccessControlImpl.checkCanExecuteQuery(context);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanViewQueryOwnedBy")) {
            systemAccessControlImpl.checkCanViewQueryOwnedBy(context, queryOwner);
        }
    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("filterViewQueryOwnedBy")) {
            return systemAccessControlImpl.filterViewQueryOwnedBy(context, queryOwners);
        }
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanKillQueryOwnedBy")) {
            systemAccessControlImpl.checkCanKillQueryOwnedBy(context, queryOwner);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSetSystemSessionProperty")) {
            systemAccessControlImpl.checkCanSetSystemSessionProperty(context, propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanAccessCatalog")) {
            systemAccessControlImpl.checkCanAccessCatalog(context, catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanAccessCatalog")) {
            return systemAccessControlImpl.filterCatalogs(context, catalogs);
        }
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanCreateSchema")) {
            systemAccessControlImpl.checkCanCreateSchema(context, schema);
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanDropSchema")) {
            systemAccessControlImpl.checkCanDropSchema(context, schema);
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanRenameSchema")) {
            systemAccessControlImpl.checkCanRenameSchema(context, schema, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, PrestoPrincipal principal) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSetSchemaAuthorization")) {
            systemAccessControlImpl.checkCanSetSchemaAuthorization(context, schema, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowSchemas")) {
            systemAccessControlImpl.checkCanShowSchemas(context, catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("filterSchemas")) {
            return systemAccessControlImpl.filterSchemas(context, catalogName, schemaNames);
        }
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowCreateSchema")) {
            systemAccessControlImpl.checkCanShowCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowCreateTable")) {
            systemAccessControlImpl.checkCanShowCreateTable(context, table);
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanCreateTable")) {
            systemAccessControlImpl.checkCanCreateTable(context, table);
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanDropTable")) {
            systemAccessControlImpl.checkCanDropTable(context, table);
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanRenameTable")) {
            systemAccessControlImpl.checkCanRenameTable(context, table, newTable);
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSetTableComment")) {
            systemAccessControlImpl.checkCanSetTableComment(context, table);
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowTables")) {
            systemAccessControlImpl.checkCanShowTables(context, schema);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("filterTables")) {
            return systemAccessControlImpl.filterTables(context, catalogName, tableNames);
        }
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowColumns")) {
            systemAccessControlImpl.checkCanShowColumns(context, table);
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("filterColumns")) {
            return systemAccessControlImpl.filterColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanAddColumn")) {
            systemAccessControlImpl.checkCanAddColumn(context, table);
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanDropColumn")) {
            systemAccessControlImpl.checkCanDropColumn(context, table);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanRenameColumn")) {
            systemAccessControlImpl.checkCanRenameColumn(context, table);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSelectFromColumns")) {
            systemAccessControlImpl.checkCanSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanInsertIntoTable")) {
            systemAccessControlImpl.checkCanInsertIntoTable(context, table);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanDeleteFromTable")) {
            systemAccessControlImpl.checkCanDeleteFromTable(context, table);
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanCreateView")) {
            systemAccessControlImpl.checkCanCreateView(context, view);
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanRenameView")) {
            systemAccessControlImpl.checkCanRenameView(context, view, newView);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanDropView")) {
            systemAccessControlImpl.checkCanDropView(context, view);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanCreateViewWithSelectFromColumns")) {
            systemAccessControlImpl.checkCanCreateViewWithSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, PrestoPrincipal grantee, boolean grantOption) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanGrantExecuteFunctionPrivilege")) {
            systemAccessControlImpl.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanSetCatalogSessionProperty")) {
            systemAccessControlImpl.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanGrantTablePrivilege")) {
            systemAccessControlImpl.checkCanGrantTablePrivilege(context, privilege, table, grantee, withGrantOption);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanRevokeTablePrivilege")) {
            systemAccessControlImpl.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOptionFor);
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanShowRoles")) {
            systemAccessControlImpl.checkCanShowRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanExecuteProcedure")) {
            systemAccessControlImpl.checkCanExecuteProcedure(systemSecurityContext, procedure);
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("checkCanExecuteFunction")) {
            systemAccessControlImpl.checkCanExecuteFunction(systemSecurityContext, functionName);
        }
    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("getRowFilter")) {
            return systemAccessControlImpl.getRowFilter(context, tableName);
        }
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("getRowFilter")) {
            return systemAccessControlImpl.getColumnMask(context, tableName, columnName, type);
        }
    }

    private class PluginClassLoaderActivator implements AutoCloseable {
        private final String methodName;

        PluginClassLoaderActivator(String methodName) {
            this.methodName = methodName;

            if (rangerPluginClassLoader != null) {
                rangerPluginClassLoader.activate();
            }
        }

        @Override
        public void close() {
            if (rangerPluginClassLoader != null) {
                rangerPluginClassLoader.deactivate();
            }
        }
    }
}
