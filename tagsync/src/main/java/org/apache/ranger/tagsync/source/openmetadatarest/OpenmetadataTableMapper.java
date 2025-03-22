package org.apache.ranger.tagsync.source.openmetadatarest;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.process.TagSyncConfig;

public class OpenmetadataTableMapper extends OpenmetadataResourceMapper {
    private static final Log LOG = LogFactory.getLog(OpenmetadataTableMapper.class);

    public static final String OPENMETADATA_ENTITY_TYPE_CATALOG = "catalog";
    public static final String OPENMETADATA_ENTITY_TYPE_SCHEMA  = "schema";
    public static final String OPENMETADATA_ENTITY_TYPE_TABLE   = "table";
    public static final String OPENMETADATA_ENTITY_TYPE_COLUMN  = "column";

    public static final String RANGER_TYPE_TRINO_CATALOG = "catalog";
    public static final String RANGER_TYPE_TRINO_SCHEMA  = "schema";
    public static final String RANGER_TYPE_TRINO_TABLE   = "table";
    public static final String RANGER_TYPE_TRINO_COLUMN  = "column";

    public static final String[] SUPPORTED_ENTITY_TYPES = { OPENMETADATA_ENTITY_TYPE_CATALOG, OPENMETADATA_ENTITY_TYPE_SCHEMA, OPENMETADATA_ENTITY_TYPE_TABLE, OPENMETADATA_ENTITY_TYPE_COLUMN };

    static TagSyncConfig config = TagSyncConfig.getInstance();

    static Properties props = config.getProperties();

    public static final String COMPONENT_NAME = TagSyncConfig.getRangerOpenmetadataTableComponentName(props);

    public OpenmetadataTableMapper() {
        super(COMPONENT_NAME, SUPPORTED_ENTITY_TYPES);
    }

    @Override
    public RangerServiceResource buildResource(final RangerOpenmetadataEntity entity) throws Exception {
        Map<String, RangerPolicyResource> mapOpenmetadataRangerElements = new HashMap<String, RangerPolicyResource>();
        // Get type of Entity from openmetadata. I.e., Table or Column
        String entityType = entity.getType();
        if (StringUtils.isEmpty(entityType)){
            throwExceptionWithMessage("property 'entityType' is either null or invalid." );
        }
        LOG.debug(entityType);

		/*
		Reason for not using get_id() of GUID in openmetadata:
		Since Open metadata does not have GUID or ID for its columns unlike Atlas, the guid of table to which the column belongs
		if included, will result in the entity being overwritten during the sink. Because, the Admin Rest Sink will overwrite the columns with preceeding one,
		since all of those have same GUID which is the table entity
		*/

		/*
		Get the openmetadata service name which is called as cluster name in terms of Ranger and Trino. If there are multiple trino instances,
		this will distinguish the services in Ranger and openmetadata(ingestion services)
		Note: There is no attribute or method to get cluster for column in openmetadata
		*/

        String clusterName = entity.getTableEntityObject().getService().getFullyQualifiedName();
        if (StringUtils.isEmpty(clusterName)){
            throwExceptionWithMessage("property 'clusterName' is either null or invalid." );
        }
        LOG.debug(clusterName);

        // Get service name in ranger
        String serviceName = getRangerServiceName(clusterName);
        if (StringUtils.isEmpty(serviceName)){
            throwExceptionWithMessage("property 'serviceName' is either null or invalid." );
        }
        LOG.debug(serviceName);

        if (entity.getType() == "table"){
            // Get table entity name
            String tableName = entity.getTableEntityObject().getName();
            if (StringUtils.isEmpty(tableName)){
                throwExceptionWithMessage("property 'tableName' is either null or invalid." );
            }
            LOG.debug(tableName);

            // get the schema name to which the table belongs
            String tableSchemaName = entity.getTableEntityObject().getDatabaseSchema().getName();
            if (StringUtils.isEmpty(tableSchemaName)){
                throwExceptionWithMessage("property 'tableSchemaName' is either null or invalid." );
            }
            LOG.debug(tableSchemaName);

            // Merge the schema name and table name to get the qualifiedName in Ranger and Atlas compatible format
            String tableQualifiedName = tableSchemaName + "." + tableName;
            if (StringUtils.isEmpty(tableQualifiedName)){
                throwExceptionWithMessage("property 'tableQualifiedName' is either null or invalid." );
            }
            LOG.debug(tableQualifiedName);

            // Get the catalog to which the table belongs
            String tableCatalogName = entity.getTableEntityObject().getDatabase().getName();
            if (StringUtils.isEmpty(tableCatalogName)){
                throwExceptionWithMessage("property 'tableCatalogName' is either null or invalid." );
            }
            LOG.debug(tableCatalogName);
            // Map Openmetadata Table Entity and its attributes with Ranger Trino Table Attributes.
            //Note: Open metadata and Trino formats are almost identical as openmetadata ingests tables, databases and catalogs from Trino directly
            if (StringUtils.isNotEmpty(tableCatalogName) && StringUtils.isNotEmpty(tableSchemaName) && StringUtils.isNotEmpty(tableName)){
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(tableCatalogName));
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_SCHEMA, new RangerPolicyResource(tableSchemaName));
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_TABLE, new RangerPolicyResource(tableName));
            }

        } else if(entity.getType() == "column"){
            // Get column entity name
            String columnName = entity.getColumnEntityObject().getName();
            if (StringUtils.isEmpty(columnName)){
                throwExceptionWithMessage("property 'columnName' is either null or invalid." );
            }
            LOG.debug(columnName);

            // get the column's table name to which the table belongs
            String columnTableName = entity.getColumnEntityObject().getFullyQualifiedName().split("\\.")[3];
            if (StringUtils.isEmpty(columnTableName)){
                throwExceptionWithMessage("property 'columnTableName' is either null or invalid." );
            }
            LOG.debug(columnTableName);

            // get the column schema name to which the table belongs
            String columnSchemaName = entity.getColumnEntityObject().getFullyQualifiedName().split("\\.")[2];
            if (StringUtils.isEmpty(columnSchemaName)){
                throwExceptionWithMessage("property 'columnSchemaName' is either null or invalid." );
            }
            LOG.debug(columnSchemaName);

            // Get the catalog to which the column's related table belongs
            String columnCatalogName = entity.getColumnEntityObject().getFullyQualifiedName().split("\\.")[1];
            if (StringUtils.isEmpty(columnCatalogName)){
                throwExceptionWithMessage("property 'columnCatalogName' is either null or invalid." );
            }
            LOG.debug(columnCatalogName);

            // get the column qualifiedName of Column in Ranger and Atlas compatible format from openmetadata
            String columnQualifiedName = columnSchemaName + columnTableName + columnName;
            if (StringUtils.isEmpty(columnQualifiedName)){
                throwExceptionWithMessage("property 'columnQualifiedName' is either null or invalid." );
            }
            LOG.debug(columnQualifiedName);
            // Map Openmetadata Table Entity and its attributes with Ranger Trino Table Attributes.
            //Note: Open metadata and Trino formats are almost identical as openmetadata ingests tables, databases and catalogs from Trino directly
            if (StringUtils.isNotEmpty(columnCatalogName) && StringUtils.isNotEmpty(columnSchemaName) && StringUtils.isNotEmpty(columnTableName) && StringUtils.isNotEmpty(columnName)){
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_CATALOG, new RangerPolicyResource(columnCatalogName));
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_SCHEMA, new RangerPolicyResource(columnSchemaName));
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_TABLE, new RangerPolicyResource(columnTableName));
                mapOpenmetadataRangerElements.put(RANGER_TYPE_TRINO_COLUMN, new RangerPolicyResource(columnName));
            }
        }
        else {
            throwExceptionWithMessage("unrecognized entity-type: " + entityType + "Expecting type 'table' or 'column'.");
        }

        if(mapOpenmetadataRangerElements.isEmpty()) {
            throwExceptionWithMessage("mapOpenmetadataRangerElements is empty!!. Which means the map of Trino Ranger Entity Types and OpenMetadata Entity types are unmapped.!!");
        }

        RangerServiceResource ret = new RangerServiceResource(null, serviceName, mapOpenmetadataRangerElements);

        return ret;
    }
}
