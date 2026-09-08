package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Maps Hive entities from Metadata Registry URNs to Ranger Hive service resources.
 *
 * <p>Example URN: {@code urn:hive:table::hms:prod:sales:orders}
 */
public class MetadataRegistryHiveResourceMapper extends MetadataRegistryResourceMapper {
    public static final String ENTITY_TYPE_HIVE_DATABASE = "hive:database";
    public static final String ENTITY_TYPE_HIVE_TABLE    = "hive:table";

    public static final String RANGER_TYPE_HIVE_DB     = "database";
    public static final String RANGER_TYPE_HIVE_TABLE  = "table";
    public static final String RANGER_TYPE_HIVE_COLUMN = "column";

    public static final String FIELD_PATH_COLUMNS_PREFIX = "columns.";

    private static final String URN_CANONICAL_DELIMITER = "::";

    public MetadataRegistryHiveResourceMapper() {
        super("hive", new String[] {ENTITY_TYPE_HIVE_DATABASE, ENTITY_TYPE_HIVE_TABLE});
    }

    @Override
    public RangerServiceResource buildResource(MetadataRegistryEntity entity) throws Exception {
        if (entity == null || StringUtils.isBlank(entity.getUrn())) {
            throw new Exception("Metadata Registry entity URN is required");
        }

        String[] segments = canonicalSegments(entity.getUrn());
        if (segments.length < 3) {
            throw new Exception("Hive entity URN missing deployment segments: " + entity.getUrn());
        }

        String deployment = entity.getDeployment() != null ? entity.getDeployment() : segments[1];
        String serviceName = getRangerServiceName(deployment);
        String resourceKey = entity.getResourceKey();

        Map<String, RangerPolicyResource> elements = new HashMap<>();

        if (isColumnFieldPath(entity.getFieldPath())) {
            if (!ENTITY_TYPE_HIVE_TABLE.equals(entity.getEntityType())) {
                throw new Exception("Column fieldPath requires hive:table entity: " + entity.getUrn());
            }
            if (segments.length < 4) {
                throw new Exception("Hive table URN missing database/table segments: " + entity.getUrn());
            }
            String database = segments[2];
            String table    = entity.getName() != null ? entity.getName() : segments[3];
            String column   = columnName(entity.getFieldPath());
            elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(database));
            elements.put(RANGER_TYPE_HIVE_TABLE, new RangerPolicyResource(table));
            elements.put(RANGER_TYPE_HIVE_COLUMN, new RangerPolicyResource(column));
        } else if (ENTITY_TYPE_HIVE_DATABASE.equals(entity.getEntityType())) {
            String database = entity.getName() != null ? entity.getName() : segments[segments.length - 1];
            elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(database));
        } else if (ENTITY_TYPE_HIVE_TABLE.equals(entity.getEntityType())) {
            if (segments.length < 4) {
                throw new Exception("Hive table URN missing database/table segments: " + entity.getUrn());
            }
            String database = segments[2];
            String table    = entity.getName() != null ? entity.getName() : segments[3];
            elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(database));
            elements.put(RANGER_TYPE_HIVE_TABLE, new RangerPolicyResource(table));
        } else {
            throw new Exception("Unsupported Metadata Registry entity type for Hive mapper: " + entity.getEntityType());
        }

        if (elements.isEmpty()) {
            throw new Exception("Unable to derive Hive resource elements from " + entity.getUrn());
        }

        return new RangerServiceResource(resourceKey, serviceName, elements);
    }

    static String[] canonicalSegments(String urn) throws Exception {
        int delimiter = urn.indexOf(URN_CANONICAL_DELIMITER);
        if (delimiter < 0 || delimiter + URN_CANONICAL_DELIMITER.length() >= urn.length()) {
            throw new Exception("Malformed Metadata Registry URN: " + urn);
        }
        String canonicalPath = urn.substring(delimiter + URN_CANONICAL_DELIMITER.length());
        return canonicalPath.split(":", -1);
    }

    static boolean isColumnFieldPath(String fieldPath) {
        return fieldPath != null
                && fieldPath.toLowerCase(Locale.ROOT).startsWith(FIELD_PATH_COLUMNS_PREFIX);
    }

    static String columnName(String fieldPath) {
        if (!isColumnFieldPath(fieldPath)) {
            return null;
        }
        String remainder = fieldPath.substring(FIELD_PATH_COLUMNS_PREFIX.length()).trim();
        int    dot       = remainder.indexOf('.');
        return dot >= 0 ? remainder.substring(0, dot) : remainder;
    }
}
