package org.apache.ranger.tagsync.source.metadataregistry;

/**
 * JSON field names in Metadata Registry governance notification payloads.
 */
public final class MetadataRegistryNotificationFields {
    public static final String MESSAGE            = "message";
    public static final String MESSAGE_TYPE       = "type";
    public static final String MESSAGE_TYPE_VALUE = "METADATA_REGISTRY_ENTITY_NOTIFICATION";
    public static final String OPERATION_TYPE     = "operationType";
    public static final String ENTITY             = "entity";

    public static final String URN            = "urn";
    public static final String ENTITY_ID        = "entityId";
    public static final String ENTITY_TYPE      = "entityType";
    public static final String FIELD_PATH       = "fieldPath";
    public static final String NAME             = "name";
    public static final String DISPLAY_STRING   = "displayString";
    public static final String DEPLOYMENT       = "deployment";
    public static final String TECHNOLOGY       = "technology";
    public static final String CLASSIFICATIONS  = "classifications";
    public static final String TYPE_NAME        = "typeName";
    public static final String ATTRIBUTES       = "attributes";

    private MetadataRegistryNotificationFields() {}
}
