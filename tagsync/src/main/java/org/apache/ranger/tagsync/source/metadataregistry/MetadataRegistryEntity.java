package org.apache.ranger.tagsync.source.metadataregistry;

/**
 * Entity identity from Metadata Registry {@code udf.governance} notifications.
 */
public class MetadataRegistryEntity {
    private final String urn;
    private final Long   entityId;
    private final String entityType;
    private final String fieldPath;
    private final String name;
    private final String displayString;
    private final String deployment;
    private final String technology;

    public MetadataRegistryEntity(
            String urn,
            Long entityId,
            String entityType,
            String fieldPath,
            String name,
            String displayString,
            String deployment,
            String technology) {
        this.urn           = urn;
        this.entityId      = entityId;
        this.entityType    = entityType;
        this.fieldPath     = fieldPath;
        this.name          = name;
        this.displayString = displayString;
        this.deployment    = deployment;
        this.technology    = technology;
    }

    public String getUrn() {
        return urn;
    }

    public Long getEntityId() {
        return entityId;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getFieldPath() {
        return fieldPath;
    }

    public String getName() {
        return name;
    }

    public String getDisplayString() {
        return displayString;
    }

    public String getDeployment() {
        return deployment;
    }

    public String getTechnology() {
        return technology;
    }

    public String getResourceKey() {
        return urn != null ? urn : String.valueOf(entityId);
    }

    @Override
    public String toString() {
        return "{urn=" + urn + ", entityType=" + entityType + ", fieldPath=" + fieldPath + "}";
    }
}
