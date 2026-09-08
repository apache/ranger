package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class MetadataRegistryNotificationWrapper {
    private final MetadataRegistryEntity                                      entity;
    private final List<MetadataRegistryNotificationParser.MetadataRegistryClassification> classifications;
    private final MetadataRegistryNotificationParser.NotificationOpType       opType;
    private final boolean                                                       entityTypeHandled;
    private final boolean                                                       emptyClassifications;

    public MetadataRegistryNotificationWrapper(
            MetadataRegistryEntity entity,
            List<MetadataRegistryNotificationParser.MetadataRegistryClassification> classifications,
            MetadataRegistryNotificationParser.NotificationOpType opType) {
        this.entity               = entity;
        this.classifications        = classifications;
        this.opType                 = opType;
        this.entityTypeHandled      = MetadataRegistryResourceMapperUtil.isEntityTypeHandled(entity);
        this.emptyClassifications   = CollectionUtils.isEmpty(classifications);
    }

    public MetadataRegistryEntity getEntity() {
        return entity;
    }

    public List<MetadataRegistryNotificationParser.MetadataRegistryClassification> getClassifications() {
        return classifications;
    }

    public MetadataRegistryNotificationParser.NotificationOpType getOpType() {
        return opType;
    }

    public boolean isEntityTypeHandled() {
        return entityTypeHandled;
    }

    public boolean isEmptyClassifications() {
        return emptyClassifications;
    }

    public boolean isEntityDeleteOp() {
        return opType == MetadataRegistryNotificationParser.NotificationOpType.ENTITY_DELETE;
    }

    public boolean isEntityCreateOp() {
        return opType == MetadataRegistryNotificationParser.NotificationOpType.ENTITY_CREATE;
    }
}
