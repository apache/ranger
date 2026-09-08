package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class MetadataRegistryEntityWithTags {
    private final MetadataRegistryEntity entity;
    private final List<MetadataRegistryNotificationParser.MetadataRegistryClassification> tags;

    public MetadataRegistryEntityWithTags(MetadataRegistryNotificationWrapper notification) {
        this.entity = notification.getEntity();
        this.tags   = notification.getClassifications();
    }

    public MetadataRegistryEntity getEntity() {
        return entity;
    }

    public List<MetadataRegistryNotificationParser.MetadataRegistryClassification> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "{entity=" + entity + ", tags=" + (CollectionUtils.isEmpty(tags) ? "[]" : tags) + "}";
    }
}
