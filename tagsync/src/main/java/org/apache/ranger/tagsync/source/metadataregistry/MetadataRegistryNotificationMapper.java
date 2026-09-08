package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Metadata Registry notifications to Ranger {@link ServiceTags}, following Atlas Tag Sync flow.
 */
public final class MetadataRegistryNotificationMapper {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataRegistryNotificationMapper.class);

    private MetadataRegistryNotificationMapper() {}

    public static boolean isNotificationHandled(MetadataRegistryNotificationWrapper notification) {
        if (notification == null || notification.getOpType() == null) {
            return false;
        }

        boolean ret = switch (notification.getOpType()) {
            case ENTITY_CREATE, ENTITY_UPDATE ->
                    !notification.isEmptyClassifications();
            case ENTITY_DELETE -> true;
            case CLASSIFICATION_ADD, CLASSIFICATION_UPDATE, CLASSIFICATION_DELETE -> true;
            default -> false;
        };

        if (ret) {
            ret = notification.isEntityTypeHandled();
        }

        return ret;
    }

    public static void logUnhandledNotification(MetadataRegistryNotificationWrapper notification) {
        if (notification == null) {
            return;
        }
        if (!notification.isEntityTypeHandled()) {
            LOG.warn("Tag Sync is not enabled to handle Metadata Registry entity type [{}]",
                    notification.getEntity().getEntityType());
        }
    }

    public static Map<String, ServiceTags> processEntities(List<MetadataRegistryEntityWithTags> entitiesWithTags) {
        Map<String, ServiceTags> ret = new HashMap<>();
        if (CollectionUtils.isEmpty(entitiesWithTags)) {
            return ret;
        }
        for (MetadataRegistryEntityWithTags element : entitiesWithTags) {
            if (element.getEntity() != null) {
                buildServiceTags(element, ret);
            }
        }
        if (MapUtils.isNotEmpty(ret)) {
            for (ServiceTags serviceTags : ret.values()) {
                serviceTags.setOp(ServiceTags.OP_REPLACE);
            }
        }
        return ret;
    }

    private static void buildServiceTags(
            MetadataRegistryEntityWithTags entityWithTags,
            Map<String, ServiceTags> serviceTagsMap) {
        MetadataRegistryEntity entity          = entityWithTags.getEntity();
        RangerServiceResource  serviceResource = MetadataRegistryResourceMapperUtil.getRangerServiceResource(entity);
        if (serviceResource == null) {
            LOG.error("Failed to build serviceResource for Metadata Registry entity {}", entity.getUrn());
            return;
        }

        String serviceName = serviceResource.getServiceName();
        ServiceTags serviceTags = serviceTagsMap.computeIfAbsent(serviceName, ignored -> {
            ServiceTags created = new ServiceTags();
            created.setOp(ServiceTags.OP_ADD_OR_UPDATE);
            created.setServiceName(serviceName);
            return created;
        });

        serviceResource.setId((long) serviceTags.getServiceResources().size());
        serviceTags.getServiceResources().add(serviceResource);

        List<Long> tagIds = new ArrayList<>();
        List<MetadataRegistryNotificationParser.MetadataRegistryClassification> tags = entityWithTags.getTags();
        if (CollectionUtils.isNotEmpty(tags)) {
            for (MetadataRegistryNotificationParser.MetadataRegistryClassification tag : tags) {
                RangerTag rangerTag = new RangerTag(
                        null,
                        tag.getTagName(),
                        new HashMap<>(tag.getAttributes()),
                        RangerTag.OWNER_SERVICERESOURCE);
                rangerTag.setId((long) serviceTags.getTags().size());
                serviceTags.getTags().put(rangerTag.getId(), rangerTag);
                tagIds.add(rangerTag.getId());

                RangerTagDef tagDef = new RangerTagDef(tag.getTagName(), "MetadataRegistry");
                if (MapUtils.isNotEmpty(tag.getAttributes())) {
                    List<RangerTagAttributeDef> attributeDefs = new ArrayList<>();
                    for (String attributeName : tag.getAttributes().keySet()) {
                        attributeDefs.add(new RangerTagAttributeDef(attributeName, "string"));
                    }
                    tagDef.setAttributeDefs(attributeDefs);
                }
                tagDef.setId((long) serviceTags.getTagDefinitions().size());
                serviceTags.getTagDefinitions().put(tagDef.getId(), tagDef);
            }
        }

        serviceTags.getResourceToTagIds().put(serviceResource.getId(), tagIds);
    }
}
