package org.apache.ranger.tagsync.source.openmetadatarest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;

import org.apache.ranger.plugin.util.ServiceTags;

import org.openmetadata.client.model.TagLabel;
import org.apache.ranger.tagsync.source.atlas.EntityNotificationWrapper;
import org.apache.ranger.tagsync.source.openmetadatarest.OpenmetadataRESTTagSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenmetadataMappingHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OpenmetadataMappingHelper.class);
    public static Map<String, ServiceTags> processOpenmetadataEntities(List<RangerOpenmetadataEntityWithTags> openmetadataEntities) {
        Map<String, ServiceTags> ret = null;

        try {
            ret = buildServiceTags(openmetadataEntities);
        } catch (Exception exception) {
           LOG.error("Failed to build serviceTags",exception);
        }

        return ret;
    }

    static private Map<String, ServiceTags> buildServiceTags(List<RangerOpenmetadataEntityWithTags> entitiesWithTags) {

        Map<String, ServiceTags> ret = new HashMap<>();

        for (RangerOpenmetadataEntityWithTags element : entitiesWithTags) {
            RangerOpenmetadataEntity entity = element.getEntity();
            if (entity != null) {
                buildServiceTags(element, ret);
            } else {
                LOG.warn("Ignoring entity because its State is not ACTIVE:", element);
            }
        }

        // Remove duplicate tag definitions
        if(CollectionUtils.isNotEmpty(ret.values())) {
            for (ServiceTags serviceTag : ret.values()) {
                if(MapUtils.isNotEmpty(serviceTag.getTagDefinitions())) {
                    Map<String, RangerTagDef> uniqueTagDefs = new HashMap<>();

                    for (RangerTagDef tagDef : serviceTag.getTagDefinitions().values()) {
                        RangerTagDef existingTagDef = uniqueTagDefs.get(tagDef.getName());

                        if (existingTagDef == null) {
                            uniqueTagDefs.put(tagDef.getName(), tagDef);
                        } else {
                            if(CollectionUtils.isNotEmpty(tagDef.getAttributeDefs())) {
                                for(RangerTagAttributeDef tagAttrDef : tagDef.getAttributeDefs()) {
                                    boolean attrDefExists = false;

                                    if(CollectionUtils.isNotEmpty(existingTagDef.getAttributeDefs())) {
                                        for(RangerTagAttributeDef existingTagAttrDef : existingTagDef.getAttributeDefs()) {
                                            if(StringUtils.equalsIgnoreCase(existingTagAttrDef.getName(), tagAttrDef.getName())) {
                                                attrDefExists = true;
                                                break;
                                            }
                                        }
                                    }

                                    if(! attrDefExists) {
                                        existingTagDef.getAttributeDefs().add(tagAttrDef);
                                    }
                                }
                            }
                        }
                    }
                    serviceTag.getTagDefinitions().clear();
                    for(RangerTagDef tagDef : uniqueTagDefs.values()) {
                        serviceTag.getTagDefinitions().put(tagDef.getId(), tagDef);
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(ret)) {
            for (Map.Entry<String, ServiceTags> entry : ret.entrySet()) {
                ServiceTags serviceTags = entry.getValue();
                serviceTags.setOp(ServiceTags.OP_REPLACE);
            }
        }
        return ret;
    }    
    static private ServiceTags buildServiceTags(RangerOpenmetadataEntityWithTags entityWithTags, Map<String, ServiceTags> serviceTagsMap) {
        ServiceTags             ret             = null;
        RangerOpenmetadataEntity       entity          = entityWithTags.getEntity();
        RangerServiceResource  serviceResource  = OpenmetadataResourceMapperUtil.getRangerServiceResource(entity);

        if (serviceResource != null) {

            List<RangerTag>    tags = getTags(entityWithTags);
            List<RangerTagDef> tagDefs = getTagDefs(entityWithTags);
            String             serviceName = serviceResource.getServiceName();
            ret = createOrGetServiceTags(serviceTagsMap, serviceName);
            serviceResource.setId((long) ret.getServiceResources().size());
            ret.getServiceResources().add(serviceResource);

            List<Long> tagIds = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(tags)) {
                for (RangerTag tag : tags) {
                    tag.setId((long) ret.getTags().size());
                    ret.getTags().put(tag.getId(), tag);

                    tagIds.add(tag.getId());
                }
            } else {
                    LOG.warn("Entity " + entityWithTags + " does not have any tags associated with it");
            }

            ret.getResourceToTagIds().put(serviceResource.getId(), tagIds);
            LOG.debug("==> Service name from Ranger Service resource: ",serviceName);
            if (CollectionUtils.isNotEmpty(tagDefs)) {
                for (RangerTagDef tagDef : tagDefs) {
                    tagDef.setId((long) ret.getTagDefinitions().size());
                    ret.getTagDefinitions().put(tagDef.getId(), tagDef);
                }
            }
            else {
                LOG.warn("==> Null tag definitions received. Ignoring to build service resource.<==");
            }
        }
        return ret;
    }
    static private ServiceTags createOrGetServiceTags(Map<String, ServiceTags> serviceTagsMap, String serviceName) {
        ServiceTags ret = serviceTagsMap == null ? null : serviceTagsMap.get(serviceName);

        if (ret == null) {
            ret = new ServiceTags();

            if (serviceTagsMap != null) {
                serviceTagsMap.put(serviceName, ret);
            }

            ret.setOp(ServiceTags.OP_ADD_OR_UPDATE);
            ret.setServiceName(serviceName);
        }

        return ret;
    }
    static private List<RangerTag> getTags(RangerOpenmetadataEntityWithTags entityWithTags) {
        List<RangerTag> ret = new ArrayList<>();

        if (entityWithTags != null && CollectionUtils.isNotEmpty(entityWithTags.getTags())) {
            List<TagLabel> tags = entityWithTags.getTags();

            for (TagLabel tag : tags) {
                RangerTag rangerTag = new RangerTag(null, tag.getName(), null, RangerTag.OWNER_SERVICERESOURCE);
                // Note: Openmetadata tags does not have attribute validtity period
                ret.add(rangerTag);
            }
        }

        return ret;
    }
    static private List<RangerTagDef> getTagDefs(RangerOpenmetadataEntityWithTags entityWithTags) {
        List<RangerTagDef> ret = new ArrayList<>();

        if (entityWithTags != null && CollectionUtils.isNotEmpty(entityWithTags.getTags())) {

            Map<String, String> tagNames = new HashMap<>();

            for (TagLabel tag : entityWithTags.getTags()) {

                if (!tagNames.containsKey(tag.getName())) {
                    tagNames.put(tag.getName(), tag.getName());
                    RangerTagDef tagDef = new RangerTagDef(tag.getName(), "Openmetadata");
                    //tagDef.getAttributeDefs().add(new RangerTagAttributeDef(null, tag.getName()));
                    ret.add(tagDef);
                    }
                }
            }
            return ret;
    }
}