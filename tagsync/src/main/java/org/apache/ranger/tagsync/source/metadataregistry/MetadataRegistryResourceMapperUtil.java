package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class MetadataRegistryResourceMapperUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataRegistryResourceMapperUtil.class);

    private static final Map<String, MetadataRegistryResourceMapper> resourceMappers = new HashMap<>();

    private MetadataRegistryResourceMapperUtil() {}

    public static boolean isEntityTypeHandled(String entityType) {
        return resourceMappers.containsKey(entityType);
    }

    public static boolean isEntityTypeHandled(MetadataRegistryEntity entity) {
        if (entity == null) {
            return false;
        }
        if (MetadataRegistryHiveResourceMapper.isColumnFieldPath(entity.getFieldPath())) {
            return isEntityTypeHandled(MetadataRegistryHiveResourceMapper.ENTITY_TYPE_HIVE_TABLE);
        }
        return isEntityTypeHandled(entity.getEntityType());
    }

    public static RangerServiceResource getRangerServiceResource(MetadataRegistryEntity entity) {
        if (entity == null) {
            return null;
        }
        MetadataRegistryResourceMapper mapper = resolveMapper(entity);
        if (mapper == null) {
            return null;
        }
        try {
            return mapper.buildResource(entity);
        } catch (Exception exception) {
            LOG.error("Could not build Ranger service resource for urn={}", entity.getUrn(), exception);
            return null;
        }
    }

    public static boolean initializeResourceMappers(Properties properties) {
        String customMapperNames = properties == null
                ? null
                : properties.getProperty("ranger.tagsync.metadataregistry.custom.resource.mappers");

        boolean ret = true;
        List<String> mapperNames = new ArrayList<>();
        mapperNames.add(MetadataRegistryHiveResourceMapper.class.getName());

        if (customMapperNames != null && !customMapperNames.isBlank()) {
            for (String mapperName : customMapperNames.split(",")) {
                mapperNames.add(mapperName.trim());
            }
        }

        resourceMappers.clear();

        for (String mapperName : mapperNames) {
            if (mapperName.isBlank()) {
                continue;
            }
            try {
                Class<?>                       clazz  = Class.forName(mapperName);
                MetadataRegistryResourceMapper mapper = (MetadataRegistryResourceMapper) clazz.getDeclaredConstructor().newInstance();
                mapper.initialize(properties);
                for (String entityType : mapper.getSupportedEntityTypes()) {
                    resourceMappers.put(entityType, mapper);
                }
            } catch (Exception exception) {
                LOG.error("Failed to create MetadataRegistryResourceMapper: {}", mapperName, exception);
                ret = false;
            }
        }

        return ret;
    }

    private static MetadataRegistryResourceMapper resolveMapper(MetadataRegistryEntity entity) {
        if (MetadataRegistryHiveResourceMapper.isColumnFieldPath(entity.getFieldPath())) {
            return resourceMappers.get(MetadataRegistryHiveResourceMapper.ENTITY_TYPE_HIVE_TABLE);
        }
        return resourceMappers.get(entity.getEntityType());
    }
}
