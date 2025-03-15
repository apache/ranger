
package org.apache.ranger.tagsync.source.openmetadatarest;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.*;

import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenmetadataResourceMapperUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OpenmetadataResourceMapperUtil.class);

    private static Map<String, OpenmetadataResourceMapper> openmetadataResourceMappers = new HashMap<String, OpenmetadataResourceMapper>();

    public static boolean isEntityTypeHandled(String entityTypeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isEntityTypeHandled(entityTypeName=" + entityTypeName + ")");
        }

        OpenmetadataResourceMapper mapper = openmetadataResourceMappers.get(entityTypeName);

        boolean ret = mapper != null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isEntityTypeHandled(entityTypeName=" + entityTypeName + ") : " + ret);
        }

        return ret;
    }

    public static RangerServiceResource getRangerServiceResource(RangerOpenmetadataEntity openmetadataEntity) {

        LOG.info("==> getRangerServiceResource(" + openmetadataEntity.getId() +")");

        RangerServiceResource resource = null;

        OpenmetadataResourceMapper mapper = openmetadataResourceMappers.get(openmetadataEntity.getType());

        LOG.info("==> getRangerServiceResource(" + openmetadataEntity.getType() +")");

        if (mapper != null) {
            try {
                resource = mapper.buildResource(openmetadataEntity);
            } catch (Exception exception) {
                LOG.error("Could not get serviceResource for openmetadata entity:" + openmetadataEntity.getId() + ": ", exception);
            }
        }

        LOG.debug("<== getRangerServiceResource(" + openmetadataEntity.getId() +"): resource=" + resource);

        return resource;
    }

    static public boolean initializeOpenmetadataResourceMappers(Properties properties) {
        final String MAPPER_NAME_DELIMITER = ",";

        String customMapperNames = TagSyncConfig.getCustomOpenmetadataRESTResourceMappers(properties);

        LOG.info("==> initializeOpenmetadataResourceMappers.initializeOpenmetadataResourceMappers(" + customMapperNames + ")");
        boolean ret = true;

        List<String> mapperNames = new ArrayList<String>();
        mapperNames.add("org.apache.ranger.tagsync.source.openmetadatarest.OpenmetadataTableMapper");
        if (StringUtils.isNotBlank(customMapperNames)) {
            for (String customMapperName : customMapperNames.split(MAPPER_NAME_DELIMITER)) {
                mapperNames.add(customMapperName.trim());
            }
        }

        for (String mapperName : mapperNames) {
            try {
                Class<?> clazz = Class.forName(mapperName);
                OpenmetadataResourceMapper resourceMapper = (OpenmetadataResourceMapper) clazz.newInstance();
                LOG.debug("INITIALIZING CLASS: " + resourceMapper);
                resourceMapper.initialize(properties);
                LOG.debug("INITIALIZED CLASS: " + resourceMapper);
                for (String entityTypeName : resourceMapper.getSupportedEntityTypes()) {
                    add(entityTypeName, resourceMapper);
                }

            } catch (Exception exception) {
                LOG.error("Failed to create OpenmetadataResourceMapper:" + mapperName + ": ", exception);
                ret = false;
            }
        }

        LOG.info("<== initializeOpenmetadataResourceMappers.initializeOpenmetadataResourceMappers(" + mapperNames + "): " + ret);
        
        return ret;
    }

    private static void add(String entityType, OpenmetadataResourceMapper mapper) {
       openmetadataResourceMappers.put(entityType, mapper);
   } 
}