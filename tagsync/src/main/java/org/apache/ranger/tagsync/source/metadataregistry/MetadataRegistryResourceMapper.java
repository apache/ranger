package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.Properties;

/**
 * Maps Metadata Registry entity types to Ranger service resources.
 */
public abstract class MetadataRegistryResourceMapper {
    public static final String TAGSYNC_DEFAULT_CLUSTER_NAME =
            "ranger.tagsync.metadataregistry.default.cluster.name";
    public static final String TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX =
            "ranger.tagsync.metadataregistry.";
    public static final String TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX =
            ".ranger.service";
    public static final String TAGSYNC_CLUSTER_IDENTIFIER = ".instance.";
    public static final String TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR = "_";

    protected final String   componentName;
    protected final String[] supportedEntityTypes;

    protected Properties properties;
    protected String     defaultClusterName;

    protected MetadataRegistryResourceMapper(String componentName, String[] supportedEntityTypes) {
        this.componentName        = componentName;
        this.supportedEntityTypes = supportedEntityTypes;
    }

    public final String getComponentName() {
        return componentName;
    }

    public final String[] getSupportedEntityTypes() {
        return supportedEntityTypes;
    }

    public void initialize(Properties properties) {
        this.properties         = properties;
        this.defaultClusterName = properties != null ? properties.getProperty(TAGSYNC_DEFAULT_CLUSTER_NAME) : null;
    }

    public abstract RangerServiceResource buildResource(MetadataRegistryEntity entity) throws Exception;

    protected String getRangerServiceName(String deployment) {
        String serviceName = getCustomRangerServiceName(deployment);
        if (StringUtils.isBlank(serviceName)) {
            serviceName = deployment + TAGSYNC_DEFAULT_CLUSTERNAME_AND_COMPONENTNAME_SEPARATOR + componentName;
        }
        return serviceName;
    }

    protected String getCustomRangerServiceName(String deployment) {
        if (properties == null || StringUtils.isBlank(deployment)) {
            return null;
        }
        String propName = TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX + componentName
                + TAGSYNC_CLUSTER_IDENTIFIER + deployment
                + TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX;
        return properties.getProperty(propName);
    }

    protected void throwExceptionWithMessage(String message) throws Exception {
        throw new Exception(message);
    }
}
