package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Parses Metadata Registry {@code udf.governance} Kafka payloads.
 */
public final class MetadataRegistryNotificationParser {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataRegistryNotificationParser.class);

    private MetadataRegistryNotificationParser() {}

    public static MetadataRegistryNotificationWrapper parse(String payload) {
        if (StringUtils.isBlank(payload)) {
            return null;
        }
        try {
            JsonNode root    = JsonUtils.getMapper().readTree(payload);
            JsonNode message = root.path(MetadataRegistryNotificationFields.MESSAGE);
            if (message.isMissingNode()) {
                LOG.warn("Metadata Registry notification missing message node");
                return null;
            }
            if (!MetadataRegistryNotificationFields.MESSAGE_TYPE_VALUE.equals(
                    message.path(MetadataRegistryNotificationFields.MESSAGE_TYPE).asText(null))) {
                LOG.debug("Ignoring non Metadata Registry notification type");
                return null;
            }

            String operationType = message.path(MetadataRegistryNotificationFields.OPERATION_TYPE).asText(null);
            NotificationOpType opType = toOpType(operationType);
            if (opType == NotificationOpType.UNKNOWN) {
                LOG.warn("Unsupported Metadata Registry operationType={}", operationType);
                return null;
            }

            JsonNode              entityNode = message.path(MetadataRegistryNotificationFields.ENTITY);
            MetadataRegistryEntity entity    = toEntity(entityNode);
            if (entity == null) {
                LOG.warn("Metadata Registry notification missing entity");
                return null;
            }

            List<MetadataRegistryClassification> classifications = toClassifications(
                    entityNode.path(MetadataRegistryNotificationFields.CLASSIFICATIONS));

            return new MetadataRegistryNotificationWrapper(entity, classifications, opType);
        } catch (Exception exception) {
            LOG.error("Failed to parse Metadata Registry notification payload", exception);
            return null;
        }
    }

    private static NotificationOpType toOpType(String operationType) {
        if (operationType == null) {
            return NotificationOpType.UNKNOWN;
        }
        try {
            return NotificationOpType.valueOf(operationType);
        } catch (IllegalArgumentException exception) {
            return NotificationOpType.UNKNOWN;
        }
    }

    private static MetadataRegistryEntity toEntity(JsonNode entityNode) {
        if (entityNode == null || entityNode.isMissingNode()) {
            return null;
        }
        String urn = entityNode.path(MetadataRegistryNotificationFields.URN).asText(null);
        if (StringUtils.isBlank(urn)) {
            return null;
        }
        Long entityId = entityNode.hasNonNull(MetadataRegistryNotificationFields.ENTITY_ID)
                ? entityNode.path(MetadataRegistryNotificationFields.ENTITY_ID).asLong()
                : null;
        return new MetadataRegistryEntity(
                urn,
                entityId,
                entityNode.path(MetadataRegistryNotificationFields.ENTITY_TYPE).asText(null),
                entityNode.path(MetadataRegistryNotificationFields.FIELD_PATH).asText(""),
                entityNode.path(MetadataRegistryNotificationFields.NAME).asText(null),
                entityNode.path(MetadataRegistryNotificationFields.DISPLAY_STRING).asText(null),
                entityNode.path(MetadataRegistryNotificationFields.DEPLOYMENT).asText(null),
                entityNode.path(MetadataRegistryNotificationFields.TECHNOLOGY).asText(null));
    }

    private static List<MetadataRegistryClassification> toClassifications(JsonNode classificationsNode) {
        List<MetadataRegistryClassification> classifications = new ArrayList<>();
        if (classificationsNode == null || !classificationsNode.isArray()) {
            return classifications;
        }
        for (JsonNode node : classificationsNode) {
            String typeName = node.path(MetadataRegistryNotificationFields.TYPE_NAME).asText(null);
            if (StringUtils.isBlank(typeName)) {
                continue;
            }
            Map<String, String> attributes = new HashMap<>();
            JsonNode              attrsNode  = node.path(MetadataRegistryNotificationFields.ATTRIBUTES);
            if (attrsNode.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = attrsNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    if (!entry.getValue().isNull()) {
                        attributes.put(entry.getKey(), entry.getValue().asText());
                    }
                }
            }
            classifications.add(new MetadataRegistryClassification(typeName, attributes));
        }
        return classifications;
    }

    public enum NotificationOpType {
        ENTITY_CREATE,
        ENTITY_UPDATE,
        ENTITY_DELETE,
        CLASSIFICATION_ADD,
        CLASSIFICATION_UPDATE,
        CLASSIFICATION_DELETE,
        UNKNOWN
    }

    public static final class MetadataRegistryClassification {
        private final String              typeName;
        private final Map<String, String> attributes;

        public MetadataRegistryClassification(String typeName, Map<String, String> attributes) {
            this.typeName   = typeName;
            this.attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        }

        public String getTypeName() {
            return typeName;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public String getTagName() {
            if (typeName == null) {
                return "UNKNOWN";
            }
            int colon = typeName.indexOf(':');
            return colon >= 0 ? typeName.substring(colon + 1).toUpperCase() : typeName.toUpperCase();
        }
    }
}
