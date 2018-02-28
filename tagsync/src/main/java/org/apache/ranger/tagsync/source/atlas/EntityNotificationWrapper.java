/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.atlas;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.atlas.v1.model.notification.EntityNotificationV2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntity;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityNotificationWrapper {
	private static final Log LOG = LogFactory.getLog(EntityNotificationWrapper.class);

	public enum NotificationType { UNKNOWN, ENTITY_CREATE, ENTITY_UPDATE, ENTITY_DELETE, CLASSIFICATION_ADD, CLASSIFICATION_UPDATE, CLASSIFICATION_DELETE}

	private final EntityNotification notification;
	private final EntityNotification.EntityNotificationType notificationType;
	private final RangerAtlasEntity rangerAtlasEntity;
	private final String entityTypeName;
	private final boolean isEntityTypeHandled;
	private final boolean isEntityDeleteOp;
	private final boolean isEmptyClassifications;

	EntityNotificationWrapper(@Nonnull EntityNotification notification) {
		this.notification = notification;
		notificationType = this.notification.getType();

		switch (notificationType) {
			case ENTITY_NOTIFICATION_V2: {

				EntityNotificationV2 v2Notification = (EntityNotificationV2) notification;
				AtlasEntity atlasEntity = v2Notification.getEntity();
				String guid = atlasEntity.getGuid();
				String typeName = atlasEntity.getTypeName();

				rangerAtlasEntity = new RangerAtlasEntity(typeName, guid, atlasEntity.getAttributes());
				entityTypeName = atlasEntity.getTypeName();
				isEntityTypeHandled = atlasEntity.getStatus() == AtlasEntity.Status.ACTIVE
						&& AtlasResourceMapperUtil.isEntityTypeHandled(entityTypeName);
				isEntityDeleteOp = EntityNotificationV2.OperationType.ENTITY_DELETE == v2Notification.getOperationType();
				isEmptyClassifications = CollectionUtils.isNotEmpty(v2Notification.getClassifications());
			}
			break;
			case ENTITY_NOTIFICATION_V1: {
				EntityNotificationV1 v1Notification = (EntityNotificationV1) notification;

				Referenceable atlasEntity = v1Notification.getEntity();
				String guid = atlasEntity.getId()._getId();
				String typeName = atlasEntity.getTypeName();

				rangerAtlasEntity = new RangerAtlasEntity(typeName, guid, atlasEntity.getValues());
				entityTypeName = atlasEntity.getTypeName();
				isEntityTypeHandled = atlasEntity.getId().getState() == Id.EntityState.ACTIVE
						&& AtlasResourceMapperUtil.isEntityTypeHandled(entityTypeName);
				isEntityDeleteOp = EntityNotificationV1.OperationType.ENTITY_DELETE == v1Notification.getOperationType();
				isEmptyClassifications = CollectionUtils.isNotEmpty(v1Notification.getAllTraits());
			}
			break;
			default: {
				LOG.error("Unknown notification type - [" + notificationType + "]");

				rangerAtlasEntity = null;
				entityTypeName = null;
				isEntityTypeHandled = false;
				isEntityDeleteOp = false;
				isEmptyClassifications = true;
			}

			break;
		}
	}

	public RangerAtlasEntity getRangerAtlasEntity() {
		return rangerAtlasEntity;
	}

	public String getEntityTypeName() {
		return entityTypeName;
	}

	public boolean getIsEntityTypeHandled() {
		return isEntityTypeHandled;
	}

	public boolean getIsEntityDeleteOp() {
		return isEntityDeleteOp;
	}

	public boolean getIsEmptyClassifications() {
		return isEmptyClassifications;
	}

	public NotificationType getEntityNotificationType() {
		NotificationType ret = NotificationType.UNKNOWN;

		switch (notificationType) {
			case ENTITY_NOTIFICATION_V2: {
				EntityNotificationV2.OperationType opType = ((EntityNotificationV2) notification).getOperationType();
				switch (opType) {
					case ENTITY_CREATE:
						ret = NotificationType.ENTITY_CREATE;
						break;
					case ENTITY_UPDATE:
						ret = NotificationType.ENTITY_UPDATE;
						break;
					case ENTITY_DELETE:
						ret = NotificationType.ENTITY_DELETE;
						break;
					case CLASSIFICATION_ADD:
						ret = NotificationType.CLASSIFICATION_ADD;
						break;
					case CLASSIFICATION_UPDATE:
						ret = NotificationType.CLASSIFICATION_UPDATE;
						break;
					case CLASSIFICATION_DELETE:
						ret = NotificationType.CLASSIFICATION_DELETE;
						break;
					default:
						LOG.error("Received OperationType [" + opType + "], converting to UNKNOWN");
						break;
				}
				break;
			}
			case ENTITY_NOTIFICATION_V1: {
				EntityNotificationV1.OperationType opType = ((EntityNotificationV1) notification).getOperationType();
				switch (opType) {
					case ENTITY_CREATE:
						ret = NotificationType.ENTITY_CREATE;
						break;
					case ENTITY_UPDATE:
						ret = NotificationType.ENTITY_UPDATE;
						break;
					case ENTITY_DELETE:
						ret = NotificationType.ENTITY_DELETE;
						break;
					case TRAIT_ADD:
						ret = NotificationType.CLASSIFICATION_ADD;
						break;
					case TRAIT_UPDATE:
						ret = NotificationType.CLASSIFICATION_UPDATE;
						break;
					case TRAIT_DELETE:
						ret = NotificationType.CLASSIFICATION_DELETE;
						break;
					default:
						LOG.error("Received OperationType [" + opType + "], converting to UNKNOWN");
						break;
				}
				break;
			}
			default: {
				LOG.error("Unknown notification type - [" + notificationType + "]");
			}
			break;
		}

		return ret;
	}

	public Map<String, Map<String, String>> getAllClassifications() {
		Map<String, Map<String, String>> ret = new HashMap<>();

		switch (notificationType) {
			case ENTITY_NOTIFICATION_V2: {
				List<AtlasClassification> allClassifications = ((EntityNotificationV2) notification).getClassifications();
				if (CollectionUtils.isNotEmpty(allClassifications)) {
					for (AtlasClassification classification : allClassifications) {
						String classificationName = classification.getTypeName();

						Map<String, Object> valuesMap = classification.getAttributes();
						Map<String, String> attributes = new HashMap<>();
						if (valuesMap != null) {
							for (Map.Entry<String, Object> value : valuesMap.entrySet()) {
								if (value.getValue() != null) {
									attributes.put(value.getKey(), value.getValue().toString());
								}
							}
						}
						ret.put(classificationName, attributes);
					}
				}
			}
			break;
			case ENTITY_NOTIFICATION_V1: {
				List<Struct> allTraits = ((EntityNotificationV1) notification).getAllTraits();
				if (CollectionUtils.isNotEmpty(allTraits)) {
					for (Struct trait : allTraits) {
						String traitName = trait.getTypeName();

						Map<String, Object> valuesMap = trait.getValuesMap();
						Map<String, String> attributes = new HashMap<>();
						if (valuesMap != null) {
							for (Map.Entry<String, Object> value : valuesMap.entrySet()) {
								if (value.getValue() != null) {
									attributes.put(value.getKey(), value.getValue().toString());
								}
							}
						}
						ret.put(traitName, attributes);
					}
				}
			}
			break;
			default: {
				LOG.error("Unknown notification type - [" + notificationType + "]");
			}
			break;
		}

		return ret;
	}

}
