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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class AtlasTagSourceConfigTest {
    private final AtlasTagSource tagSource = new AtlasTagSource();

    @Test
    public void validateRequiredAtlasKafkaProperties_acceptsBootstrapAndGroupWithoutZookeeper() {
        Properties atlasProperties = new Properties();

        atlasProperties.setProperty(AtlasTagSource.TAGSYNC_ATLAS_KAFKA_ENDPOINTS, "kafka1:9092,kafka2:9092");
        atlasProperties.setProperty(AtlasTagSource.TAGSYNC_ATLAS_CONSUMER_GROUP, "ranger_entities_consumer");

        Assertions.assertTrue(tagSource.validateRequiredAtlasKafkaProperties(atlasProperties));
    }

    @Test
    public void validateRequiredAtlasKafkaProperties_rejectsMissingBootstrapServers() {
        Properties atlasProperties = new Properties();

        atlasProperties.setProperty(AtlasTagSource.TAGSYNC_ATLAS_CONSUMER_GROUP, "ranger_entities_consumer");

        Assertions.assertFalse(tagSource.validateRequiredAtlasKafkaProperties(atlasProperties));
    }

    @Test
    public void validateRequiredAtlasKafkaProperties_rejectsMissingConsumerGroup() {
        Properties atlasProperties = new Properties();

        atlasProperties.setProperty(AtlasTagSource.TAGSYNC_ATLAS_KAFKA_ENDPOINTS, "kafka1:9092");

        Assertions.assertFalse(tagSource.validateRequiredAtlasKafkaProperties(atlasProperties));
    }
}
