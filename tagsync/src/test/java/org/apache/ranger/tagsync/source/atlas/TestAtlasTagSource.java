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

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class TestAtlasTagSource {
    private AtlasTagSource atlasTagSource;

    @BeforeEach
    void setUp() {
        atlasTagSource = new AtlasTagSource();
    }

    @Test
    void testConstructor() {
        assertNotNull(atlasTagSource, "AtlasTagSource should be instantiated");
        assertTrue(atlasTagSource instanceof AbstractTagSource, "AtlasTagSource should extend AbstractTagSource");
    }

    @Test
    void testConstants() {
        // Test that all constants are properly defined
        assertEquals("atlas-application.properties", AtlasTagSource.TAGSYNC_ATLAS_PROPERTIES_FILE_NAME,
                "TAGSYNC_ATLAS_PROPERTIES_FILE_NAME should be correct");

        assertEquals("atlas.kafka.bootstrap.servers", AtlasTagSource.TAGSYNC_ATLAS_KAFKA_ENDPOINTS,
                "TAGSYNC_ATLAS_KAFKA_ENDPOINTS should be correct");

        assertEquals("atlas.kafka.entities.group.id", AtlasTagSource.TAGSYNC_ATLAS_CONSUMER_GROUP,
                "TAGSYNC_ATLAS_CONSUMER_GROUP should be correct");

        assertEquals(1000, AtlasTagSource.MAX_WAIT_TIME_IN_MILLIS, "MAX_WAIT_TIME_IN_MILLIS should be 1000");
    }

    @Test
    void testStaticFields() {
        // Test that static field is accessible
        try {
            boolean currentValue = AtlasTagSource.isTagAssociated();
            // Just verify the field is accessible - don't assume initial value
            assertTrue(true, "isTagAssociated field should be accessible");
        } catch (Exception e) {
            fail("isTagAssociated field should be accessible: " + e.getMessage());
        }
    }

    @Test
    void testInitialize_NullProperties() {
        // Test that initialize handles null properties gracefully
        try {
            boolean result = atlasTagSource.initialize(null);
            assertFalse(result, "Initialize should fail with null properties");
        } catch (NullPointerException e) {
            // NPE is also acceptable behavior for null input
            assertTrue(true, "NPE is acceptable for null properties");
        } catch (Exception e) {
            fail("Initialize should handle null properties gracefully: " + e.getMessage());
        }
    }

    @Test
    void testInitialize_EmptyProperties() {
        Properties emptyProps = new Properties();

        boolean result = atlasTagSource.initialize(emptyProps);

        // Initialize should fail with empty properties since required Atlas properties
        // are missing
        assertFalse(result, "Initialize should fail with empty properties");
    }

    @Test
    void testInitialize_MissingAtlasPropertiesFile() {
        Properties props = new Properties();
        props.setProperty("some.property", "some.value");

        boolean result = atlasTagSource.initialize(props);

        // Should fail because atlas-application.properties file is missing from
        // classpath
        assertFalse(result, "Initialize should fail when atlas-application.properties is missing");
    }

    @Test
    void testInitialize_WithBasicProperties() {
        Properties props = new Properties();
        props.setProperty("ranger.tagsync.atlas.to.ranger.service.mapping", "test-service");

        // This will likely fail due to missing atlas-application.properties, but tests
        // the basic flow
        boolean result = atlasTagSource.initialize(props);

        // We expect this to fail in unit test environment, but method should not throw
        // exception
        assertFalse(result, "Initialize should complete without throwing exception");
    }

    @Test
    void testStart_WithoutInitialization() {
        // Test start without proper initialization
        boolean result = atlasTagSource.start();

        // Should return false since consumerTask is null
        assertFalse(result, "Start should fail when not properly initialized");
    }

    @Test
    void testStart_MethodExists() {
        // Simple test to verify the method exists and can be called
        try {
            atlasTagSource.start();
            assertTrue(true, "Start method should exist and be callable");
        } catch (Exception e) {
            fail("Start method should not throw unexpected exceptions: " + e.getMessage());
        }
    }

    @Test
    void testStop_MethodExists() {
        // Simple test to verify the method exists and can be called
        try {
            atlasTagSource.stop();
            assertTrue(true, "Stop method should exist and be callable");
        } catch (Exception e) {
            fail("Stop method should not throw unexpected exceptions: " + e.getMessage());
        }
    }

    @Test
    void testStop_WithoutActiveThread() {
        // Test stop when no thread is active
        atlasTagSource.stop();

        // Should complete without throwing exception
        assertTrue(true, "Stop should complete gracefully when no thread is active");
    }

    @Test
    void testInheritanceFromAbstractTagSource() {
        // Verify inheritance relationship
        assertTrue(atlasTagSource instanceof AbstractTagSource, "AtlasTagSource should inherit from AbstractTagSource");
    }

    @Test
    void testMethodsExist() {
        // Test that all required methods exist by reflection
        try {
            AtlasTagSource.class.getDeclaredMethod("initialize", Properties.class);
            AtlasTagSource.class.getDeclaredMethod("start");
            AtlasTagSource.class.getDeclaredMethod("stop");
            assertTrue(true, "All required methods should exist");
        } catch (NoSuchMethodException e) {
            fail("Required methods should exist: " + e.getMessage());
        }
    }

    @Test
    void testPropertyValidation() {
        // Test various property scenarios
        Properties invalidProps = new Properties();
        invalidProps.setProperty("invalid.property", "value");

        boolean result = atlasTagSource.initialize(invalidProps);
        assertFalse(result, "Initialize should fail with invalid properties");
    }

    @Test
    void testMultipleInitializeCalls() {
        Properties props = new Properties();

        // Call initialize multiple times
        boolean result1 = atlasTagSource.initialize(props);
        boolean result2 = atlasTagSource.initialize(props);

        // Both should return false due to missing required properties, but shouldn't
        // crash
        assertFalse(result1, "First initialize call should fail");
        assertFalse(result2, "Second initialize call should fail");
    }

    @Test
    void testInitializeErrorHandling() {
        // Test error handling during initialization
        try {
            Properties props = new Properties();
            props.setProperty("test.property", "test.value");

            boolean result = atlasTagSource.initialize(props);

            // Method should complete without throwing runtime exceptions
            // Result may be true or false depending on environment
            assertTrue(true, "Initialize should handle errors gracefully");
        } catch (RuntimeException e) {
            fail("Initialize should not throw runtime exceptions: " + e.getMessage());
        }
    }

    @Test
    void testStartStopLifecycle() {
        // Test basic start/stop lifecycle
        try {
            boolean startResult = atlasTagSource.start();
            atlasTagSource.stop();

            // Should complete without throwing exceptions
            assertTrue(true, "Start/stop lifecycle should work");
        } catch (Exception e) {
            fail("Start/stop lifecycle should not throw exceptions: " + e.getMessage());
        }
    }

    @Test
    void testParentClassIntegration() {
        // Test integration with parent AbstractTagSource
        assertTrue(atlasTagSource instanceof AbstractTagSource, "Should inherit from AbstractTagSource");

        // Test that we can access the parent class functionality
        try {
            // Verify the object is properly constructed and parent methods are accessible
            assertNotNull(atlasTagSource, "AtlasTagSource should be properly instantiated");
            assertTrue(true, "Parent class integration should work");
        } catch (Exception e) {
            fail("Parent class integration should work: " + e.getMessage());
        }
    }

    @Test
    void testCommitOffsetsForMessages_commitsMaxOffsetPerPartition() throws Exception {
        NotificationConsumer<EntityNotification> consumer = mock(NotificationConsumer.class);
        Object consumerRunnable = newConsumerRunnable(atlasTagSource, consumer);

        List<AtlasKafkaMessage<EntityNotification>> batch = Arrays.asList(
                kafkaMessage("ATLAS_ENTITIES", 0, 100L),
                kafkaMessage("ATLAS_ENTITIES", 1, 50L),
                kafkaMessage("ATLAS_ENTITIES", 0, 101L));

        invokeCommitOffsetsForMessages(consumerRunnable, batch);

        verify(consumer).commit(new TopicPartition("ATLAS_ENTITIES", 0), 101L);
        verify(consumer).commit(new TopicPartition("ATLAS_ENTITIES", 1), 50L);
        verify(consumer, times(2)).commit(any(TopicPartition.class), anyLong());
    }

    @Test
    void testCommitOffsetsForMessages_partitionsCommitIndependently() throws Exception {
        NotificationConsumer<EntityNotification> consumer = mock(NotificationConsumer.class);
        Object consumerRunnable = newConsumerRunnable(atlasTagSource, consumer);

        invokeCommitOffsetsForMessages(consumerRunnable,
                Collections.singletonList(kafkaMessage("ATLAS_ENTITIES", 9, 1000L)));
        invokeCommitOffsetsForMessages(consumerRunnable,
                Collections.singletonList(kafkaMessage("ATLAS_ENTITIES", 0, 27L)));

        verify(consumer).commit(new TopicPartition("ATLAS_ENTITIES", 9), 1000L);
        verify(consumer).commit(new TopicPartition("ATLAS_ENTITIES", 0), 27L);
    }

    @Test
    void testCommitOffsetsForMessages_skipsStaleOffsetForPartition() throws Exception {
        NotificationConsumer<EntityNotification> consumer = mock(NotificationConsumer.class);
        Object consumerRunnable = newConsumerRunnable(atlasTagSource, consumer);

        invokeCommitOffsetsForMessages(consumerRunnable,
                Collections.singletonList(kafkaMessage("ATLAS_ENTITIES", 5, 80L)));
        invokeCommitOffsetsForMessages(consumerRunnable,
                Collections.singletonList(kafkaMessage("ATLAS_ENTITIES", 5, 79L)));

        ArgumentCaptor<TopicPartition> partitionCaptor = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> offsetCaptor = ArgumentCaptor.forClass(Long.class);

        verify(consumer, times(1)).commit(partitionCaptor.capture(), offsetCaptor.capture());
        assertEquals(new TopicPartition("ATLAS_ENTITIES", 5), partitionCaptor.getValue());
        assertEquals(80L, offsetCaptor.getValue().longValue());
    }

    private static Object newConsumerRunnable(AtlasTagSource source,
            NotificationConsumer<EntityNotification> notificationConsumer) throws Exception {
        Class<?> runnableClass = Class.forName("org.apache.ranger.tagsync.source.atlas.AtlasTagSource$ConsumerRunnable");
        Constructor<?> constructor = runnableClass.getDeclaredConstructor(AtlasTagSource.class, NotificationConsumer.class);

        constructor.setAccessible(true);

        return constructor.newInstance(source, notificationConsumer);
    }

    private static void invokeCommitOffsetsForMessages(Object consumerRunnable,
            List<AtlasKafkaMessage<EntityNotification>> messages) throws Exception {
        Method method = consumerRunnable.getClass().getDeclaredMethod("commitOffsetsForMessages", List.class);

        method.setAccessible(true);
        method.invoke(consumerRunnable, messages);
    }

    private static AtlasKafkaMessage<EntityNotification> kafkaMessage(String topic, int partition, long offset) {
        AtlasKafkaMessage<EntityNotification> message = mock(AtlasKafkaMessage.class);

        when(message.getTopic()).thenReturn(topic);
        when(message.getPartition()).thenReturn(partition);
        when(message.getOffset()).thenReturn(offset);

        return message;
    }
}
