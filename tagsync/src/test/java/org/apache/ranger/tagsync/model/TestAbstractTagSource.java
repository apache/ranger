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

package org.apache.ranger.tagsync.model;

import org.apache.ranger.plugin.util.ServiceTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAbstractTagSource {
    @Mock
    private TagSink           mockTagSink;
    private ConcreteTagSource tagSource;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        tagSource = new ConcreteTagSource();
    }

    @Test
    public void test01SetTagSinkStoresCorrectly() {
        tagSource.setTagSink(mockTagSink);
        assertEquals(mockTagSink, tagSource.getStoredSink());
    }

    @Test
    public void test02InitializeWithValidProperties() {
        Properties props = new Properties();
        props.setProperty("test.property", "value");
        assertTrue(tagSource.initialize(props));
    }

    @Test
    public void test03InitializeWithNullProperties() {
        assertFalse(tagSource.initialize(null));
    }

    @Test
    public void test04StartReturnsTrue() {
        assertTrue(tagSource.start());
    }

    @Test
    public void test05StopDoesNotThrowException() {
        tagSource.stop();
        assertNotNull(tagSource);
    }

    @Test
    public void test06UpdateSinkWithValidServiceTags() throws Exception {
        tagSource.setTagSink(mockTagSink);
        ServiceTags serviceTags = new ServiceTags();
        serviceTags.setServiceName("test_service");

        tagSource.updateSink(serviceTags);
        assertNotNull(serviceTags);
    }

    @Test
    public void test07UpdateSinkHandlesNullServiceTags() throws Exception {
        tagSource.setTagSink(mockTagSink);
        tagSource.updateSink(null);
        assertNotNull(tagSource);
    }

    @Test
    public void test08GetNameReturnsSetName() {
        String testName = "TestSource";
        tagSource.setName(testName);
        assertEquals(testName, tagSource.getName());
    }

    public static class ConcreteTagSource extends AbstractTagSource {
        private TagSink storedSink;

        @Override
        public boolean initialize(Properties properties) {
            return properties != null;
        }

        @Override
        public boolean start() {
            return true;
        }

        @Override
        public void stop() {
        }

        @Override
        public void setTagSink(TagSink sink) {
            this.storedSink = sink;
            super.setTagSink(sink);
        }

        public TagSink getStoredSink() {
            return storedSink;
        }
    }
}
