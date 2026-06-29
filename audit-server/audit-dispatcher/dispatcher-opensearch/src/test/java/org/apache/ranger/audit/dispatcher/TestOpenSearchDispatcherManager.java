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

package org.apache.ranger.audit.dispatcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestOpenSearchDispatcherManager {
    @AfterEach
    void clearSystemProperty() {
        System.clearProperty("ranger.audit.dispatcher.type");
    }

    @Test
    void init_skipsWhenDispatcherTypeIsNotOpenSearch() {
        System.setProperty("ranger.audit.dispatcher.type", "solr");

        OpenSearchDispatcherManager manager = new OpenSearchDispatcherManager();
        Properties props = new Properties();

        assertDoesNotThrow(() -> manager.init(props));
    }

    @Test
    void init_throwsWhenPropsAreNull() {
        OpenSearchDispatcherManager manager = new OpenSearchDispatcherManager();

        assertThrows(RuntimeException.class, () -> manager.init(null));
    }

    @Test
    void init_skipsWhenOpenSearchDestinationDisabled() {
        OpenSearchDispatcherManager manager = new OpenSearchDispatcherManager();
        Properties props = new Properties();
        props.setProperty("xasecure.audit.destination.opensearch", "false");

        assertDoesNotThrow(() -> manager.init(props));
    }

    @Test
    void shutdown_handlesNullDispatcherGracefully() {
        OpenSearchDispatcherManager manager = new OpenSearchDispatcherManager();

        assertDoesNotThrow(manager::shutdown);
    }

    @Test
    void init_throwsWhenEnabledDispatcherClassCannotBeCreated() {
        OpenSearchDispatcherManager manager = new OpenSearchDispatcherManager();
        Properties props = new Properties();
        props.setProperty("xasecure.audit.destination.opensearch", "true");
        props.setProperty("ranger.audit.dispatcher.class", "com.nonexistent.FakeDispatcher");
        props.setProperty("ranger.audit.dispatcher.kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("ranger.audit.dispatcher.kafka.topic", "ranger_audits");

        assertThrows(RuntimeException.class, () -> manager.init(props));
    }
}
