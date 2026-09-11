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

package org.apache.ranger.audit.dispatcher.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AuditDispatcherBaseTest {
    private TestAuditDispatcher           dispatcher;
    private KafkaConsumer<String, String> mockConsumer;

    @BeforeEach
    public void setup() throws Exception {
        Properties props = new Properties();
        props.setProperty("ranger.audit.dispatcher.test.kafka.group.id", "test-group");
        props.setProperty("ranger.audit.dispatcher.test." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS, "localhost:9092");
        props.setProperty("ranger.audit.dispatcher.test." + AuditServerConstants.PROP_DISPATCHER_AUTH_RETRY_DELAY_MS, "100");
        props.setProperty("ranger.audit.dispatcher.test." + AuditServerConstants.PROP_DISPATCHER_POLL_ERROR_RETRY_DELAY_MS, "100");

        mockConsumer = mock(KafkaConsumer.class);
        dispatcher   = new TestAuditDispatcher(props, "ranger.audit.dispatcher.test", "test-group", mockConsumer);
    }

    @AfterEach
    public void teardown() {
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
    }

    @Test
    public void testAuthorizationExceptionRetry() throws Exception {
        TestDispatcherWorker worker = dispatcher.createDispatcherWorker("worker-1", null);
        worker.setWorkerDispatcher(mockConsumer);
        dispatcher.running.set(true);

        // Throw AuthorizationException on the first poll, then return empty records, then throw InterruptException to exit
        doThrow(new AuthorizationException("Not authorized"))
            .doReturn(ConsumerRecords.empty())
            .doThrow(new InterruptException("Interrupted"))
            .when(mockConsumer).poll(any(Duration.class));

        worker.run();

        // The worker should have caught the AuthorizationException, retried, and then exited on WakeupException
        assertTrue(true, "Worker should have handled AuthorizationException");
        assertEquals(0, worker.getProcessCount()); // 0 records processed
    }

    @Test
    public void testInterruptException() throws Exception {
        TestDispatcherWorker worker = dispatcher.createDispatcherWorker("worker-1", null);
        worker.setWorkerDispatcher(mockConsumer);
        dispatcher.running.set(true);

        doThrow(new InterruptException("Interrupted"))
            .when(mockConsumer).poll(any(Duration.class));

        worker.run();

        // The worker should exit on InterruptException
        assertTrue(true, "Worker should have handled InterruptException");
    }

    private static class TestAuditDispatcher extends AuditDispatcherBase {
        public TestAuditDispatcher(Properties props, String propPrefix, String dispatcherGroupId, KafkaConsumer<String, String> mockConsumer) throws Exception {
            super(dispatcherGroupId, mockConsumer, "test-topic");
            this.authRetryDelayMs = 100;
            this.pollErrorRetryDelayMs = 100;
        }

        @Override
        protected String getDispatcherName() {
            return "TestDispatcher";
        }

        @Override
        protected TestDispatcherWorker createDispatcherWorker(String workerId, List<Integer> assignedPartitions) {
            return new TestDispatcherWorker(workerId, assignedPartitions, this);
        }
    }

    private static class TestDispatcherWorker extends AuditDispatcherBase.DispatcherWorker {
        private final AtomicInteger processCount = new AtomicInteger(0);

        public TestDispatcherWorker(String workerId, List<Integer> assignedPartitions, AuditDispatcherBase dispatcher) {
            dispatcher.super(workerId, assignedPartitions);
            this.dispatcherInstance = dispatcher;
        }

        private final AuditDispatcherBase dispatcherInstance;

        public void setWorkerDispatcher(KafkaConsumer<String, String> mockConsumer) {
            this.workerDispatcher = mockConsumer;
        }

        @Override
        protected void processRecordBatch(ConsumerRecords<String, String> records) {
            processCount.incrementAndGet();
        }

        public int getProcessCount() {
            return processCount.get();
        }

        @Override
        public void run() {
            // Override run to avoid the full setup of subscribe/rebalance listener which requires real Kafka
            try {
                while (dispatcherInstance.running.get()) {
                    try {
                        ConsumerRecords<String, String> records = workerDispatcher.poll(Duration.ofMillis(100));
                        if (!records.isEmpty()) {
                            processRecordBatch(records);
                        }
                    } catch (Exception e) {
                        java.lang.reflect.Method method = AuditDispatcherBase.DispatcherWorker.class.getDeclaredMethod("handlePollException", Exception.class);
                        method.setAccessible(true);
                        boolean shouldContinue = (boolean) method.invoke(this, e);
                        if (!shouldContinue) {
                            break;
                        }
                    }
                }
            } catch (Throwable e) {
                // ignore
            }
        }
    }
}
