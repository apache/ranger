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

import org.apache.ranger.audit.destination.OpenSearchAuditDestination;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAuditOpenSearchDispatcher {
    private static final String TEST_DISPATCHER_GROUP = "test-dispatcher-group";
    private static final String TEST_TOPIC            = "ranger_audits";

    @Mock
    private OpenSearchAuditDestination openSearchAuditDestination;

    private AuditOpenSearchDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new AuditOpenSearchDispatcher(openSearchAuditDestination, TEST_DISPATCHER_GROUP, null, TEST_TOPIC);
    }

    @Test
    void processMessageBatch_delegatesToDestinationLogJSON() throws Exception {
        List<String> audits = Collections.singletonList("{\"eventId\":\"id-1\"}");

        when(openSearchAuditDestination.logJSON(anyCollection())).thenReturn(true);

        invokeProcessMessageBatch(audits);

        verify(openSearchAuditDestination).logJSON(audits);
    }

    @Test
    void processMessageBatch_throwsWhenDestinationReportsFailure() {
        List<String> audits = Collections.singletonList("{\"eventId\":\"id-1\"}");

        when(openSearchAuditDestination.logJSON(anyCollection())).thenReturn(false);

        assertThrows(Exception.class, () -> invokeProcessMessageBatch(audits));
    }

    @Test
    void processMessageBatch_throwsOnNullInputWithoutDelegating() {
        verifyNoInteractions(openSearchAuditDestination);
    }

    @Test
    void processMessageBatch_throwsOnEmptyInputWithoutDelegating() {
        verify(openSearchAuditDestination, never()).logJSON(anyCollection());
    }

    private void invokeProcessMessageBatch(Collection<String> audits) throws Exception {
        Method method = AuditOpenSearchDispatcher.class.getDeclaredMethod("processMessageBatch", Collection.class);
        method.setAccessible(true);
        try {
            method.invoke(dispatcher, audits);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }
}
