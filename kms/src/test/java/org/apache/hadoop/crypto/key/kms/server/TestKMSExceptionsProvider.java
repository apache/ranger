/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.core.Response;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMSExceptionsProvider {
    @Test
    public void testGetOneLineMessage() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        KMSExceptionsProvider kmsExceptionsProvider = new KMSExceptionsProvider();

        // Mock an exception with a valid message
        Throwable exception = mock(Throwable.class);
        when(exception.getMessage()).thenReturn("Simulated exception message\nWith new line");

        Method method = KMSExceptionsProvider.class.getDeclaredMethod("getOneLineMessage", Throwable.class);
        method.setAccessible(true);
        String message = (String) method.invoke(kmsExceptionsProvider, exception);

        // Verify that the message is not null or empty
        if (message == null || message.isEmpty()) {
            throw new AssertionError("The one-line message should not be null or empty");
        }
    }

    @Test
    public void testCreateResponse() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        KMSExceptionsProvider kmsExceptionsProvider = new KMSExceptionsProvider();

        // Mock an exception with a valid message
        Throwable       exception = mock(Throwable.class);
        Response.Status status    = Response.Status.INTERNAL_SERVER_ERROR;

        when(exception.getMessage()).thenReturn("Simulated exception message");

        Method method = KMSExceptionsProvider.class.getDeclaredMethod("createResponse", Response.Status.class, Throwable.class);
        method.setAccessible(true);

        Object response = method.invoke(kmsExceptionsProvider, status, exception);

        if (response == null) {
            throw new AssertionError("The response should not be null");
        }

        if (!(response instanceof Response)) {
            throw new AssertionError("The response should be of type javax.ws.rs.core.Response");
        }

        Response jerseyResponse = (Response) response;

        if (jerseyResponse.getStatus() != Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            throw new AssertionError("Expected HTTP 500 INTERNAL_SERVER_ERROR");
        }
    }

    @Test
    public void testToResponse() {
        try {
            KMSExceptionsProvider kmsExceptionsProvider = new KMSExceptionsProvider();
            Exception             exception             = new Exception("Simulated exception message");
            Response              response              = kmsExceptionsProvider.toResponse(exception);

            if (response == null) {
                throw new AssertionError("Response is null");
            }

            if (response.getStatus() != Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
                throw new AssertionError("Unexpected status");
            }
        } catch (Exception e) {
            e.printStackTrace(); // Full trace
        }
    }

    @Test
    public void testLog() {
        KMSExceptionsProvider kmsExceptionsProvider = new KMSExceptionsProvider();
        Throwable             exception             = mock(Throwable.class);
        Response.Status       status                = Response.Status.INTERNAL_SERVER_ERROR;

        try {
            Method method = KMSExceptionsProvider.class.getDeclaredMethod("log", Response.Status.class, Throwable.class);
            method.setAccessible(true);
            method.invoke(kmsExceptionsProvider, status, exception);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace(); // Full trace
        }
    }
}
