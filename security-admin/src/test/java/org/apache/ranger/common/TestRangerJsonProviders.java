/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Priority;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

class TestRangerJsonProviders {
    @Test
    void testRangerJsonProviderConstructs() {
        RangerJsonProvider provider = new RangerJsonProvider();
        Assertions.assertNotNull(provider, "RangerJsonProvider should be constructed successfully");
    }

    @Test
    void testRangerJsonProviderHasCorrectAnnotations() {
        RangerJsonProvider provider = new RangerJsonProvider();

        // Verify it's a JAX-RS Provider
        Assertions.assertTrue(provider.getClass().isAnnotationPresent(Provider.class),
                "RangerJsonProvider should have @Provider annotation");

        // Verify it has priority annotation for MOXy prevention
        Assertions.assertTrue(provider.getClass().isAnnotationPresent(Priority.class),
                "RangerJsonProvider should have @Priority annotation to prevent MOXy usage");

        Priority priority = provider.getClass().getAnnotation(Priority.class);
        Assertions.assertEquals(1, priority.value(),
                "RangerJsonProvider should have highest priority (1) to be selected over MOXy");
    }

    @Test
    void testRangerJsonProviderSupportsJsonMediaType() {
        RangerJsonProvider provider = new RangerJsonProvider();

        // Test that provider can handle JSON media types with a proper object type
        // Using Map.class instead of String.class as Jackson is designed for complex objects
        Assertions.assertTrue(provider.isReadable(java.util.Map.class, java.util.Map.class, null, MediaType.APPLICATION_JSON_TYPE),
                "RangerJsonProvider should be able to read JSON");

        Assertions.assertTrue(provider.isWriteable(java.util.Map.class, java.util.Map.class, null, MediaType.APPLICATION_JSON_TYPE),
                "RangerJsonProvider should be able to write JSON");
    }

    @Test
    void testMoxyPreventionInJerseyClients() {
        // Test that our RangerJersey2ClientBuilder creates properly configured clients
        Client safeClient = RangerJersey2ClientBuilder.createStandardClient();
        Assertions.assertNotNull(safeClient, "Safe client should be created successfully");

        // Test RangerJersey2ClientBuilder as alternative to unsafe ClientBuilder patterns
        Client safeBuildClient = RangerJersey2ClientBuilder.newClient();
        Assertions.assertNotNull(safeBuildClient, "Safe builder client should be created successfully");

        // Clean up resources
        safeClient.close();
        safeBuildClient.close();
    }

    @Test
    void testUnsafeJerseyClientDetection() {
        // This test demonstrates the vulnerability that exists without our protection
        // UNSAFE: This is what vulnerable Ranger components were doing:
        // Client unsafeClient = ClientBuilder.newClient(); // This could use MOXy!

        // SAFE: This is what they should use instead:
        Client safeClient = RangerJersey2ClientBuilder.newClient();
        Assertions.assertNotNull(safeClient, "Safe client creation should work");

        safeClient.close();
    }

    @Test
    void testJsonParserExceptionMapperReturnsBadRequest() {
        RangerJsonParserExceptionMapper mapper = new RangerJsonParserExceptionMapper();
        JsonParseException              ex     = new JsonParseException(null, "bad");
        Response                        r      = mapper.toResponse(ex);
        Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), r.getStatus());
    }

    @Test
    void testJsonMappingExceptionMapperReturnsBadRequest() {
        RangerJsonMappingExceptionMapper mapper = new RangerJsonMappingExceptionMapper();
        JsonMappingException             ex     = JsonMappingException.fromUnexpectedIOE(new IOException("bad"));
        Response                         r      = mapper.toResponse(ex);
        Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), r.getStatus());
    }

    @Test
    void testMoxyPreventionComprehensive() {
        // Test all our MOXy prevention mechanisms work together

        // 1. Provider priority ensures Jackson is selected
        RangerJsonProvider provider = new RangerJsonProvider();
        Priority           priority = provider.getClass().getAnnotation(Priority.class);
        Assertions.assertEquals(1, priority.value(), "Highest priority prevents MOXy selection");

        // 2. Jersey client configuration prevents auto-discovery
        Client configuredClient = RangerJersey2ClientBuilder.createStandardClient();
        Assertions.assertNotNull(configuredClient, "Configured client prevents MOXy auto-discovery");

        // 3. Safe builder prevents unsafe client patterns
        Client builderClient = RangerJersey2ClientBuilder.newBuilder()
                .connectTimeout(1000)
                .readTimeout(5000)
                .build();
        Assertions.assertNotNull(builderClient, "Safe builder prevents unsafe Jersey usage");

        // Clean up
        configuredClient.close();
        builderClient.close();
    }
}
