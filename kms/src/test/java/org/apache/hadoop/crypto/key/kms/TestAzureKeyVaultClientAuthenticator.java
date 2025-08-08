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
package org.apache.hadoop.crypto.key.kms;

import com.microsoft.azure.keyvault.KeyVaultClient;
import org.apache.hadoop.crypto.key.AzureKeyVaultClientAuthenticator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestAzureKeyVaultClientAuthenticator {
    @Test
    public void testDoAuthenticate_shouldThrowRuntimeException() {
        String authClientID     = "test-client-id";
        String authClientSecret = "test-client-secret";

        AzureKeyVaultClientAuthenticator authenticator = new AzureKeyVaultClientAuthenticator(authClientID, authClientSecret);

        String authorization = "https://login.microsoftonline.com/test-tenant";
        String resource      = "https://vault.azure.net";
        String scope         = "https://vault.azure.net.default";

        assertThrows(RuntimeException.class, () ->
                authenticator.doAuthenticate(authorization, resource, scope));
    }

    @Test
    public void testGetAuthentication_withMissingPemFile_shouldThrowException() {
        AzureKeyVaultClientAuthenticator authenticator = new AzureKeyVaultClientAuthenticator("dummy-client-id");

        Exception ex = assertThrows(Exception.class, () -> {
            authenticator.getAuthentication("non-existent.pem", "dummyPassword");
        });

        assertTrue(ex.getMessage().contains("Error while parsing pem certificate"));
    }

    @Test
    public void testGetAuthentication_withMissingPfxFile_shouldThrowException() {
        AzureKeyVaultClientAuthenticator authenticator = new AzureKeyVaultClientAuthenticator("dummy-client-id");

        Exception ex = assertThrows(Exception.class, () ->
                authenticator.getAuthentication("non-existent.pfx", "dummyPassword"));

        assertTrue(ex.getMessage().contains("Error while parsing pfx certificate"));
    }

    @Test
    public void testGetAuthentication_withInvalidExtension_shouldReturnNull() throws Exception {
        AzureKeyVaultClientAuthenticator authenticator = new AzureKeyVaultClientAuthenticator("dummy-client-id");

        KeyVaultClient result = authenticator.getAuthentication("file.unknown", "dummyPassword");

        assertNull(result);
    }
}
