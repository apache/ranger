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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.DBToAzureKeyVault;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;

import static org.eclipse.persistence.jpa.jpql.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestDBToAzureKeyVault {
    @Test
    public void testShowUsage() {
        PrintStream           originalErr = System.err;
        ByteArrayOutputStream errContent  = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        DBToAzureKeyVault.showUsage();

        System.setErr(originalErr);
        String output = errContent.toString();

        assertTrue(output.contains("USAGE: java " + DBToAzureKeyVault.class.getName()));
        assertTrue(output.contains("<azureMasterKeyName>"));
    }

    @Test
    public void testMain_EmptyAzureKeyName_ShouldExit11() {
        String[] args = new String[] {"", "RSA", "AES", "https://vault.url", "lulu", "clientId", "false", "clientSecret"};

        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            DBToAzureKeyVault.main(args);
            fail("Expected System.exit to be called");
        } catch (SecurityException ex) {
            assertTrue(ex.getMessage().contains("exit:1"));
        } finally {
            System.setSecurityManager(originalSM);
        }
    }

    @Test
    void testDoExportMKToAzureKeyVault_WhenMasterKeyMissing_ShouldThrow() throws Exception {
        // 1. Instance of the class under test
        DBToAzureKeyVault vault = new DBToAzureKeyVault();

        // 2. Grab the private method via reflection
        Method m = DBToAzureKeyVault.class.getDeclaredMethod(
                "doExportMKToAzureKeyVault",
                boolean.class,                       // sslEnabled
                String.class, String.class, String.class, // key name/type/algo
                String.class, String.class,               // clientId + vault‑URL
                String.class, String.class,               // pwd/cert + certPwd
                Configuration.class);                     // Hadoop conf
        m.setAccessible(true);

        // 3. Build a Configuration that will FAIL the very first check
        Configuration conf = new Configuration();
        conf.set("ranger.db.encrypt.key.password", "crypted");   // triggers IOException path

        // 4. Parameters – values after the first three don’t matter for this test
        Object[] params = {
                false,                       // sslEnabled
                "testKey",                   // masterKeyName
                "RSA",                       // masterKeyType
                "AES",                       // zoneKeyEncryptionAlgo
                "clientId",                  // azureClientId
                "https://dummy.vault",       // azureKeyVaultUrl
                "secretOrPem",               // passwordOrCertPath
                null,                        // certificatePassword
                conf                         // Hadoop conf
        };

        // ---- act / assert -------------------------------------------------------------
        InvocationTargetException ite = assertThrows(
                InvocationTargetException.class,
                () -> m.invoke(vault, params));

        // unwrap and assert message
        Throwable cause = ite.getCause();
        assertInstanceOf(RuntimeException.class, cause);
        assertTrue(cause.getMessage()
                .contains("Unable to import Master key from Ranger DB to Azure Key Vault"));
    }

    @Test
    void test01_MissingMasterKeyType_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "", "AES", "https://vault", "clientId", "false", "password"},
                "Azure master key type not provided.");
    }

    @Test
    void test02_MissingZoneKeyAlgo_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "", "https://vault", "clientId", "false", "password"},
                "Zone Key Encryption algorithm name not provided.");
    }

    @Test
    void test03_MissingVaultUrl_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "", "clientId", "false", "password"},
                "Azure Key Vault url not provided.");
    }

    @Test
    void test04_MissingClientId_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "", "false", "password"},
                "Azure Client Id is not provided.");
    }

    @Test
    void test05_MissingIsSslEnabled_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "clientId", "", "password"},
                "isSSLEnabled not provided.");
    }

    @Test
    void test06_InvalidIsSslEnabled_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "clientId", "maybe", "password"},
                "Please provide the valid value for isSSLEnabled");
    }

    @Test
    void test07_MissingPasswordOrCert_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "clientId", "false", ""},
                "Please provide Azure client password of certificate password");
    }

    @Test
    void test08_InvalidCertExtension_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "clientId", "true", "notCert.txt"},
                "Please provide valid certificate file path E.G .pem /.pfx");
    }

    @Test
    void test09_InvalidCertExtension_ShouldExit() {
        assertExitsWithMessage(
                new String[] {"azureKey", "RSA", "AES", "https://vault", "clientId", "true", "notCert.txt", "turuturu", "ewgifug"},
                "Please provide valid certificate file path E.G .pem /.pfx");
    }

    private void assertExitsWithMessage(String[] args, String expectedMsg) {
        SecurityManager originalSM  = System.getSecurityManager();
        PrintStream     originalErr = System.err;

        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errBuf));

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            DBToAzureKeyVault.main(args);
            fail("Expected System.exit(1)");
        } catch (SecurityException ex) {
            assertEquals("exit:1", ex.getMessage());
            assertTrue(errBuf.toString().contains(expectedMsg),
                    "stderr did not contain: " + expectedMsg);
        } finally {
            System.setSecurityManager(originalSM);
            System.setErr(originalErr);
        }
    }
}
