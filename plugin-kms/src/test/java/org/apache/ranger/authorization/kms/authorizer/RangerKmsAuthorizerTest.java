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

package org.apache.ranger.authorization.kms.authorizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.KMSWebApp;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletContextEvent;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Policies available from admin via:
 * <p>
 * http://localhost:6080/service/plugins/policies/download/KMSTest
 * <p>
 * The user "bob" can do anything. The group "IT" can only call the "get" methods
 */
@ExtendWith(MockitoExtension.class)
public class RangerKmsAuthorizerTest {
    private static final Logger LOG = LoggerFactory.getLogger(RangerKmsAuthorizerTest.class);
    private static final boolean   UNRESTRICTED_POLICIES_INSTALLED;
    private static       KMSWebApp kmsWebapp;

    @BeforeAll
    public static void startServers() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }
        DerbyTestUtils.startDerby();

        Path configDir = Paths.get("src/test/resources/kms");
        Path logDir    = Paths.get("target");

        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());
        System.setProperty("kms.log.dir", logDir.toFile().getAbsolutePath());
        System.setProperty("hostname", "localhost");
        System.setProperty("user", "autotest");

        // Start KMSWebApp
        ServletContextEvent servletContextEvent = Mockito.mock(ServletContextEvent.class);

        kmsWebapp = new KMSWebApp();
        kmsWebapp.contextInitialized(servletContextEvent);
    }

    @AfterAll
    public static void stopServers() throws Exception {
        DerbyTestUtils.stopDerby();
    }

    @Test
    public void testCreateKeys() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to create
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.CREATE, ugi, KMSOp.CREATE_KEY, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to create
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.CREATE, ugi2, KMSOp.CREATE_KEY, "newkey2", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should not have permission to create
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.CREATE, ugi3, KMSOp.CREATE_KEY, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });
    }

    @Test
    public void testDeleteKeys() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to delete
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.DELETE, ugi, KMSOp.DELETE_KEY, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to delete
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.DELETE, ugi2, KMSOp.DELETE_KEY, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should not have permission to delete
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.DELETE, ugi3, KMSOp.DELETE_KEY, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });
    }

    @Test
    public void testRollover() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to rollover
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.ROLLOVER, ugi, KMSOp.ROLL_NEW_VERSION, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to rollover
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.ROLLOVER, ugi2, KMSOp.ROLL_NEW_VERSION, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should not have permission to rollover
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.ROLLOVER, ugi3, KMSOp.ROLL_NEW_VERSION, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });
    }

    @Test
    public void testGetKeys() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to get keys
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.GET_KEYS, ugi, KMSOp.GET_KEYS, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to get keys
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.GET_KEYS, ugi2, KMSOp.GET_KEYS, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should have permission to get keys
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.GET_KEYS, ugi3, KMSOp.GET_KEYS, "newkey1", "127.0.0.1");
                return null;
            }
        });
    }

    @Test
    public void testGetMetadata() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to get the metadata
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.GET_METADATA, ugi, KMSOp.GET_METADATA, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to get the metadata
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.GET_METADATA, ugi2, KMSOp.GET_METADATA, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should have permission to get the metadata
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.GET_METADATA, ugi3, KMSOp.GET_METADATA, "newkey1", "127.0.0.1");
                return null;
            }
        });
    }

    @Test
    public void testGenerateEEK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to generate EEK
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.GENERATE_EEK, ugi, KMSOp.GENERATE_EEK, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to generate EEK
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.GENERATE_EEK, ugi2, KMSOp.GENERATE_EEK, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should not have permission to generate EEK
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.GENERATE_EEK, ugi3, KMSOp.GENERATE_EEK, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });
    }

    @Test
    public void testDecryptEEK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        // bob should have permission to generate EEK
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                KMSWebApp.getACLs().assertAccess(Type.DECRYPT_EEK, ugi, KMSOp.DECRYPT_EEK, "newkey1", "127.0.0.1");
                return null;
            }
        });

        // "eve" should not have permission to decrypt EEK
        final UserGroupInformation ugi2 = UserGroupInformation.createRemoteUser("eve");
        ugi2.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.DECRYPT_EEK, ugi2, KMSOp.DECRYPT_EEK, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });

        // the IT group should not have permission to decrypt EEK
        final UserGroupInformation ugi3 = UserGroupInformation.createUserForTesting("alice", new String[] {"IT"});
        ugi3.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    KMSWebApp.getACLs().assertAccess(Type.DECRYPT_EEK, ugi3, KMSOp.DECRYPT_EEK, "newkey1", "127.0.0.1");
                    Assertions.fail("Failure expected");
                } catch (AuthorizationException ex) {
                    LOG.error("", ex);
                }
                return null;
            }
        });
    }

    @Test
    public void testHasAccessToKey() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("john");
        boolean result = authorizer.hasAccessToKey("key1", ugi, KeyOpType.MANAGEMENT);
        assertTrue(result);
    }

    @Test
    public void testIsACLPresent() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        boolean result = authorizer.isACLPresent("key1", KeyOpType.MANAGEMENT);
        assertTrue(result);
    }

    @Test
    public void testStartAndStopReloader() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        authorizer.startReloader();
        // Should not throw, and executorService should not be null after start
        authorizer.stopReloader();
    }

    @Test
    public void testRunReloadsACLs() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        assertDoesNotThrow(authorizer::run);
    }

    @Test
    public void testInit() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        Configuration conf = Mockito.mock(Configuration.class);
        assertDoesNotThrow(() -> authorizer.init(conf));
    }

    /**
    * @generated by copilot
    */
    @Test
    public void testSetKMSACLs() {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        Configuration conf = Mockito.mock(Configuration.class);
        Mockito.when(conf.get(Mockito.anyString())).thenReturn("user1,user2");
        // Use reflection to call private setKMSACLs
        assertDoesNotThrow(() -> {
            java.lang.reflect.Method m = RangerKmsAuthorizer.class.getDeclaredMethod("setKMSACLs", Configuration.class);
            m.setAccessible(true);
            m.invoke(authorizer, conf);
        });
    }

    /**
    * @generated by copilot
    */
    @Test
    public void testHasAccess_UserInBlacklist() throws Exception {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        // Use reflection to set a blacklist for CREATE
        java.lang.reflect.Field blacklistField = RangerKmsAuthorizer.class.getDeclaredField("blacklistedAcls");
        blacklistField.setAccessible(true);
        java.util.Map<Type, org.apache.hadoop.security.authorize.AccessControlList> blacklist = new java.util.HashMap<>();
        blacklist.put(Type.CREATE, new org.apache.hadoop.security.authorize.AccessControlList("bob"));
        blacklistField.set(authorizer, blacklist);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        boolean result = authorizer.hasAccess(Type.CREATE, ugi, "127.0.0.1");
        assertFalse(result, "User in blacklist should not have access");
    }

    /**
    * @generated by copilot
    */
    @Test
    public void testHasAccess_WithPluginAllows() throws Exception {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        // No blacklist, but plugin returns allowed
        RangerKMSPlugin plugin = Mockito.mock(RangerKMSPlugin.class);
        RangerAccessResult accessResult = Mockito.mock(RangerAccessResult.class);
        Mockito.when(accessResult.getIsAllowed()).thenReturn(true);
        Mockito.when(plugin.isAccessAllowed((org.apache.ranger.plugin.policyengine.RangerAccessRequest) Mockito.any())).thenReturn(accessResult);
        java.lang.reflect.Field pluginField = RangerKmsAuthorizer.class.getDeclaredField("kmsPlugin");
        pluginField.setAccessible(true);
        pluginField.set(null, plugin);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        boolean result = authorizer.hasAccess(Type.CREATE, ugi, "127.0.0.1");
        assertTrue(result, "Plugin allows access");
    }

    /**
    * @generated by copilot
    */
    @Test
    public void testHasAccess_WithPluginDenies() throws Exception {
        RangerKmsAuthorizer authorizer = new RangerKmsAuthorizer((Configuration) null);
        // No blacklist, but plugin returns denied
        RangerKMSPlugin plugin = Mockito.mock(RangerKMSPlugin.class);
        RangerAccessResult accessResult = Mockito.mock(RangerAccessResult.class);
        Mockito.when(accessResult.getIsAllowed()).thenReturn(false);
        Mockito.when(plugin.isAccessAllowed((org.apache.ranger.plugin.policyengine.RangerAccessRequest) Mockito.any())).thenReturn(accessResult);
        java.lang.reflect.Field pluginField = RangerKmsAuthorizer.class.getDeclaredField("kmsPlugin");
        pluginField.setAccessible(true);
        pluginField.set(null, plugin);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bob");
        boolean result = authorizer.hasAccess(Type.CREATE, ugi, "127.0.0.1");
        assertFalse(result, "Plugin denies access");
    }

    static {
        boolean ok = false;
        try {
            byte[] data = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};

            SecretKey key192 = new SecretKeySpec(
                    new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17},
                    "AES");
            Cipher c = Cipher.getInstance("AES");
            c.init(Cipher.ENCRYPT_MODE, key192);
            c.doFinal(data);
            ok = true;
        } catch (Exception e) {
            //
        }
        UNRESTRICTED_POLICIES_INSTALLED = ok;
    }
}
