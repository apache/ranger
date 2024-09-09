/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import static org.apache.hadoop.crypto.key.kms.server.KMSConfiguration.*;
import static org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KEY_ACL;
import static org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class TestKMSACLs {

  String ipAddress = "192.168.90.1";

  @Test
  public void testDefaults() {
    final KMSACLs acls = new KMSACLs(new Configuration(false));
    for (Type type : Type.values()) {
      Assertions.assertTrue(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser("foo"), ipAddress));
    }
  }

  @Test
  public void testCustom()  {
    final Configuration conf = new Configuration(false);
    for (Type type : Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString() + " ");
    }
    final KMSACLs acls = new KMSACLs(conf);
    for (Type type : Type.values()) {
      Assertions.assertTrue(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser(type.toString()), ipAddress));
      Assertions.assertFalse(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser("foo"), ipAddress));
    }
  }

  @Test
  public void testKeyAclConfigurationLoad() {
    final Configuration conf = new Configuration(false);
    conf.set(KEY_ACL + "test_key_1.MANAGEMENT", "CREATE");
    conf.set(KEY_ACL + "test_key_2.ALL", "CREATE");
    conf.set(KEY_ACL + "test_key_3.NONEXISTOPERATION", "CREATE");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "ROLLOVER");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "DECRYPT_EEK");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "ALL", "invalid");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "ALL", "invalid");
    final KMSACLs acls = new KMSACLs(conf);
    Assertions.assertEquals(2, acls.keyAcls.size(), "expected key ACL size is 2 but got " + acls.keyAcls.size());
    Assertions.assertEquals(1, acls.whitelistKeyAcls.size(), "expected whitelist ACL size is 1 but got "  + acls.whitelistKeyAcls.size());
    Assertions.assertFalse(acls.whitelistKeyAcls.containsKey(KeyOpType.ALL), "ALL should not be allowed for whitelist ACLs.");
    Assertions.assertEquals(1, acls.defaultKeyAcls.size(), "expected default ACL size is 1 but got " + acls.defaultKeyAcls.size());
    Assertions.assertEquals(1, acls.defaultKeyAcls.size(), "ALL should not be allowed for default ACLs.");
 }

  @Test
  public void testKeyAclDuplicateEntries() {
    final Configuration conf = new Configuration(false);
    conf.set(KEY_ACL + "test_key_1.DECRYPT_EEK", "decrypt1");
    conf.set(KEY_ACL + "test_key_2.ALL", "all2");
    conf.set(KEY_ACL + "test_key_1.DECRYPT_EEK", "decrypt2");
    conf.set(KEY_ACL + "test_key_2.ALL", "all1,all3");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "default1");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "whitelist1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    final KMSACLs acls = new KMSACLs(conf);
    Assertions.assertEquals(2, acls.keyAcls.size(), "expected key ACL size is 2 but got " + acls.keyAcls.size());
    assertKeyAcl("test_key_1", acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("test_key_2", acls, KeyOpType.ALL, "all1", "all3");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK);
    AccessControlList acl = acls.whitelistKeyAcls.get(KeyOpType.DECRYPT_EEK);
    Assertions.assertNotNull(acl);
    Assertions.assertTrue(acl.isAllAllowed());
  }

  @Test
  public void testKeyAclReload() {
    Configuration conf = new Configuration(false);
    conf.set(DEFAULT_KEY_ACL_PREFIX + "READ", "read1");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "*");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "decrypt1");
    conf.set(KEY_ACL + "testuser1.ALL", "testkey1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "READ", "admin_read1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK", "*");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "admin_decrypt1");
    final KMSACLs acls = new KMSACLs(conf);

    // update config and hot-reload.
    conf.set(DEFAULT_KEY_ACL_PREFIX + "READ", "read2");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "mgmt1,mgmt2");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "decrypt2");
    conf.set(KEY_ACL + "testkey1.ALL", "testkey1,testkey2");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "READ", "admin_read2");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "admin_mgmt,admin_mgmt1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "admin_decrypt2");
    acls.setKeyACLs(conf);

    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
      "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");
    // reloading same config, nothing should change.
    acls.setKeyACLs(conf);
    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
      "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");
    // test wildcard.
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    acls.setKeyACLs(conf);
    AccessControlList acl = acls.defaultKeyAcls.get(KeyOpType.DECRYPT_EEK);
    Assertions.assertTrue(acl.isAllAllowed());
    Assertions.assertTrue(acl.getUsers().isEmpty());
    // everything else should still be the same.
    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
      "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");
    // test new configuration should clear other items
    conf = new Configuration();
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "new");
    acls.setKeyACLs(conf);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "new");
    Assertions.assertTrue(acls.keyAcls.isEmpty());
    Assertions.assertTrue(acls.whitelistKeyAcls.isEmpty());
    Assertions.assertEquals(1, acls.defaultKeyAcls.size(), "Got unexpected sized acls:" + acls.defaultKeyAcls);
  }

  private void assertDefaultKeyAcl(final KMSACLs acls, final KeyOpType op,
    final String... names) {
    final AccessControlList acl = acls.defaultKeyAcls.get(op);
    assertAcl(acl, op, names);
  }

  private void assertWhitelistKeyAcl(final KMSACLs acls, final KeyOpType op,
    final String... names) {
    final AccessControlList acl = acls.whitelistKeyAcls.get(op);
    assertAcl(acl, op, names);
  }

  private void assertKeyAcl(final String keyName, final KMSACLs acls,
    final KeyOpType op, final String... names) {
    Assertions.assertTrue(acls.keyAcls.containsKey(keyName));
    final HashMap<KeyOpType, AccessControlList> keyacl = acls.keyAcls.get(keyName);
    Assertions.assertNotNull(keyacl.get(op));
    assertAcl(keyacl.get(op), op, names);
  }

  private void assertAcl(final AccessControlList acl,
    final KeyOpType op, final String... names) {
    Assertions.assertNotNull(acl);
    Assertions.assertFalse(acl.isAllAllowed());
    final Collection<String> actual = acl.getUsers();
    final HashSet<String> expected = new HashSet<>(Arrays.asList(names));
    Assertions.assertEquals(expected, actual, "defaultKeyAcls don't match for op:" + op);
  }

}
