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
package org.apache.ranger.unixusersync.process;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.ugsyncutil.model.XGroupInfo;
import org.apache.ranger.ugsyncutil.model.XUserInfo;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PolicyMgrUserGroupBuilderCrossProviderDeleteTest {
    private static final String LDAP_SYNC_SOURCE = "LDAP/AD";
    private static final String UNIX_SYNC_SOURCE = "Unix";
    private static final String FILE_SYNC_SOURCE = "File";
    private static final String LDAP_URL_A = "ldap://ldap-a.example.com:389";
    private static final String LDAP_URL_B = "ldap://ldap-b.example.com:389";
    private static final String ISVISIBLE = "1";
    private static final String ISHIDDEN = "0";

    private PolicyMgrUserGroupBuilder builder;

    @BeforeEach
    public void setUp() throws Exception {
        UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();
        config.setProperty(UgsyncCommonConstants.UGSYNC_GROUPNAME_CASE_CONVERSION_PARAM, UgsyncCommonConstants.UGSYNC_NONE_CASE_CONVERSION_VALUE);
        config.setProperty(UgsyncCommonConstants.UGSYNC_USERNAME_CASE_CONVERSION_PARAM, UgsyncCommonConstants.UGSYNC_NONE_CASE_CONVERSION_VALUE);
        config.setProperty(UserGroupSyncConfig.UGSYNC_NAME_VALIDATION_ENABLED, "false");

        builder = new PolicyMgrUserGroupBuilder();

        setField("groupCache", new HashMap<String, XGroupInfo>());
        setField("userCache", new HashMap<String, XUserInfo>());
        setField("groupNameMap", new HashMap<String, String>());
        setField("userNameMap", new HashMap<String, String>());
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);
        setField("isStartupFlag", false);
    }

    private void setField(String name, Object value) throws Exception {
        Field f = PolicyMgrUserGroupBuilder.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(builder, value);
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(String name) throws Exception {
        Field f = PolicyMgrUserGroupBuilder.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(builder);
    }

    private void invoke(String name, Class<?> paramType, Object arg) throws Exception {
        Method m = PolicyMgrUserGroupBuilder.class.getDeclaredMethod(name, paramType);
        m.setAccessible(true);
        try {
            m.invoke(builder, arg);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    private Map<String, String> baseAttrs(String fullName, String syncSource, String ldapUrl) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.FULL_NAME, fullName);
        if (syncSource != null) {
            attrs.put(UgsyncCommonConstants.SYNC_SOURCE, syncSource);
        }
        if (ldapUrl != null) {
            // Explicit "" models Admin round-trip storing an empty ldap_url.
            attrs.put(UgsyncCommonConstants.LDAP_URL, ldapUrl);
        }
        return attrs;
    }

    private XGroupInfo group(String name, String fullName, String syncSource, String ldapUrl, String isVisible) {
        XGroupInfo g = new XGroupInfo();
        g.setName(name);
        g.setIsVisible(isVisible);
        g.setSyncSource(syncSource);
        Map<String, String> attrs = baseAttrs(fullName, syncSource, ldapUrl);
        g.setOtherAttrsMap(attrs);
        g.setOtherAttributes(JsonUtils.objectToJson(attrs));
        return g;
    }

    private XUserInfo user(String name, String fullName, String syncSource, String ldapUrl, String isVisible) {
        XUserInfo u = new XUserInfo();
        u.setName(name);
        u.setFirstName(name);
        u.setIsVisible(isVisible);
        u.setSyncSource(syncSource);
        Map<String, String> attrs = baseAttrs(fullName, syncSource, ldapUrl);
        u.setOtherAttrsMap(attrs);
        u.setOtherAttributes(JsonUtils.objectToJson(attrs));
        return u;
    }

    private void seedUser(XUserInfo u) throws Exception {
        this.<Map<String, XUserInfo>>getField("userCache").put(u.getName(), u);
    }

    private void seedGroup(XGroupInfo g) throws Exception {
        this.<Map<String, XGroupInfo>>getField("groupCache").put(g.getName(), g);
    }

    private Map<String, XUserInfo> deletedUsers() throws Exception {
        return getField("deletedUsers");
    }

    private Map<String, XGroupInfo> deletedGroups() throws Exception {
        return getField("deletedGroups");
    }

    // ---------- LDAP URL scoping ----------

    @Test
    public void testL1_ldapUser_otherUrl_notDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedUser(user("alice", "cn=alice,dc=example,dc=com", LDAP_SYNC_SOURCE, LDAP_URL_B, ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("alice"),
                "LDAP user from a different ldap_url must not be soft-deleted");
        assertEquals(ISVISIBLE, this.<Map<String, XUserInfo>>getField("userCache").get("alice").getIsVisible());
    }

    @Test
    public void testL2_ldapGroup_otherUrl_notDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedGroup(group("eng", "cn=eng,dc=example,dc=com", LDAP_SYNC_SOURCE, LDAP_URL_B, ISVISIBLE));

        invoke("computeDeletedGroups", Map.class, Collections.emptyMap());

        assertFalse(deletedGroups().containsKey("eng"),
                "LDAP group from a different ldap_url must not be soft-deleted");
    }

    @Test
    public void testL3_ldapUser_sameUrl_orphanDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedUser(user("bob", "cn=bob,dc=example,dc=com", LDAP_SYNC_SOURCE, LDAP_URL_A, ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertTrue(deletedUsers().containsKey("bob"),
                "Orphan LDAP user on the same ldap_url must be soft-deleted");
        assertEquals(ISHIDDEN, deletedUsers().get("bob").getIsVisible());
    }

    @Test
    public void testL4_ldapGroup_sameUrl_orphanDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedGroup(group("ops", "cn=ops,dc=example,dc=com", LDAP_SYNC_SOURCE, LDAP_URL_A, ISVISIBLE));

        invoke("computeDeletedGroups", Map.class, Collections.emptyMap());

        assertTrue(deletedGroups().containsKey("ops"));
        assertEquals(ISHIDDEN, deletedGroups().get("ops").getIsVisible());
    }

    @Test
    public void testL5_ldapUser_dnCaseDrift_isDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        String cachedDn = "CN=Alice,DC=example,DC=com";
        String sourceDn = "cn=alice,dc=example,dc=com";
        seedUser(user("alice", cachedDn, LDAP_SYNC_SOURCE, LDAP_URL_A, ISVISIBLE));

        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        sourceUsers.put(sourceDn, baseAttrs(sourceDn, LDAP_SYNC_SOURCE, LDAP_URL_A));

        invoke("computeDeletedUsers", Map.class, sourceUsers);

        assertTrue(deletedUsers().containsKey("alice"),
                "Delete matching stays an exact key compare; DN case drift is absent from the snapshot");
    }

    @Test
    public void testL6_ldapGroup_dnCaseDrift_isDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        String cachedDn = "CN=Eng,DC=example,DC=com";
        String sourceDn = "cn=eng,dc=example,dc=com";
        seedGroup(group("eng", cachedDn, LDAP_SYNC_SOURCE, LDAP_URL_A, ISVISIBLE));

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(sourceDn, baseAttrs(sourceDn, LDAP_SYNC_SOURCE, LDAP_URL_A));

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        assertTrue(deletedGroups().containsKey("eng"),
                "Delete matching stays an exact key compare; DN case drift is absent from the snapshot");
    }

    @Test
    public void testL7_ldapUser_blankAttrWithConfiguredUrl_notDeleted() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedUser(user("legacy", "cn=legacy,dc=example,dc=com", LDAP_SYNC_SOURCE, "", ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("legacy"),
                "Blank ldap_url on the record must not match a configured LDAP URL");
    }

    // ---------- Unix / File blank ldap_url ----------

    @Test
    public void testU1_unixUser_nullLdapUrlBothSides_orphanDeleted() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedUser(user("alice", "alice", UNIX_SYNC_SOURCE, null, ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertTrue(deletedUsers().containsKey("alice"),
                "Unix orphan with null/blank ldap_url on both sides must be soft-deleted");
    }

    @Test
    public void testU2_unixUser_emptyStringLdapUrlAttr_notDeleted() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedUser(user("bob", "bob", UNIX_SYNC_SOURCE, "", ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("bob"),
                "ldap_url=\"\" does not match a null configured ldapUrl");
    }

    @Test
    public void testU3_unixGroup_emptyStringLdapUrlAttr_notDeleted() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedGroup(group("devs", "devs", UNIX_SYNC_SOURCE, "", ISVISIBLE));

        invoke("computeDeletedGroups", Map.class, Collections.emptyMap());

        assertFalse(deletedGroups().containsKey("devs"));
        assertEquals(ISVISIBLE, this.<Map<String, XGroupInfo>>getField("groupCache").get("devs").getIsVisible());
    }

    @Test
    public void testF1_fileUser_blankLdapUrl_notDeleted() throws Exception {
        setField("currentSyncSource", FILE_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedUser(user("fileuser", "fileuser", FILE_SYNC_SOURCE, "", ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("fileuser"),
                "ldap_url=\"\" does not match a null configured ldapUrl");
    }

    @Test
    public void testF2_fileGroup_nullLdapUrl_orphanDeleted() throws Exception {
        setField("currentSyncSource", FILE_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedGroup(group("filegroup", "filegroup", FILE_SYNC_SOURCE, null, ISVISIBLE));

        invoke("computeDeletedGroups", Map.class, Collections.emptyMap());

        assertTrue(deletedGroups().containsKey("filegroup"));
    }

    // ---------- sync_source isolation + entity fallback ----------

    @Test
    public void testS1_unixCycle_doesNotDeleteLdapUser() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedUser(user("ldap_only", "cn=ldap_only,dc=example,dc=com", LDAP_SYNC_SOURCE, LDAP_URL_A, ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("ldap_only"),
                "Unix cycle must not soft-delete LDAP/AD users");
    }

    @Test
    public void testS2_ldapCycle_doesNotDeleteUnixUser() throws Exception {
        setField("currentSyncSource", LDAP_SYNC_SOURCE);
        setField("ldapUrl", LDAP_URL_A);

        seedUser(user("unix_only", "unix_only", UNIX_SYNC_SOURCE, null, ISVISIBLE));

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("unix_only"),
                "LDAP cycle must not soft-delete Unix users");
    }

    @Test
    public void testS3_missingSyncSourceAttr_notDeleted() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        XUserInfo u = user("carol", "carol", UNIX_SYNC_SOURCE, null, ISVISIBLE);
        Map<String, String> attrs = new HashMap<>(u.getOtherAttrsMap());
        attrs.remove(UgsyncCommonConstants.SYNC_SOURCE);
        u.setOtherAttrsMap(attrs);
        u.setOtherAttributes(JsonUtils.objectToJson(attrs));
        seedUser(u);

        invoke("computeDeletedUsers", Map.class, Collections.emptyMap());

        assertFalse(deletedUsers().containsKey("carol"),
                "Delete scoping reads sync_source from otherAttributes; the entity column is not a fallback");
    }

    @Test
    public void testS4_presentInSource_notDeleted() throws Exception {
        setField("currentSyncSource", UNIX_SYNC_SOURCE);
        setField("ldapUrl", null);

        seedUser(user("dave", "dave", UNIX_SYNC_SOURCE, "", ISVISIBLE));

        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        sourceUsers.put("dave", baseAttrs("dave", UNIX_SYNC_SOURCE, ""));

        invoke("computeDeletedUsers", Map.class, sourceUsers);

        assertFalse(deletedUsers().containsKey("dave"),
                "User still present in the source snapshot must not be deleted");
    }
}
