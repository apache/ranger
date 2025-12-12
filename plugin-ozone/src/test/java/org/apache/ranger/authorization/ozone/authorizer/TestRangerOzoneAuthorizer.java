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

package org.apache.ranger.authorization.ozone.authorizer;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerInlinePolicy;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerOzoneAuthorizer {
    private static final String RANGER_SERVICE_TYPE = "ozone";
    private static final String RANGER_APP_ID       = "om";
    private static final String OZONE_SERVICE_ID    = "om";
    private static final String OWNER_NAME          = "ozone";

    private static RangerOzoneAuthorizer ozoneAuthorizer;

    private final String               hostname  = "localhost";
    private final InetAddress          ipAddress = InetAddress.getLoopbackAddress();
    private final UserGroupInformation user1     = UserGroupInformation.createRemoteUser("user1");
    private final UserGroupInformation user2     = UserGroupInformation.createRemoteUser("user2");
    private final String               role1     = "role1";

    private final OzoneObj   vol1  = new OzoneObjInfo.Builder().setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OzoneObj.StoreType.OZONE).setVolumeName("vol1").build();
    private final OzoneObj   buck1 = new OzoneObjInfo.Builder().setResType(OzoneObj.ResourceType.BUCKET).setStoreType(OzoneObj.StoreType.OZONE).setVolumeName("vol1").setBucketName("buck1").build();
    private final OzoneObj   key1  = new OzoneObjInfo.Builder().setResType(OzoneObj.ResourceType.KEY).setStoreType(OzoneObj.StoreType.OZONE).setVolumeName("vol1").setBucketName("buck1").setKeyName("key1").build();
    private final OzoneObj   vol2  = new OzoneObjInfo.Builder().setResType(OzoneObj.ResourceType.VOLUME).setStoreType(OzoneObj.StoreType.OZONE).setVolumeName("vol2").build();
    private final OzoneGrant grantList = new OzoneGrant(new HashSet<>(Arrays.asList(vol1, buck1)), Collections.singleton(IAccessAuthorizer.ACLType.LIST));
    private final OzoneGrant grantRead = new OzoneGrant(Collections.singleton(key1), Collections.singleton(IAccessAuthorizer.ACLType.READ));

    @BeforeAll
    public static void setUpBeforeClass() {
        RangerPluginConfig pluginConfig = new RangerPluginConfig(RANGER_SERVICE_TYPE, null, RANGER_APP_ID, null, null, null); // loads ranger-ozone-security.xml
        RangerBasePlugin   plugin       = new RangerBasePlugin(pluginConfig);

        // loads policies from om_dev_ozone.json, by EmbeddedResourcePolicySource configured in ranger-ozone-security.xml
        plugin.init();

        ozoneAuthorizer = new RangerOzoneAuthorizer(plugin);

        assertNotNull(ozoneAuthorizer);
    }

    @Test
    public void testAssumeRoleDeny() {
        // user2 should not be allowed to assume role1 - no Ranger policy grants this permission
        AssumeRoleRequest request = new AssumeRoleRequest(hostname, ipAddress, user2, role1, null);

        assertThrows(OMException.class, () -> ozoneAuthorizer.generateAssumeRoleSessionPolicy(request));
    }

    @Test
    public void testAssumeRoleWithEmptyGrants() throws Exception {
        Set<OzoneGrant>   grants  = Collections.emptySet();
        AssumeRoleRequest request = new AssumeRoleRequest(hostname, ipAddress, user1, role1, grants);

        // user1 should be allowed to assume role1 - Ranger policy #100 grants this permission
        String sessionPolicy = ozoneAuthorizer.generateAssumeRoleSessionPolicy(request);

        assertNotNull(sessionPolicy);
        assertNotEquals("", sessionPolicy);

        RangerInlinePolicy inlinePolicy = JsonUtilsV2.jsonToObj(sessionPolicy, RangerInlinePolicy.class);

        assertEquals("r:role1", inlinePolicy.getGrantor());
        assertEquals("user1", inlinePolicy.getCreatedBy());
        assertEquals(RangerInlinePolicy.Mode.INLINE, inlinePolicy.getMode());
        assertNotNull(inlinePolicy.getGrants());

        RequestContext ctxListWithoutSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.LIST, OWNER_NAME);
        RequestContext ctxReadWithoutSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.READ, OWNER_NAME);
        RequestContext ctxListWithSessionPolicy    = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.LIST, OWNER_NAME, false, sessionPolicy);
        RequestContext ctxReadWithSessionPolicy    = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.READ, OWNER_NAME, false, sessionPolicy);

        // user1 doesn't have access without session-policy
        assertFalse(ozoneAuthorizer.checkAccess(vol1, ctxListWithoutSessionPolicy), "session-policy should not allow list on volume vol1");
        assertFalse(ozoneAuthorizer.checkAccess(vol2, ctxListWithoutSessionPolicy), "session-policy should not allow list on volume vol2");
        assertFalse(ozoneAuthorizer.checkAccess(buck1, ctxListWithoutSessionPolicy), "session-policy should not allow list on bucket vol1/buck1");
        assertFalse(ozoneAuthorizer.checkAccess(key1, ctxReadWithoutSessionPolicy), "session-policy should not allow read on key vol1/buck1/key1");

        // user1 should not have access with session-policy as well, due to empty grants
        assertFalse(ozoneAuthorizer.checkAccess(vol1, ctxListWithSessionPolicy), "session-policy should not allow list on volume vol1");
        assertFalse(ozoneAuthorizer.checkAccess(vol2, ctxListWithSessionPolicy), "session-policy should not allow list on volume vol2");
        assertFalse(ozoneAuthorizer.checkAccess(buck1, ctxListWithSessionPolicy), "session-policy should not allow list on bucket vol1/buck1");
        assertFalse(ozoneAuthorizer.checkAccess(key1, ctxReadWithSessionPolicy), "session-policy should not allow read on key vol1/buck1/key1");
    }

    @Test
    public void testAssumeRoleWithNullGrants() throws Exception {
        Set<OzoneGrant>   grants  = null;
        AssumeRoleRequest request = new AssumeRoleRequest(hostname, ipAddress, user1, role1, grants);

        // user1 should be allowed to assume role1 - Ranger policy #100 grants this permission
        String sessionPolicy = ozoneAuthorizer.generateAssumeRoleSessionPolicy(request);

        assertNotNull(sessionPolicy);
        assertNotEquals("", sessionPolicy);

        RangerInlinePolicy inlinePolicy = JsonUtilsV2.jsonToObj(sessionPolicy, RangerInlinePolicy.class);

        assertEquals("r:role1", inlinePolicy.getGrantor());
        assertEquals("user1", inlinePolicy.getCreatedBy());
        assertEquals(RangerInlinePolicy.Mode.INLINE, inlinePolicy.getMode());
        assertNull(inlinePolicy.getGrants());

        RequestContext ctxListWithoutSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.LIST, OWNER_NAME);
        RequestContext ctxReadWithoutSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.READ, OWNER_NAME);
        RequestContext ctxListWithSessionPolicy    = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.LIST, OWNER_NAME, false, sessionPolicy);
        RequestContext ctxReadWithSessionPolicy    = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.READ, OWNER_NAME, false, sessionPolicy);

        // user1 doesn't have access without session-policy
        assertFalse(ozoneAuthorizer.checkAccess(vol1, ctxListWithoutSessionPolicy), "session-policy should not allow list on volume vol1");
        assertFalse(ozoneAuthorizer.checkAccess(vol2, ctxListWithoutSessionPolicy), "session-policy should not allow list on volume vol2");
        assertFalse(ozoneAuthorizer.checkAccess(buck1, ctxListWithoutSessionPolicy), "session-policy should not allow list on bucket vol1/buck1");
        assertFalse(ozoneAuthorizer.checkAccess(key1, ctxReadWithoutSessionPolicy), "session-policy should not allow read on key vol1/buck1/key1");

        // user1 should have access with session-policy, due to null grants which allows all accesses granted to role1
        assertTrue(ozoneAuthorizer.checkAccess(vol1, ctxListWithSessionPolicy), "session-policy should allow list on volume vol1");
        assertTrue(ozoneAuthorizer.checkAccess(vol2, ctxListWithSessionPolicy), "session-policy should allow list on volume vol2");
        assertTrue(ozoneAuthorizer.checkAccess(buck1, ctxListWithSessionPolicy), "session-policy should allow list on bucket vol1/buck1");
        assertTrue(ozoneAuthorizer.checkAccess(key1, ctxReadWithSessionPolicy), "session-policy should allow read on key vol1/buck1/key1");
    }

    @Test
    public void testAssumeRoleWithGrants() throws Exception {
        Set<OzoneGrant>   grants  = new HashSet<>(Arrays.asList(grantList, grantRead));
        AssumeRoleRequest request = new AssumeRoleRequest(hostname, ipAddress, user1, role1, grants);

        // user1 should be allowed to assume role1 - Ranger policy #100 grants this permission
        String sessionPolicy = ozoneAuthorizer.generateAssumeRoleSessionPolicy(request);

        assertNotNull(sessionPolicy);
        assertNotEquals("", sessionPolicy);

        RangerInlinePolicy inlinePolicy = JsonUtilsV2.jsonToObj(sessionPolicy, RangerInlinePolicy.class);

        assertEquals("r:role1", inlinePolicy.getGrantor());
        assertEquals("user1", inlinePolicy.getCreatedBy());
        assertEquals(RangerInlinePolicy.Mode.INLINE, inlinePolicy.getMode());
        assertNotNull(inlinePolicy.getGrants());
        assertEquals(2, inlinePolicy.getGrants().size());

        assertTrue(inlinePolicy.getGrants().contains(new RangerInlinePolicy.Grant(null, new HashSet<>(Arrays.asList("volume:vol1", "bucket:vol1/buck1")), Collections.singleton("list"))));
        assertTrue(inlinePolicy.getGrants().contains(new RangerInlinePolicy.Grant(null, Collections.singleton("key:vol1/buck1/key1"), Collections.singleton("read"))));

        RequestContext ctxListWithSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.LIST, OWNER_NAME, false, sessionPolicy);
        RequestContext ctxReadWithSessionPolicy = new RequestContext(hostname, ipAddress, user1, OZONE_SERVICE_ID, IAccessAuthorizer.ACLIdentityType.ANONYMOUS, IAccessAuthorizer.ACLType.READ, OWNER_NAME, false, sessionPolicy);

        // user1 should have access with sessionPolicy
        assertTrue(ozoneAuthorizer.checkAccess(vol1, ctxListWithSessionPolicy), "session-policy should allow list on volume vol1");
        assertFalse(ozoneAuthorizer.checkAccess(vol2, ctxListWithSessionPolicy), "session-policy should not allow list on volume vol2");
        assertTrue(ozoneAuthorizer.checkAccess(buck1, ctxListWithSessionPolicy), "session-policy should allow list on bucket vol1/buck1");
        assertTrue(ozoneAuthorizer.checkAccess(key1, ctxReadWithSessionPolicy), "session-policy should allow read on key vol1/buck1/key1");
    }
}
