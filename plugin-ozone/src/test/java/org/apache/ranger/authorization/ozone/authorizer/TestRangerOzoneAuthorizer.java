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

import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

public class TestRangerOzoneAuthorizer extends BaseRangerOzoneAuthorizer {

    @Test
    public void testIOzoneObjIsNull() {
        boolean result = rangerOzoneAuthorizer.checkAccess(null, null);
        Assert.assertFalse(result);
    }

    @Test
    public void testIRequestContextIsNull() {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoVolume(OZONE_VOLUME);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, null);
        Assert.assertFalse(result);
    }

    @Test
    public void testAdminAccessVolume() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoVolume(OZONE_VOLUME);
        RequestContext requestContext = buildRequestContext(OZONE_ADMIN);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

    @Test
    public void testUserAccessVolume() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoVolume(OZONE_VOLUME);
        RequestContext requestContext = buildRequestContext(OZONE_USER);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertFalse(result);
    }

    @Test
    public void testAdminAccessBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_ADMIN);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

    @Test
    public void testUserAccessBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_USER);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertFalse(result);
    }

    @Test
    public void testAdminAccessKey() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoKey(OZONE_VOLUME, OZONE_BUCKET, OZONE_KEY);
        RequestContext requestContext = buildRequestContext(OZONE_ADMIN);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

    @Test
    public void testUserAccessKey() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoKey(OZONE_VOLUME, OZONE_BUCKET, OZONE_KEY);
        RequestContext requestContext = buildRequestContext(OZONE_USER);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertFalse(result);
    }

    @Test
    public void testAdminAccessSnapshotBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_ADMIN, OZONE_SNAPSHOT);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

    @Test
    public void testUserAccessSnapshotBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_USER, OZONE_SNAPSHOT);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertFalse(result);
    }

    @Test
    public void testAdminAccessUserSnapshotBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_ADMIN, OZONE_SNAPSHOT_USER);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

    @Test
    public void testUserAccessUserSnapshotBucket() throws UnknownHostException {
        OzoneObjInfo ozoneObjInfo = buildOzoneObjInfoBucket(OZONE_VOLUME, OZONE_BUCKET);
        RequestContext requestContext = buildRequestContext(OZONE_USER, OZONE_SNAPSHOT_USER);
        boolean result = rangerOzoneAuthorizer.checkAccess(ozoneObjInfo, requestContext);
        Assert.assertTrue(result);
    }

}
