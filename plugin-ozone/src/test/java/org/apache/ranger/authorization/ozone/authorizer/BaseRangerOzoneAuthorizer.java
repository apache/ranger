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

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class BaseRangerOzoneAuthorizer {

    protected static String OZONE_TEST_SERVICE_NAME = "test";
    protected static String OZONE_ADMIN = "admin";
    protected static String OZONE_USER = "user";
    protected static String OZONE_VOLUME = "volume";
    protected static String OZONE_BUCKET = "bucket";
    protected static String OZONE_KEY = "key";
    protected static String OZONE_SNAPSHOT = "snapshot";
    protected static String OZONE_SNAPSHOT_USER = "snapshotUser";

    protected static RangerOzoneAuthorizer rangerOzoneAuthorizer =
            new RangerOzoneAuthorizer(OZONE_TEST_SERVICE_NAME);

    protected OzoneObjInfo buildOzoneObjInfoVolume(String volume) {
        return OzoneObjInfo.Builder.newBuilder()
                .setStoreType(OzoneObj.StoreType.OZONE)
                .setResType(OzoneObj.ResourceType.VOLUME)
                .setVolumeName(volume)
                .build();
    }

    protected OzoneObjInfo buildOzoneObjInfoBucket(String volume,
                                                   String bucket) {
        return OzoneObjInfo.Builder.newBuilder()
                .setStoreType(OzoneObj.StoreType.OZONE)
                .setResType(OzoneObj.ResourceType.BUCKET)
                .setVolumeName(volume)
                .setBucketName(bucket)
                .build();
    }

    protected OzoneObjInfo buildOzoneObjInfoKey(String volume,
                                                 String bucket,
                                                 String key) {
        return OzoneObjInfo.Builder.newBuilder()
                .setStoreType(OzoneObj.StoreType.OZONE)
                .setResType(OzoneObj.ResourceType.KEY)
                .setVolumeName(volume)
                .setBucketName(bucket)
                .setKeyName(key)
                .build();
    }

    protected RequestContext buildRequestContext(String user,
                                                 String snapshotName)
            throws UnknownHostException {
        return RequestContext.newBuilder()
                .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
                .setAclRights(IAccessAuthorizer.ACLType.READ)
                .setClientUgi(UserGroupInformation.createRemoteUser(user))
                .setSnapshotName(snapshotName)
                .setIp(InetAddress.getLocalHost())
                .build();
    }

    protected RequestContext buildRequestContext(String user)
            throws UnknownHostException {
        return RequestContext.newBuilder()
                .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
                .setAclRights(IAccessAuthorizer.ACLType.READ)
                .setClientUgi(UserGroupInformation.createRemoteUser(user))
                .setIp(InetAddress.getLocalHost())
                .build();
    }

}
