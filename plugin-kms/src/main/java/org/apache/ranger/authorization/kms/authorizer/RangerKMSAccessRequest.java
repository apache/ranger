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

package org.apache.ranger.authorization.kms.authorizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;

import java.util.Date;

class RangerKMSAccessRequest extends RangerAccessRequestImpl {
    public RangerKMSAccessRequest(String keyName, String accessType, UserGroupInformation ugi, String clientIp) {
        super.setResource(new RangerKMSResource(keyName));
        super.setAccessType(accessType);
        super.setUser(ugi.getShortUserName());
        super.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
        super.setAccessTime(new Date());
        super.setClientIPAddress(clientIp);
        super.setAction(accessType);
    }
}
