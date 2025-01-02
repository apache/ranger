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

package org.apache.ranger.authorization.hadoop;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Objects;

class RangerHdfsResource extends RangerAccessResourceImpl {
    public RangerHdfsResource(String path, String owner) {
        super.setValue(RangerHdfsAuthorizer.KEY_RESOURCE_PATH, path);
        super.setOwnerUser(owner);
    }

    @Override
    public String getAsString() {
        String ret = super.getStringifiedValue();

        if (ret == null) {
            ret = Objects.toString(super.getValue(RangerHdfsAuthorizer.KEY_RESOURCE_PATH));

            super.setStringifiedValue(ret);
        }

        return ret;
    }
}
