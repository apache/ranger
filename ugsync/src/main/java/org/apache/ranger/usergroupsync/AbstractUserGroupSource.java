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
package org.apache.ranger.usergroupsync;

import org.apache.ranger.ugsyncutil.transform.Mapper;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractUserGroupSource {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractUserGroupSource.class);

    protected final UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();
    protected final Mapper              userNameRegExInst;
    protected final Mapper              groupNameRegExInst;

    public AbstractUserGroupSource() {
        String mappingUserNameHandler  = config.getUserSyncMappingUserNameHandler();
        String mappingGroupNameHandler = config.getUserSyncMappingGroupNameHandler();
        Mapper userNameRegExInst       = null;
        Mapper groupNameRegExInst      = null;

        if (mappingUserNameHandler != null) {
            try {
                Class<Mapper> regExClass = (Class<Mapper>) Class.forName(mappingUserNameHandler);

                userNameRegExInst = regExClass.newInstance();

                userNameRegExInst.init(UgsyncCommonConstants.SYNC_MAPPING_USERNAME, config.getAllRegexPatterns(UgsyncCommonConstants.SYNC_MAPPING_USERNAME), config.getRegexSeparator());
            } catch (ClassNotFoundException cne) {
                LOG.error("Failed to load {}: {}", mappingUserNameHandler, cne);
            } catch (Throwable te) {
                LOG.error("Failed to instantiate {}: {}", mappingUserNameHandler, te);
            }
        }

        if (mappingGroupNameHandler != null) {
            try {
                Class<Mapper> regExClass = (Class<Mapper>) Class.forName(mappingGroupNameHandler);

                groupNameRegExInst = regExClass.newInstance();

                groupNameRegExInst.init(UgsyncCommonConstants.SYNC_MAPPING_GROUPNAME, config.getAllRegexPatterns(UgsyncCommonConstants.SYNC_MAPPING_GROUPNAME), config.getRegexSeparator());
            } catch (ClassNotFoundException cne) {
                LOG.error("Failed to load {}: {}", mappingGroupNameHandler, cne);
            } catch (Throwable te) {
                LOG.error("Failed to instantiate {}: {}", mappingGroupNameHandler, te);
            }
        }

        this.userNameRegExInst  = userNameRegExInst;
        this.groupNameRegExInst = groupNameRegExInst;
    }
}
