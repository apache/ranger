/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.patch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PatchForExternalUserStatusUpdate_J10056 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForExternalUserStatusUpdate_J10056.class);

    @Autowired
    private RangerDaoManager daoManager;

    public static void main(String[] args) {
        try {
            PatchForExternalUserStatusUpdate_J10056 loader = (PatchForExternalUserStatusUpdate_J10056) CLIUtil.getBean(PatchForExternalUserStatusUpdate_J10056.class);

            loader.init();

            while (loader.isMoreToProcess()) {
                loader.load();
            }

            logger.info("Load complete. Exiting!!!");

            System.exit(0);
        } catch (Exception e) {
            logger.error("Error loading", e);

            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {
        // Do Nothing
    }

    @Override
    public void printStats() {
        // TODO Auto-generated method stub
    }

    @Override
    public void execLoad() {
        updateExternalUserStatus();
    }

    private void updateExternalUserStatus() {
        XXPortalUserDao    dao           = this.daoManager.getXXPortalUser();
        List<XXPortalUser> xXPortalUsers = dao.findByUserSourceAndStatus(RangerCommonEnums.USER_EXTERNAL, RangerCommonEnums.ACT_STATUS_DISABLED);

        if (CollectionUtils.isEmpty(xXPortalUsers)) {
            return;
        }

        // Do not bulk-reactivate external disabled users. Disabled is a valid administrative state (see XUserREST.modifyUserActiveStatus),
        // and this patch cannot distinguish accounts left disabled by an old usersync bug from accounts disabled on purpose.
        logger.warn("updateExternalUserStatus(): Skipping automatic reactivation of {} external disabled user(s). Re-enable affected accounts explicitly via Ranger Admin if required.", xXPortalUsers.size());

        for (XXPortalUser xxPortalUser : xXPortalUsers) {
            if (xxPortalUser != null) {
                logger.warn("updateExternalUserStatus(): Left unchanged (loginId={})", xxPortalUser.getLoginId());
            }
        }
    }
}
