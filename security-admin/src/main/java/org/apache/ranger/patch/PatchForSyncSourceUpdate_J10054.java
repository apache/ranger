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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;

@Component
public class PatchForSyncSourceUpdate_J10054 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForSyncSourceUpdate_J10054.class);

    @Autowired
    RangerDaoManager daoManager;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    public static void main(String[] args) {
        logger.info("main()");

        try {
            PatchForSyncSourceUpdate_J10054 loader = (PatchForSyncSourceUpdate_J10054) CLIUtil.getBean(PatchForSyncSourceUpdate_J10054.class);

            loader.init();

            while (loader.isMoreToProcess()) {
                loader.load();
            }

            logger.info("Load complete. Exiting.");

            System.exit(0);
        } catch (Exception e) {
            logger.error("Error loading", e);

            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {/* Do Nothing */}

    @Override
    public void printStats() {
        logger.info("PatchForSyncSourceUpdate data");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForSyncSourceUpdate.execLoad()");

        try {
            if (!updateSyncSourceForUsers() || !updateSyncSourceForGroups()) {
                logger.error("Failed to apply the patch.");

                System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Error while PatchForSyncSourceUpdate()data.", e);

            System.exit(1);
        }

        logger.info("<== PatchForSyncSourceUpdate.execLoad()");
    }

    public boolean updateSyncSourceForUsers() {
        List<XXUser> users = daoManager.getXXUser().getAll();

        for (XXUser xUser : users) {
            String syncSource      = xUser.getSyncSource();
            String otherAttributes = xUser.getOtherAttributes();

            if (StringUtils.isNotEmpty(otherAttributes) && StringUtils.isEmpty(syncSource)) {
                syncSource = (String) JsonUtils.jsonToObject(otherAttributes, Map.class).get(UgsyncCommonConstants.SYNC_SOURCE);

                xUser.setSyncSource(syncSource);

                TransactionTemplate txTemplate = new TransactionTemplate(txManager);

                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                String finalSyncSource = syncSource;

                try {
                    txTemplate.execute(status -> {
                        if (StringUtils.isNotEmpty(finalSyncSource)) {
                            XXPortalUser xXPortalUser = daoManager.getXXPortalUser().findByLoginId(xUser.getName());
                            if (xXPortalUser != null && xXPortalUser.getUserSource() == 0) {
                            /* updating the user source to external for users which had some sync source prior to upgrade
                               but the user source was marked internal to due bugs which were fixed later.
                               See RANGER-3297 for more info
                            */
                                xXPortalUser.setUserSource(1);

                                daoManager.getXXPortalUser().update(xXPortalUser);

                                logger.debug("USER: Name: {} userSource changed to External", xUser.getName());
                            }
                        }

                        daoManager.getXXUser().update(xUser);

                        logger.debug("USER: Name: {} syncSource updated to {}", xUser.getName(), finalSyncSource);

                        return null;
                    });
                } catch (Throwable ex) {
                    logger.error("updateSyncSourceForUsers(): Failed to update DB for user: {}", xUser.getName(), ex);

                    throw new RuntimeException(ex);
                }
            } else {
                logger.debug("Skipping syncSource update for user: {}", xUser.getName());
            }
        }
        return true;
    }

    public boolean updateSyncSourceForGroups() {
        List<XXGroup> groups = daoManager.getXXGroup().getAll();

        for (XXGroup xGroup : groups) {
            String syncSource      = xGroup.getSyncSource();
            String otherAttributes = xGroup.getOtherAttributes();

            if (StringUtils.isNotEmpty(otherAttributes) && StringUtils.isEmpty(syncSource)) {
                syncSource = (String) JsonUtils.jsonToObject(otherAttributes, Map.class).get(UgsyncCommonConstants.SYNC_SOURCE);

                if (StringUtils.isNotEmpty(syncSource) && xGroup.getGroupSource() == 0) {
                    xGroup.setGroupSource(1);
                    logger.debug("GROUP: Name: {} groupSource changed to External", xGroup.getName());
                }

                xGroup.setSyncSource(syncSource);

                logger.debug("GROUP: Name: {} syncSource updated to {}", xGroup.getName(), syncSource);

                TransactionTemplate txTemplate = new TransactionTemplate(txManager);

                txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                try {
                    txTemplate.execute(status -> {
                        daoManager.getXXGroup().update(xGroup);
                        return null;
                    });
                } catch (Throwable ex) {
                    logger.error("updateSyncSourceForGroups(): Failed to update DB for group: {}", xGroup.getName(), ex);

                    throw new RuntimeException(ex);
                }
            } else {
                logger.debug("Skipping syncSource update for group: {}", xGroup.getName());
            }
        }

        return true;
    }
}
