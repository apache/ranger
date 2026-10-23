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
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.service.RangerServiceDefService;
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

@Component
public class PatchForServiceDefJson_J10067 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForServiceDefJson_J10067.class);

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RangerServiceDefService serviceDefService;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    public static void main(String[] args) {
        logger.info("main()");
        try {
            PatchForServiceDefJson_J10067 loader = (PatchForServiceDefJson_J10067) CLIUtil.getBean(PatchForServiceDefJson_J10067.class);

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
        logger.info("PatchForServiceDefJson_J10067");
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForServiceDefJson_J10067.execLoad()");

        try {
            updateServiceDefText();
        } catch (Exception e) {
            logger.error("Error while applying PatchForServiceDefJson_J10067", e);
            throw new RuntimeException("PatchForServiceDefJson_J10067 failed", e);
        }

        logger.info("<== PatchForServiceDefJson_J10067.execLoad()");
    }

    private void updateServiceDefText() {
        List<XXServiceDef> serviceDefs = daoMgr.getXXServiceDef().getAll();

        if (CollectionUtils.isEmpty(serviceDefs)) {
            logger.info("No service-defs found");

            return;
        }

        TransactionTemplate txTemplate = new TransactionTemplate(txManager);

        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        for (XXServiceDef xServiceDef : serviceDefs) {
            Long serviceDefId = xServiceDef.getId();

            txTemplate.execute(status -> {
                XXServiceDef dbServiceDef = daoMgr.getXXServiceDef().getById(serviceDefId);

                if (dbServiceDef != null && StringUtils.isEmpty(dbServiceDef.getDefText())) {
                    // constraintDefs were not stored before def_text existed, so there are none to carry over
                    String defText = serviceDefService.buildDefText(dbServiceDef, null);

                    if (StringUtils.isEmpty(defText)) {
                        throw new IllegalStateException("failed to build JSON for service-def(id=" + serviceDefId + ", name=" + dbServiceDef.getName() + ")");
                    }

                    dbServiceDef.setDefText(defText);

                    daoMgr.getXXServiceDef().update(dbServiceDef);

                    logger.info("Stored JSON for service-def(id={}, name={})", serviceDefId, dbServiceDef.getName());
                } else if (dbServiceDef != null) {
                    logger.info("JSON already present for service-def(id={}, name={}); skipping", serviceDefId, dbServiceDef.getName());
                }

                return null;
            });
        }
    }
}
