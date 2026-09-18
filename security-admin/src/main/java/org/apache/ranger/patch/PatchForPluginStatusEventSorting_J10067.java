/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.patch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPluginInfo;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.service.RangerPluginInfoService;
import org.apache.ranger.util.CLIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PatchForPluginStatusEventSorting_J10067 extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(PatchForPluginStatusEventSorting_J10067.class);

    private static final int BATCH_SIZE  = 25;
    private static final int NUM_THREADS = 5;

    private TransactionTemplate          txTemplate;
    private Iterator<XXPluginInfo>       pluginInfoIterator = Collections.emptyIterator();
    private final AtomicLong             migratedCount      = new AtomicLong();

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RangerPluginInfoService pluginInfoService;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    public static void main(String[] args) {
        logger.info("PatchForPluginStatusEventSorting_J10067: main()");

        try {
            PatchForPluginStatusEventSorting_J10067 loader = (PatchForPluginStatusEventSorting_J10067) CLIUtil.getBean(PatchForPluginStatusEventSorting_J10067.class);

            loader.init();

            while (loader.isMoreToProcess()) {
                loader.load();
            }

            logger.info("Load complete. Exiting!!!");
            System.exit(0);
        } catch (Exception e) {
            logger.error("Error loading plugin status event sorting patch.", e);
            System.exit(1);
        }
    }

    @Override
    public void init() throws Exception {
        txTemplate = new TransactionTemplate(txManager);
        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public void execLoad() {
        logger.info("==> PatchForPluginStatusEventSorting_J10067.execLoad()");

        try {
            updateXPluginInfoForEventSorting();
        } catch (Exception e) {
            logger.error("Error while updating plugin info for event sorting.", e);
        }

        logger.info("<== PatchForPluginStatusEventSorting_J10067.execLoad()");
    }

    @Override
    public void printStats() {
        logger.debug("Starting plugin status migration with {} threads and a commit batch size of {}.", NUM_THREADS, BATCH_SIZE);
    }

    private void updateXPluginInfoForEventSorting() throws Exception {
        logger.info("==> updateXPluginInfoForEventSorting() ");

        String             queryStr = "SELECT obj FROM " + XXPluginInfo.class.getName() + " obj ";
        List<XXPluginInfo> xObjList  = daoMgr.getEntityManager().createQuery(queryStr, XXPluginInfo.class).getResultList();

        if (CollectionUtils.isNotEmpty(xObjList)) {
            logger.info("Found {} plugin info records to process.", xObjList.size());

            pluginInfoIterator = xObjList.iterator();

            PluginStatusThread[] migrationThreads = new PluginStatusThread[NUM_THREADS];

            for (int i = 0; i < migrationThreads.length; i++) {
                migrationThreads[i] = new PluginStatusThread();
                migrationThreads[i].start();
            }

            for (PluginStatusThread migrationThread : migrationThreads) {
                migrationThread.join();
            }
        } else {
            logger.info("No plugin info records found to process.");
        }

        logger.info("<== updateXPluginInfoForEventSorting() ");
    }

    private void updateXPluginInfo(XXPluginInfo xObj) {
        RangerPluginInfo pluginInfo = pluginInfoService.populateViewObject(xObj);

        daoMgr.getXXPluginInfo().update(pluginInfoService.populateDBObject(pluginInfo));

        long count = migratedCount.incrementAndGet();

        if (count % 1000 == 0) {
            logger.info("PROGRESS: {} plugin status records processed.", count);
        }
    }

    private void fetchNextBatch(List<XXPluginInfo> pluginInfos) {
        pluginInfos.clear();

        synchronized (this) {
            for (int i = 0; i < BATCH_SIZE && pluginInfoIterator.hasNext(); i++) {
                pluginInfos.add(pluginInfoIterator.next());
            }
        }
    }

    class PluginStatusThread extends Thread {
        @Override
        public void run() {
            List<XXPluginInfo> pluginInfos = new ArrayList<>(BATCH_SIZE);

            fetchNextBatch(pluginInfos);

            while (!pluginInfos.isEmpty()) {
                txTemplate.execute((TransactionCallback<Void>) status -> {
                    for (XXPluginInfo pluginInfo : pluginInfos) {
                        updateXPluginInfo(pluginInfo);
                    }

                    return null;
                });
                fetchNextBatch(pluginInfos);
            }
        }
    }
}
