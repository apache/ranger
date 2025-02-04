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
package org.apache.ranger.patch.cliutil;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.patch.BaseLoader;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.util.CLIUtil;
import org.apache.ranger.view.VXTrxLog;
import org.apache.ranger.view.VXTrxLogV2;
import org.apache.ranger.view.VXTrxLogV2.ObjectChangeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class TrxLogV2MigrationUtil extends BaseLoader {
    private static final Logger logger = LoggerFactory.getLogger(TrxLogV2MigrationUtil.class);

    private final Stats               stats;
    private       TransactionTemplate txTemplate;
    private       Iterator<String>    trxIdIter       = Collections.emptyIterator();
    private       int                 commitBatchSize = 25;


    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;


    public static void main(String[] args) {
        if (logger.isDebugEnabled()) {
            logger.info("TrxLogV2MigrationUtil: main()");
        }

        try {
            TrxLogV2MigrationUtil loader = (TrxLogV2MigrationUtil) CLIUtil.getBean(TrxLogV2MigrationUtil.class);

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

    public TrxLogV2MigrationUtil() {
        this.stats = new Stats();
    }

    @Override
    public void init() throws Exception {
        txTemplate = new TransactionTemplate(txManager);

        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public void execLoad() {
        logger.info("==> TrxLogV2MigrationUtil.execLoad()");

        try {
            if (isXTrxLogTableExists()) {
                migrateTrxLogs();
            }
        } catch (Exception e) {
            logger.error("Error while migrating trx logs from v1 to v2", e);
        }

        logger.info("<== TrxLogV2MigrationUtil.execLoad(): migration completed. Transaction counts(total: {}, migrated: {}, already-migrated: {}, failed: {})", stats.totalCount, stats.migratedCount, stats.alreadyMigratedCount, stats.failedCount);
    }

    private boolean isXTrxLogTableExists() {
        try {
            // This query checks whether the 'x_trx_log' table exists without retrieving any actual data.
            return !daoMgr.getEntityManager().createNativeQuery("select count(*) from x_trx_log where 1=2").getResultList().isEmpty();
        } catch (Exception e) {
            logger.warn("Table 'x_trx_log' does not exist. Skipping migration.");
            return false;
        }
    }

    @Override
    public void printStats() {
        stats.logStats();
    }

    private void migrateTrxLogs() throws Exception {
        logger.info("==> TrxLogV2MigrationUtil.migrateTrxLogs()");

        int trxRetentionDays = config.getInt("ranger.admin.migrate.transaction_records.retention.days", -1);
        int threadCount      = config.getInt("ranger.admin.migrate.transaction_records.thread.count", 5);

        commitBatchSize = config.getInt("ranger.admin.migrate.transaction_records.commit.batch.size", 25);

        final List<String> uniqueTrxIdList;

        if (trxRetentionDays < 0) {
            uniqueTrxIdList = daoMgr.getEntityManager().createNamedQuery("XXTrxLog.findDistinctTrxIds", String.class).getResultList();
        } else {
            // Define start and end dates based on the retention period
            Date startDate = Timestamp.valueOf(LocalDate.now().minusDays(trxRetentionDays).atStartOfDay());
            Date endDate   = Timestamp.valueOf(LocalDate.now().atTime(23, 59, 59, 999999999));

            uniqueTrxIdList = daoMgr.getEntityManager().createNamedQuery("XXTrxLog.findDistinctTrxIdsByTimeInterval", String.class)
                    .setParameter("startDate", startDate)
                    .setParameter("endDate", endDate)
                    .getResultList();
        }

        trxIdIter = uniqueTrxIdList.iterator();

        stats.totalCount = uniqueTrxIdList.size();

        logger.info("Found {} transactions to migrate", stats.totalCount);

        logger.info("Starting {} threads to migrate, commit batch size: {}", threadCount, commitBatchSize);

        LogMigrationThread[] migrationThreads = new LogMigrationThread[threadCount];

        for (int i = 0; i < migrationThreads.length; i++) {
            migrationThreads[i] = new LogMigrationThread();

            migrationThreads[i].start();
        }

        for (LogMigrationThread migrationThread : migrationThreads) {
            migrationThread.join();
        }

        logger.info("<== TrxLogV2MigrationUtil.migrateTrxLogs()");
    }

    private void migrateTrxLog(String trxId) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> TrxLogV2MigrationUtil.createTransactionLogByTrxId()");
        }

        List<XXTrxLogV2> trxLogsV2 = daoMgr.getXXTrxLogV2().findByTransactionId(trxId);

        if (CollectionUtils.isNotEmpty(trxLogsV2)) {
            if (logger.isDebugEnabled()) {
                logger.debug("transaction({}): already migrated to v2", trxId);
            }

            stats.incrAlreadyMigratedCount();
        } else {
            List<XXTrxLog> v1TrxLogs = getV1TrxLogs(trxId);

            if (!v1TrxLogs.isEmpty()) {
                ObjectChangeInfo objChangeInfo = new ObjectChangeInfo();

                for (XXTrxLog v1TrxLog : v1TrxLogs) {
                    objChangeInfo.addAttribute(v1TrxLog.getAttributeName(), v1TrxLog.getPreviousValue(), v1TrxLog.getNewValue());
                }

                XXTrxLog firstTrxLog = v1TrxLogs.get(0);

                createTrxLog(firstTrxLog, objChangeInfo);

                if (logger.isDebugEnabled()) {
                    logger.debug("transaction({}): migrated {} v1 records", trxId, v1TrxLogs.size());
                }

                stats.incrMigratedCount(firstTrxLog.getId(), firstTrxLog.getCreateTime());
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("transaction({}): no v1 records found", trxId);
                }

                stats.incrFailedCount();
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== TrxLogV2MigrationUtil.createTransactionLogByTrxId()");
        }
    }

    private List<XXTrxLog> getV1TrxLogs(String trxId) {
        List<XXTrxLog> ret = Collections.emptyList();

        try {
            List<Object[]> rows = daoMgr.getEntityManager().createNamedQuery("XXTrxLog.findByTrxIdForMigration", Object[].class).setParameter("transactionId", trxId).getResultList();

            if (rows != null) {
                ret = new ArrayList<>(rows.size());

                for (Object[] row : rows) {
                    XXTrxLog trxLog = new XXTrxLog();

                    trxLog.setId(toLong(row[0]));
                    trxLog.setCreateTime((Date) row[1]);
                    trxLog.setUpdateTime(trxLog.getCreateTime());
                    trxLog.setAddedByUserId(toLong(row[2]));
                    trxLog.setUpdatedByUserId(trxLog.getAddedByUserId());
                    trxLog.setObjectClassType(toInt(row[3]));
                    trxLog.setObjectId(toLong(row[4]));
                    trxLog.setObjectName((String) row[5]);
                    trxLog.setParentObjectClassType(toInt(row[6]));
                    trxLog.setParentObjectId(toLong(row[7]));
                    trxLog.setParentObjectName((String) row[8]);
                    trxLog.setAttributeName((String) row[9]);
                    trxLog.setPreviousValue((String) row[10]);
                    trxLog.setNewValue((String) row[11]);
                    trxLog.setTransactionId((String) row[12]);
                    trxLog.setAction((String) row[13]);
                    trxLog.setRequestId((String) row[14]);
                    trxLog.setSessionId((String) row[15]);
                    trxLog.setSessionType((String) row[16]);

                    ret.add(trxLog);
                }
            }
        } catch (Exception excp) {
            logger.error("failed to get v1 transaction logs for trxId {}", trxId, excp);

            ret = Collections.emptyList();
        }

        return ret;
    }

    private void createTrxLog(XXTrxLog v1TrxLog, ObjectChangeInfo objChangeInfo) {
        VXTrxLogV2 trxLogV2 = new VXTrxLogV2(toVXTrxLog(v1TrxLog));

        trxLogV2.setChangeInfo(objChangeInfo);

        XXTrxLogV2 dbObj = toDBObject(trxLogV2);

        dbObj.setAddedByUserId(v1TrxLog.getAddedByUserId());

        daoMgr.getXXTrxLogV2().create(dbObj);
    }

    private void fetchNextBatch(List<String> trxIds) {
        trxIds.clear();

        synchronized (this) {
            for (int i = 0; i < commitBatchSize; i++) {
                if (!trxIdIter.hasNext()) {
                    break;
                }

                trxIds.add(trxIdIter.next());
            }
        }
    }

    private static VXTrxLog toVXTrxLog(XXTrxLog trxLog) {
        VXTrxLog ret = new VXTrxLog();

        ret.setId(trxLog.getId());
        ret.setCreateDate(trxLog.getCreateTime());
        ret.setUpdateDate(trxLog.getUpdateTime());
        ret.setObjectClassType(trxLog.getObjectClassType());
        ret.setObjectId(trxLog.getObjectId());
        ret.setObjectName(trxLog.getObjectName());
        ret.setParentObjectClassType(trxLog.getParentObjectClassType());
        ret.setParentObjectId(trxLog.getParentObjectId());
        ret.setParentObjectName(trxLog.getParentObjectName());
        ret.setAction(trxLog.getAction());
        ret.setRequestId(trxLog.getRequestId());
        ret.setTransactionId(trxLog.getTransactionId());
        ret.setSessionId(trxLog.getSessionId());
        ret.setSessionType(trxLog.getSessionType());

        return ret;
    }

    private static XXTrxLogV2 toDBObject(VXTrxLogV2 vObj) {
        XXTrxLogV2 ret = new XXTrxLogV2(vObj.getObjectClassType(), vObj.getObjectId(), vObj.getObjectName(),
                                        vObj.getParentObjectClassType(), vObj.getParentObjectId(), vObj.getParentObjectName(), vObj.getAction());

        ret.setCreateTime(vObj.getCreateDate());
        ret.setChangeInfo(toJson(vObj.getChangeInfo()));
        ret.setTransactionId(vObj.getTransactionId());
        ret.setSessionId(vObj.getSessionId());
        ret.setRequestId(vObj.getRequestId());
        ret.setSessionType(vObj.getSessionType());

        return ret;
    }

    private static String toJson(ObjectChangeInfo changeInfo) {
        try {
            return JsonUtilsV2.objToJson(changeInfo);
        } catch (Exception excp) {
            logger.error("Error converting ObjectChangeInfo to JSON", excp);

            return null;
        }
    }

    private static int toInt(Object obj) {
        return obj instanceof Number ? ((Number) obj).intValue() : 0;
    }

    private static long toLong(Object obj) {
        return obj instanceof Long ? ((Number) obj).longValue() : 0L;
    }

    class LogMigrationThread extends Thread {
        @Override
        public void run() {
            List<String> trxIds = new ArrayList<>(commitBatchSize);

            for (fetchNextBatch(trxIds); !trxIds.isEmpty(); fetchNextBatch(trxIds)) {
                txTemplate.execute((TransactionCallback<Void>) status -> {
                    for (String trxId : trxIds) {
                        migrateTrxLog(trxId);
                    }

                    return null;
                });
            }
        }
    }

    public static class Stats {
        private long                        totalCount;
        private final AtomicLong            migratedCount        = new AtomicLong();
        private final AtomicLong            failedCount          = new AtomicLong();
        private final AtomicLong            alreadyMigratedCount = new AtomicLong();
        private final AtomicLong            processedCount       = new AtomicLong();
        private final AtomicReference<Long> lastTrxId            = new AtomicReference<>();
        private final AtomicReference<Date> lastTrxDate          = new AtomicReference<>();

        private final ThreadLocal<SimpleDateFormat> dateFormatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy/MM/dd HH:mm:ss Z"));

        public void incrMigratedCount(Long trxId, Date trxDate) {
            migratedCount.incrementAndGet();
            lastTrxId.set(trxId);
            lastTrxDate.set(trxDate);

            incrProcessedCount();
        }

        public void incrFailedCount() {
            failedCount.incrementAndGet();

            incrProcessedCount();
        }

        public void incrAlreadyMigratedCount() {
            alreadyMigratedCount.incrementAndGet();

            incrProcessedCount();
        }

        private void incrProcessedCount() {
            if (processedCount.incrementAndGet() % 1000 == 0) {
                logStats();
            }
        }

        public void logStats() {
            logger.info("PROGRESS: {} of {} transactions processed. Last migrated transaction(id={}, time={}). Counts(migrated: {}, failed: {}, already-migrated: {})",
                        processedCount.get(), totalCount, lastTrxId.get(), toString(lastTrxDate.get()), migratedCount.get(), failedCount.get(), alreadyMigratedCount.get());
        }

        private String toString(Date date) {
            return date != null ? dateFormatter.get().format(date) : null;
        }
    }
}
