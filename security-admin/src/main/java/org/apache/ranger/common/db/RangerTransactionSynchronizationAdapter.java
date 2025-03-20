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

package org.apache.ranger.common.db;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.service.RangerTransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.OptimisticLockException;

import java.util.ArrayList;
import java.util.List;

@Component
public class RangerTransactionSynchronizationAdapter extends TransactionSynchronizationAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerTransactionSynchronizationAdapter.class);

    private static final ThreadLocal<List<Runnable>> RUNNABLES              = new ThreadLocal<>();
    private static final ThreadLocal<List<Runnable>> RUNNABLES_ASYNC        = new ThreadLocal<>();
    private static final ThreadLocal<List<Runnable>> RUNNABLES_AFTER_COMMIT = new ThreadLocal<>();

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    @Autowired
    RangerTransactionService transactionService;

    public void executeOnTransactionCompletion(Runnable runnable) {
        LOG.debug("Submitting new runnable {{}} to run after completion", runnable);

        addRunnable(runnable, RUNNABLES);
    }

    public void executeAsyncOnTransactionComplete(Runnable runnable) {
        LOG.debug("Submitting new runnable {{}} to run async after completion", runnable);

        addRunnable(runnable, RUNNABLES_ASYNC);
    }

    public void executeOnTransactionCommit(Runnable runnable) {
        LOG.debug("Submitting new runnable {{}} to run after transaction is committed", runnable);

        addRunnable(runnable, RUNNABLES_AFTER_COMMIT);
    }

    @Override
    public void afterCompletion(int status) {
        LOG.debug("==> RangerTransactionSynchronizationAdapter.afterCompletion(status={})", status == STATUS_COMMITTED ? "COMMITTED" : "ROLLED_BACK");

        final boolean  isParentTransactionCommitted = status == STATUS_COMMITTED;
        List<Runnable> runnablesAfterCommit         = RUNNABLES_AFTER_COMMIT.get();
        List<Runnable> runnables                    = RUNNABLES.get();
        List<Runnable> asyncRunnables               = RUNNABLES_ASYNC.get();

        RUNNABLES_AFTER_COMMIT.remove();
        RUNNABLES.remove();
        RUNNABLES_ASYNC.remove();

        if (asyncRunnables != null) {
            for (Runnable asyncRunnable : asyncRunnables) {
                transactionService.scheduleToExecuteInOwnTransaction(asyncRunnable, 0L);
            }
        }

        if (isParentTransactionCommitted) {
            // Run tasks scheduled to run after transaction is successfully committed
            runRunnables(runnablesAfterCommit, true);
        }

        // Run other tasks scheduled to run after transaction completes
        runRunnables(runnables, false);

        LOG.debug("<== RangerTransactionSynchronizationAdapter.afterCompletion(status={})", status == STATUS_COMMITTED ? "COMMITTED" : "ROLLED_BACK");
    }

    private void addRunnable(Runnable runnable, ThreadLocal<List<Runnable>> threadRunnables) {
        /*
        From TransactionSynchronizationManager documentation:
        TransactionSynchronizationManager is a central helper that manages resources and transaction synchronizations per thread.
        Resource management code should only register synchronizations when this manager is active,
        which can be checked via isSynchronizationActive(); it should perform immediate resource cleanup else.
        If transaction synchronization isn't active, there is either no current transaction,
        or the transaction manager doesn't support transaction synchronization.

        Note: Synchronization is an Interface for transaction synchronization callbacks which is implemented by
        TransactionSynchronizationAdapter
        */
        if (!registerSynchronization()) {
            LOG.info("Transaction synchronization is NOT ACTIVE. Executing right now runnable {{}}", runnable);

            runnable.run();

            return;
        }

        List<Runnable> runnables = threadRunnables.get();

        if (runnables == null) {
            runnables = new ArrayList<>();

            threadRunnables.set(runnables);
        }

        runnables.add(runnable);
    }

    private boolean registerSynchronization() {
        final boolean ret = TransactionSynchronizationManager.isSynchronizationActive();

        if (ret) {
            List<Runnable> threadRunnablesOnCompletion = RUNNABLES.get();
            List<Runnable> threadRunnablesOnCommit     = RUNNABLES_AFTER_COMMIT.get();
            List<Runnable> threadRunnablesAsync        = RUNNABLES_ASYNC.get();

            if (threadRunnablesOnCompletion == null && threadRunnablesOnCommit == null && threadRunnablesAsync == null) {
                TransactionSynchronizationManager.registerSynchronization(this);
            }
        }

        return ret;
    }

    private void runRunnables(final List<Runnable> runnables, final boolean isParentTransactionCommitted) {
        LOG.debug("==> RangerTransactionSynchronizationAdapter.runRunnables()");

        if (CollectionUtils.isNotEmpty(runnables)) {
            LOG.debug("Executing {{}} runnables", runnables.size());

            for (Runnable runnable : runnables) {
                boolean isThisTransactionCommitted;

                do {
                    Object result = null;
                    try {
                        //Create new transaction
                        TransactionTemplate txTemplate = new TransactionTemplate(txManager);

                        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

                        result = txTemplate.execute(status -> {
                            Object result1 = null;

                            LOG.debug("Executing runnable {{}}", runnable);

                            try {
                                runnable.run();

                                result1 = runnable;

                                LOG.debug("executed runnable {}", runnable);
                            } catch (OptimisticLockException optimisticLockException) {
                                LOG.debug("Failed to execute runnable {} because of OptimisticLockException", runnable);
                            } catch (Throwable e) {
                                LOG.debug("Failed to execute runnable {}", runnable, e);
                            }

                            return result1;
                        });
                    } catch (OptimisticLockException optimisticLockException) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Failed to commit TransactionService transaction for runnable:[{}]", runnable);
                        }
                    } catch (TransactionSystemException tse) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Failed to commit TransactionService transaction, exception:[{}]", String.valueOf(tse));
                        }
                    } catch (Throwable e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Failed to commit TransactionService transaction, throwable:[{}]", String.valueOf(e));
                        }
                    }

                    isThisTransactionCommitted = result == runnable;

                    if (isParentTransactionCommitted) {
                        if (!isThisTransactionCommitted) {
                            LOG.info("Failed to commit runnable:[{}]. Will retry!", runnable);
                        } else {
                            LOG.debug("Committed runnable:[{}].", runnable);
                        }
                    }
                } while (isParentTransactionCommitted && !isThisTransactionCommitted);
            }
        } else {
            LOG.debug("No runnables to execute");
        }

        LOG.debug("<== RangerTransactionSynchronizationAdapter.runRunnables()");
    }
}
