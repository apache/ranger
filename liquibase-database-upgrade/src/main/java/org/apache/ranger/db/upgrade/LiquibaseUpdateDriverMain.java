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
package org.apache.ranger.db.upgrade;

import com.beust.jcommander.JCommander;
import liquibase.Scope;
import liquibase.command.CommandResults;
import liquibase.exception.CommandExecutionException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class LiquibaseUpdateDriverMain {
    private static final Logger LOG = LoggerFactory.getLogger(LiquibaseUpdateDriverMain.class);

    private LiquibaseUpdateDriverMain() {
    }

    public static void main(String[] argv) throws Exception {
        ArgsParserLiquibaseUpdateDriverMain argsParserLiquibaseUpdateDriverMain = new ArgsParserLiquibaseUpdateDriverMain();

        JCommander.newBuilder()
                .addObject(argsParserLiquibaseUpdateDriverMain)
                .build()
                .parse(argv);

        String serviceName = argsParserLiquibaseUpdateDriverMain.serviceName;

        LiquibasePropertiesFactory factory = SpringContext.getBean(LiquibasePropertiesFactory.class);
        if (factory == null) {
            throw new RuntimeException("Could not initialize LiquibasePropertiesFactory. Exiting.");
        }
        final String masterChangelogFilenameUpgrade  = factory.getMasterChangelog(serviceName);
        final String masterChangelogFilenameFinalize = factory.getFinalizeChangelog(serviceName);
        final String tagName                         = argsParserLiquibaseUpdateDriverMain.tagName;
        final String tagNamePostUpdate               = Utils.getPostUpdateTagName(tagName);
        final String tagNameForFinalizeComplete      = Utils.getFinalizeTagName(tagName);
        LOG.info("tagName={} tagNamePostUpdate={} tagNameForFinalizeComplete={}", tagName, tagNamePostUpdate, tagNameForFinalizeComplete);
        LOG.info("masterChangelogFilenameUpgrade={} masterChangelogFilenameFinalize={}", masterChangelogFilenameUpgrade, masterChangelogFilenameFinalize);
        StatusCheckScheduledExecutorService updateStatusCheckService = SpringContext.getBean(
                StatusCheckScheduledExecutorService.class);
        if (updateStatusCheckService == null) {
            LOG.error("Could not initialize updateStatusCheckService but continuing execution");
        }
        ICommandDriver commandDriver = SpringContext.getBean(ICommandDriver.class);
        if (commandDriver == null) {
            throw new RuntimeException("Could not initialize commandDriver. Exiting.");
        }
        LOG.info("Main_commandDriverClass={} \n", commandDriver.getClass());
        try {
            Scope.child(Scope.Attr.resourceAccessor, new ClassLoaderResourceAccessor(), () -> {
                switch (argsParserLiquibaseUpdateDriverMain.opName) {
                    case "rollback":
                        CommandResults result = commandDriver.tagExists(serviceName, tagNameForFinalizeComplete);
                        LOG.info("finalize tag check result (for rollback): {}", result.getResults());
                        LOG.info("finalize tag check result (for rollback): {}", result.getResult("tagExistsResult").equals("false"));
                        LOG.info("finalize tag check result (for rollback): {} ", result.getResult("tagExistsResult").getClass());
                        if (LiquibaseCommandResultsUtil.doesTagExist(result)) {
                            throw new Exception("ATTEMPTING TO ROLLBACK AFTER FINALIZE WAS COMPLETED. ABORTING.");
                        } else {
                            LOG.info("Starting Rollback");
                            result = commandDriver.rollback(serviceName, tagName, masterChangelogFilenameUpgrade);
                            LOG.info("rollback result:{}", result.getResults());
                        }
                        break;
                    case "update":
                        if (updateStatusCheckService != null) {
                            updateStatusCheckService.startStatusChecks(serviceName, masterChangelogFilenameUpgrade);
                        }
                        result = commandDriver.tagExists(serviceName, tagNameForFinalizeComplete);
                        if (LiquibaseCommandResultsUtil.doesTagExist(result)) {
                            throw new Exception("ATTEMPTING TO ROLLBACK AFTER FINALIZE WAS COMPLETED. ABORTING.");
                        } else {
                            LOG.info("Starting Schema Upgrade");
                            result = commandDriver.tag(serviceName, tagName);
                            LOG.info("tagging result:{}", result.getResults());
                            result = commandDriver.update(serviceName, masterChangelogFilenameUpgrade);
                            LOG.info("update result:{}", result.getResults());
                            if (!LiquibaseCommandResultsUtil.isUpdateSuccessful(result)) {
                                throw new CommandExecutionException(
                                        "Issue occurred during execution of update command");
                            } else {
                                result = commandDriver.tag(serviceName, tagNamePostUpdate);
                                LOG.info("tagging result post update:{}", result.getResults());
                                LOG.info("Successfully upgraded {} database", serviceName);
                            }
                        }
                        break;
                    case "finalize":
                        result = commandDriver.tagExists(serviceName, tagNamePostUpdate);
                        LOG.info("tag check result (for finalize): {}", result.getResult("tagExistsResult"));
                        LOG.info("tag check result (for finalize): {}", result.getResult("tagExistsResult"));

                        Boolean tagExists = LiquibaseCommandResultsUtil.doesTagExist(result);
                        LOG.info("tagExists={}", tagExists);
                        if (tagExists) {
                            LOG.info("Finalizing Schema Upgrade. Do not try rollback after this.");
                            result = commandDriver.update(serviceName, masterChangelogFilenameFinalize);
                            if (LiquibaseCommandResultsUtil.isUpdateSuccessful(result)) {
                                result = commandDriver.tag(serviceName, tagNameForFinalizeComplete);
                                LOG.info("tagging result after finalize:{}", result.getResults());
                                LOG.info("Database upgrade finalized to: {}", tagNameForFinalizeComplete);
                            } else {
                                throw new CommandExecutionException("Issue occurred during execution of finalize command");
                            }
                        } else {
                            throw new Exception("ATTEMPTING TO FINALIZE BEFORE UPDATE COMPLETED. ABORTING.");
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(argsParserLiquibaseUpdateDriverMain.opName + " not supported");
                }
            });
        } catch (CommandExecutionException e) {
            switch (argsParserLiquibaseUpdateDriverMain.opName) {
                case "update":
                    LOG.info(e.getMessage());
                    LOG.info("Starting automatic rollback since exception occurred during upgrade");
                    CommandResults result;
                    try {
                        result = commandDriver.rollback(serviceName, tagName, masterChangelogFilenameUpgrade);
                    } catch (CommandExecutionException ex) {
                        LOG.info("Failed to auto rollback to the previous state. Manually restore using the database dump/backup as soon as possible");
                        throw new CommandExecutionException(ex);
                    }
                    LOG.info("Auto rollback result:{}", result.getResults());
                    break;
                case "rollback":
                    LOG.info("Exception Occurred during rollback");
                    break;
                case "finalize":
                    LOG.info("Exception Occurred during finalize");
                    break;
            }
            throw new CommandExecutionException(e);
        } finally {
            if (updateStatusCheckService != null) {
                updateStatusCheckService.stopStatusChecks();
            }
            LOG.info("=============================END OF UPGRADE EXECUTION=============================");
        }
    }

    static {
        // Liquibase logs to java.util.logging
        // Below statements ensures that the jul-to-slf4j binding works properly
        // Remove existing handlers attached to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        // Bridge j.u.l logging to SLF4J
        SLF4JBridgeHandler.install();

        //initialize spring beans
        SpringContext.init();
    }
}
