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
import com.google.gson.Gson;
import liquibase.Scope;
import liquibase.command.CommandResults;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.HashMap;

public class LiquibaseDBUtilsMain {
    private static final Logger LOG = LoggerFactory.getLogger(LiquibaseDBUtilsMain.class);

    private LiquibaseDBUtilsMain() {
    }

    public static void main(String[] argv) {
        ArgsParserLiquibaseDBUtilsMain argsParserLiquibaseDBUtilsMain = new ArgsParserLiquibaseDBUtilsMain();

        JCommander.newBuilder()
                .addObject(argsParserLiquibaseDBUtilsMain)
                .build()
                .parse(argv);

        String         op            = argsParserLiquibaseDBUtilsMain.opName;
        String         tagName       = argsParserLiquibaseDBUtilsMain.tagName;
        String         serviceName   = argsParserLiquibaseDBUtilsMain.serviceName;
        ICommandDriver commandDriver = SpringContext.getBean(ICommandDriver.class);
        if (commandDriver == null) {
            throw new RuntimeException("commandDriver not found. Exiting!");
        }
        HashMap<String, Object> response = new HashMap<>();
        response.put("op_status", Constants.RETURN_CODE_FAILURE);
        try {
            Scope.child(Scope.Attr.resourceAccessor, new ClassLoaderResourceAccessor(), () -> {
                switch (op) {
                    case "isUpgradeComplete":
                        final String tagNamePostUpdate = Utils.getPostUpdateTagName(tagName);
                        CommandResults upgradeCompleteTagExistsResult = commandDriver.tagExists(serviceName, tagNamePostUpdate);
                        if (LiquibaseCommandResultsUtil.doesTagExist(upgradeCompleteTagExistsResult)) {
                            LOG.info("isUpgradeComplete={}", Constants.RETURN_CODE_SUCCESS);
                            response.put("op_status", Constants.RETURN_CODE_SUCCESS);
                            response.put("isUpgradeComplete", Constants.RETURN_CODE_SUCCESS);
                        } else {
                            LOG.info("isUpgradeComplete={}", Constants.RETURN_CODE_FAILURE);
                            response.put("op_status", Constants.RETURN_CODE_SUCCESS);
                            response.put("isUpgradeComplete", Constants.RETURN_CODE_FAILURE);
                        }
                        break;
                    case "isFinalizeComplete":
                        final String tagNameForFinalizeComplete = Utils.getFinalizeTagName(tagName);
                        CommandResults finalizeCompleteTagExistsResult = commandDriver.tagExists(serviceName, tagNameForFinalizeComplete);
                        if (LiquibaseCommandResultsUtil.doesTagExist(finalizeCompleteTagExistsResult)) {
                            LOG.info("isFinalizeComplete={}", Constants.RETURN_CODE_SUCCESS);
                            response.put("op_status", Constants.RETURN_CODE_SUCCESS);
                            response.put("isFinalizeComplete", Constants.RETURN_CODE_SUCCESS);
                        } else {
                            LOG.info("isFinalizeComplete={}", Constants.RETURN_CODE_FAILURE);
                            response.put("op_status", Constants.RETURN_CODE_SUCCESS);
                            response.put("isFinalizeComplete", Constants.RETURN_CODE_FAILURE);
                        }
                        break;
                        /*
                        TODO: Implement releaseLock here. Workaround: execute command manually or use sql to change value in databasechangeloglock table manually
                         Required in some cases when liquibase does not automatically release lock incase of unclean exit of the process
                    case "releaseLock":
                        throw new NotImplementedException("releaseLock not yet implemented");
                         */
                    default:
                        throw new UnsupportedOperationException("op=" + op + " is not valid");
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // this sys out should be the last printed line (assumption only for the bash script consuming the returned result, otherwise not required)
        System.out.println(new Gson().toJson(response));
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
