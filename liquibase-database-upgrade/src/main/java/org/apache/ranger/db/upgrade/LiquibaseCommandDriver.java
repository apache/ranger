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

import liquibase.command.CommandResults;
import liquibase.command.CommandScope;
import liquibase.exception.CommandExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
public class LiquibaseCommandDriver implements ICommandDriver {
    private static final Logger LOG = LoggerFactory.getLogger(LiquibaseCommandDriver.class);
    @Autowired
    LiquibasePropertiesFactory factory;

    @Override
    public CommandResults tag(String serviceName, String tagName) throws CommandExecutionException {
        HashMap<String, Object> tagParams = new HashMap<>();
        tagParams.put("tag", tagName);
        return executeLiquibaseCommand(serviceName, "tag", tagParams);
    }

    @Override
    public CommandResults rollback(String serviceName, String tagName, String changelogFilename)
            throws CommandExecutionException {
        HashMap<String, Object> rollbackParams = new HashMap<>();
        rollbackParams.put("tag", tagName);
        rollbackParams.put("changelogFile", changelogFilename);
        return executeLiquibaseCommand(serviceName, "rollback", rollbackParams);
    }

    @Override
    public CommandResults tagExists(String serviceName, String tagName) throws CommandExecutionException {
        HashMap<String, Object> tagParams = new HashMap<>();
        tagParams.put("tag", tagName);
        return executeLiquibaseCommand(serviceName, "tagExists", tagParams);
    }

    @Override
    public CommandResults update(String serviceName, String changelogFilename) throws CommandExecutionException {
        HashMap<String, Object> updateParams = new HashMap<>();
        updateParams.put("changelogFile", changelogFilename);
        return executeLiquibaseCommand(serviceName, "update", updateParams);
    }

    @Override
    public CommandResults status(String serviceName, String changelogFilename) throws CommandExecutionException {
        HashMap<String, Object> statusParams = new HashMap<>();
        statusParams.put("changelogFile", changelogFilename);
        return executeLiquibaseCommand(serviceName, "status", statusParams);
    }

    private CommandResults executeLiquibaseCommand(
            String serviceName,
            String commandName,
            HashMap<String, Object> commandSpecificArguments
    ) throws CommandExecutionException {
        if (factory == null) {
            LOG.error("LiquibasePropertiesFactory is null, liquibase command cannot execute");
        }
        String       url          = factory.getUrl(serviceName);
        String       username     = factory.getUsername(serviceName);
        String       password     = factory.getPassword(serviceName);
        String       driver       = factory.getDriver(serviceName);
        CommandScope commandScope = new CommandScope(commandName);
        commandScope.addArgumentValue("url", url);
        commandScope.addArgumentValue("username", username);
        commandScope.addArgumentValue("password", password);
        commandScope.addArgumentValue("driver", driver);
        for (String argumentName : commandSpecificArguments.keySet()) {
            commandScope.addArgumentValue(argumentName, commandSpecificArguments.get(argumentName));
        }
        return commandScope.execute();
    }
}
