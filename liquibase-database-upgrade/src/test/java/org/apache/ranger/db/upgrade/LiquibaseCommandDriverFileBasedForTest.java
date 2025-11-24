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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@Component
public class LiquibaseCommandDriverFileBasedForTest implements ICommandDriver {
    @Value("${test_db_file.name:test.txt}")
    private String filename;

    @Override
    public CommandResults tag(String serviceName, String tagName) throws CommandExecutionException {
        FileDBForTest.tag(filename, tagName);
        return getCommandResultsObject(new TreeMap<>(), new CommandScope("tag"));
    }

    @Override
    public CommandResults rollback(String serviceName, String tagName, String changelogFilename) throws CommandExecutionException {
        try {
            FileDBForTest.removeLinesAfterTag(filename, tagName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getCommandResultsObject(new TreeMap<>(), new CommandScope("rollback"));
    }

    @Override
    public CommandResults tagExists(String serviceName, String tagName) throws CommandExecutionException {
        SortedMap<String, Object> result = new TreeMap<>();
        if (FileDBForTest.tagExists(filename, tagName)) {
            result.put("tagExistsResult", true);
        } else {
            result.put("tagExistsResult", false);
        }
        return getCommandResultsObject(result, new CommandScope("tagExists"));
    }

    @Override
    public CommandResults update(String serviceName, String changelogFilename) throws CommandExecutionException {
        List<String> lines = FileDBForTest.readLines(changelogFilename);
        System.out.println("lines=" + lines);
        FileDBForTest.writeLines(filename, lines, true);
        return getCommandResultsObject(new TreeMap<>(), new CommandScope("update"));
    }

    @Override
    public CommandResults status(String serviceName, String changelogFilename) throws CommandExecutionException {
        SortedMap<String, Object> result  = new TreeMap<>();
        int                       numRows = FileDBForTest.readLines(filename).size();
        result.put("numRows", numRows);
        return getCommandResultsObject(result, new CommandScope("status"));
    }

    private CommandResults getCommandResultsObject(SortedMap<String, Object> resultValues, CommandScope commandScope) {
        Class<?>       clazz = CommandResults.class;
        Constructor<?> constructor;
        try {
            constructor = clazz.getDeclaredConstructor(SortedMap.class, CommandScope.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        constructor.setAccessible(true);
        try {
            return (CommandResults) constructor.newInstance(resultValues, commandScope);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
