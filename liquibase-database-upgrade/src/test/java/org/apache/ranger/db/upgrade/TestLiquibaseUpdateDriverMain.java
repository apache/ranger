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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mockStatic;

public class TestLiquibaseUpdateDriverMain {
    static String testFile = "test.txt";

    @BeforeAll
    public static void setup() {
        FileDBForTest.deleteFileIfExists(testFile);
    }

    @AfterAll
    public static void cleanup() {
        FileDBForTest.deleteFileIfExists(testFile);
    }

    @Test
    public void testUpgradeSucceed() {
        System.out.println("===== Test Update====");

        FileDBForTest.deleteFileIfExists(testFile);
        try {
            try (MockedStatic<LiquibaseCommandResultsUtil> liquibaseCommandResultsUtilMock = mockStatic(LiquibaseCommandResultsUtil.class)) {
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.isUpdateSuccessful(Mockito.any(CommandResults.class))).thenReturn(true);
                System.setProperty("applicationContext.files", "liquibaseTestApplicationContext.xml");
                System.setProperty("test_db_file.name", testFile);
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "update"});
                ICommandDriver  driver         = SpringContext.getBean(ICommandDriver.class);
                IConfigProvider configProvider = SpringContext.getBean("test");
                Assertions.assertNotNull(configProvider);
                Assertions.assertNotNull(driver);
                CommandResults result  = driver.status("test", configProvider.getMasterChangelogRelativePath());
                int            numRows = (int) result.getResults().get("numRows");
                Assertions.assertEquals(4, numRows);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("===== Test Update Complete====");
    }

    @Test
    public void testRollback() {
        System.out.println("===== Test Rollback====");
        FileDBForTest.deleteFileIfExists(testFile);
        try {
            try (MockedStatic<LiquibaseCommandResultsUtil> liquibaseCommandResultsUtilMock = mockStatic(LiquibaseCommandResultsUtil.class)) {
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.isUpdateSuccessful(Mockito.any(CommandResults.class))).thenReturn(true);
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.doesTagExist(Mockito.any(CommandResults.class))).thenCallRealMethod();
                System.setProperty("applicationContext.files", "liquibaseTestApplicationContext.xml");
                System.setProperty("test_db_file.name", testFile);
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "update"});
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "rollback"});
                ICommandDriver  driver         = SpringContext.getBean(ICommandDriver.class);
                IConfigProvider configProvider = SpringContext.getBean("test");
                Assertions.assertNotNull(configProvider);
                Assertions.assertNotNull(driver);
                CommandResults result  = driver.status("test", configProvider.getMasterChangelogRelativePath());
                int            numRows = (int) result.getResults().get("numRows");
                Assertions.assertEquals(1, numRows);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("===== Test Rollback Complete====");
    }

    @Test
    public void testFinalize() {
        System.out.println("===== Test Finalize ======");
        FileDBForTest.deleteFileIfExists(testFile);
        try {
            try (MockedStatic<LiquibaseCommandResultsUtil> liquibaseCommandResultsUtilMock = mockStatic(LiquibaseCommandResultsUtil.class)) {
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.isUpdateSuccessful(Mockito.any(CommandResults.class))).thenReturn(true);
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.doesTagExist(Mockito.any(CommandResults.class))).thenCallRealMethod();
                System.setProperty("applicationContext.files", "liquibaseTestApplicationContext.xml");
                System.setProperty("test_db_file.name", testFile);
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "update"});
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "finalize"});
                ICommandDriver  driver         = SpringContext.getBean(ICommandDriver.class);
                IConfigProvider configProvider = SpringContext.getBean("test");
                Assertions.assertNotNull(configProvider);
                Assertions.assertNotNull(driver);
                CommandResults result  = driver.status("test", configProvider.getMasterChangelogRelativePath());
                int            numRows = (int) result.getResults().get("numRows");
                Assertions.assertEquals(7, numRows);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("===== Test Finalize complete =====");
    }

    @Test
    public void testFinalizeAfterRollbackShouldFail() {
        System.out.println("===== Test Finalize after Rollback Should Fail======");
        FileDBForTest.deleteFileIfExists(testFile);
        try {
            try (MockedStatic<LiquibaseCommandResultsUtil> liquibaseCommandResultsUtilMock = mockStatic(LiquibaseCommandResultsUtil.class)) {
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.isUpdateSuccessful(Mockito.any(CommandResults.class))).thenReturn(true);
                liquibaseCommandResultsUtilMock.when(() -> LiquibaseCommandResultsUtil.doesTagExist(Mockito.any(CommandResults.class))).thenCallRealMethod();
                System.setProperty("applicationContext.files", "liquibaseTestApplicationContext.xml");
                System.setProperty("test_db_file.name", testFile);
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "update"});
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "rollback"});
                LiquibaseUpdateDriverMain.main(new String[] {"-serviceName", "test", "-tag", "test_3.x", "-op", "finalize"});
                Assertions.fail("Failed: Code flow should not have reached here. Finalize not allowed after rollback");
            }
        } catch (Exception e) {
            // expected
        }
        System.out.println("===== Test Finalize after Rollback Should Fail Complete======");
    }
}
