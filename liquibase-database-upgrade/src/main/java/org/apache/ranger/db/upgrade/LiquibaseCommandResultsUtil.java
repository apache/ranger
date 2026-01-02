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
import liquibase.report.OperationInfo;
import liquibase.report.UpdateReportParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiquibaseCommandResultsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LiquibaseCommandResultsUtil.class);

    private LiquibaseCommandResultsUtil() {
    }

    public static boolean doesTagExist(CommandResults tagExistsResult) {
        boolean res = (Boolean) tagExistsResult.getResult("tagExistsResult");
        LOG.info("doesTagExist({})={}", tagExistsResult, res);
        return res;
    }

    public static boolean isUpdateSuccessful(CommandResults updateResult) {
        /*
        TODO: revisit this
        */
        boolean operationInfoResult = false;
        Object operationInfoObj = updateResult.getResult("operationInfo");
        if (operationInfoObj instanceof OperationInfo) {
            operationInfoResult = ((OperationInfo) operationInfoObj).getOperationOutcome().equals("success");
        }

        boolean updateReportResult = false;
        Object updateReportObj = updateResult.getResult("updateReport");
        if (updateReportObj instanceof UpdateReportParameters) {
            updateReportResult = ((UpdateReportParameters) updateReportObj).getSuccess();
        }
        
        return operationInfoResult || updateReportResult;
    }
}
