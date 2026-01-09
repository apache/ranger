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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.util.HashSet;
import java.util.Set;

public class ArgsParserLiquibaseDBUtilsMain {
    @Parameter(names = "-serviceName", description = "The name of the service for which db upgrade is required)")
    public String serviceName;
    @Parameter(names = "-tag", description = "The current version of the cluster (user for driving upgrades/rollbacks/other commands)")
    public String tagName;
    @Parameter(names = "-op", description = "The operation to do on the cluster (e.g. isUpgradeComplete/isFinalizeComplete)", validateWith = ArgsParserLiquibaseDBUtilsMain.SupportedOps.class)
    public String opName;

    public static class SupportedOps implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            Set<String> validOps = new HashSet<String>() {
                {
                    add("isUpgradeComplete");
                    add("isFinalizeComplete");
                    add("releaseLock");
                }
            };
            if (!validOps.contains(value)) {
                throw new ParameterException(value + " is not a supported operation." + " Valid operations are[" + validOps + "]");
            }
        }
    }
}
