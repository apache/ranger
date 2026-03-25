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

import org.apache.hadoop.crypto.key.RangerKeyStoreProvider;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component("kms")
@Lazy
public class KmsConfigProvider implements IConfigProvider {
    @Override
    public String getUrl() {
        return RangerKeyStoreProvider.getDBKSConf().get(RangerKeyStoreProvider.DB_URL);
    }

    @Override
    public String getUsername() {
        return RangerKeyStoreProvider.getDBKSConf().get(RangerKeyStoreProvider.DB_USER);
    }

    @Override
    public String getPassword() {
        return RangerKeyStoreProvider.getDBKSConf().get(RangerKeyStoreProvider.DB_PASSWORD);
    }

    @Override
    public String getDriver() {
        return RangerKeyStoreProvider.getDBKSConf().get(RangerKeyStoreProvider.DB_DRIVER);
    }

    @Override
    public String getMasterChangelogRelativePath() {
        return Constants.KMS_UPGRADE_CHANGELOG_RELATIVE_PATH;
    }

    @Override
    public String getFinalizeChangelogRelativePath() {
        return Constants.KMS_FINALIZE_CHANGELOG_RELATIVE_PATH;
    }
}
