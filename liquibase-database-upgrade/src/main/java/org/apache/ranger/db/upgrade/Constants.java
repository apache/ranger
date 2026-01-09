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

public class Constants {
    //Constants for KMS
    public static final String KMS_UPGRADE_CHANGELOG_RELATIVE_PATH  = "kms/db/db.changelog-master-upgrade.xml";
    public static final String KMS_FINALIZE_CHANGELOG_RELATIVE_PATH = "kms/db/db.changelog-master-finalize.xml";
    //Constants for Ranger
    public static final String RANGER_UPGRADE_CHANGELOG_RELATIVE_PATH  = "ranger/db/db.changelog-master-upgrade.xml";
    public static final String RANGER_FINALIZE_CHANGELOG_RELATIVE_PATH = "ranger/db/db.changelog-master-finalize.xml";
    public static final String TAG_POST_UPGRADE_POSTFIX = "_postupdate";
    public static final String TAG_FINALIZE_POSTFIX     = "_finalized";
    public static final int RETURN_CODE_SUCCESS = 1;
    public static final int RETURN_CODE_FAILURE = 0;

    private Constants() {
    }
}
