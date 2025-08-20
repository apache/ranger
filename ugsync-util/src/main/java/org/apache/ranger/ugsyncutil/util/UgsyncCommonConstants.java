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

package org.apache.ranger.ugsyncutil.util;

public class UgsyncCommonConstants {
    public enum CaseConversion { NONE, TO_LOWER, TO_UPPER }

    public static final String ORIGINAL_NAME = "original_name";
    public static final String FULL_NAME     = "full_name";
    public static final String SYNC_SOURCE   = "sync_source";
    public static final String LDAP_URL      = "ldap_url";

    public static final String UGSYNC_NONE_CASE_CONVERSION_VALUE = "none";
    public static final String UGSYNC_LOWER_CASE_CONVERSION_VALUE = "lower";
    public static final String UGSYNC_UPPER_CASE_CONVERSION_VALUE = "upper";

    public static final String UGSYNC_USERNAME_CASE_CONVERSION_PARAM = "ranger.usersync.ldap.username.caseconversion";
    public static final String DEFAULT_UGSYNC_USERNAME_CASE_CONVERSION_VALUE = UGSYNC_NONE_CASE_CONVERSION_VALUE;

    public static final String UGSYNC_GROUPNAME_CASE_CONVERSION_PARAM = "ranger.usersync.ldap.groupname.caseconversion";
    public static final String DEFAULT_UGSYNC_GROUPNAME_CASE_CONVERSION_VALUE = UGSYNC_NONE_CASE_CONVERSION_VALUE;

    public static final String SYNC_MAPPING_USERNAME = "ranger.usersync.mapping.username.regex";

    public static final String SYNC_MAPPING_GROUPNAME = "ranger.usersync.mapping.groupname.regex";

    public static final String SYNC_MAPPING_USERNAME_HANDLER = "ranger.usersync.mapping.username.handler";
    public static final String DEFAULT_SYNC_MAPPING_USERNAME_HANDLER = "org.apache.ranger.ugsyncutil.transform.RegEx";

    public static final String SYNC_MAPPING_GROUPNAME_HANDLER = "ranger.usersync.mapping.groupname.handler";
    public static final String DEFAULT_SYNC_MAPPING_GROUPNAME_HANDLER = "org.apache.ranger.ugsyncutil.transform.RegEx";

    public static final String SYNC_MAPPING_SEPARATOR = "ranger.usersync.mapping.regex.separator";

    public static final String DEFAULT_MAPPING_SEPARATOR = "/";

    private UgsyncCommonConstants() {
        // to block instantiation
    }

    public static CaseConversion toCaseConversion(String value) {
        if (UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(value)) {
            return CaseConversion.TO_LOWER;
        } else if (UGSYNC_UPPER_CASE_CONVERSION_VALUE.equalsIgnoreCase(value)) {
            return CaseConversion.TO_UPPER;
        } else {
            return CaseConversion.NONE;
        }
    }
}
