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

package org.apache.ranger.services.s3;

public class RangerS3Constants {
    public static final String S3 = "s3";
    public static final String AWS = "AWS";
    public static final String ALLOW = "Allow";
    public static final String DENY = "Deny";
    public static final String S3_POLICY_LANGUAGE_VERSION = "2012-10-17";
    public static final String USER_NAME = "username";
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";
    public static final String ENDPOINT = "endpoint";
    public static final String REGION = "region";
    public static final String BUCKET_NAME = "bucketname";
    public static final String PATH = "path";
    public static final String S3_RESOURCE_PATH_ARN = "arn:aws:s3:::";
    public static final String S3_AWS_ACCOUNT_URN = "arn:aws:iam::";
    public static final int MAX_AUTOCOMPLETE_RESULTS = 100;
    public static final int S3_LIST_OBJECTS_MAX_KEYS = 500;

    private RangerS3Constants() {
    }
}
