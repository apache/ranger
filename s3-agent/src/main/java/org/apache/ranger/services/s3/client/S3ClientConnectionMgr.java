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
package org.apache.ranger.services.s3.client;

import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.services.s3.RangerS3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class S3ClientConnectionMgr extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConnectionMgr.class);

    public S3ClientConnectionMgr(String svcName, Map<String, String> connectionProperties) {
        super(svcName, connectionProperties);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        LOG.debug("==> S3ClientConnectionMgr.connectionTest ServiceName: " + serviceName + "Configs" + configs);
        boolean connectivityStatus = false;
        Map<String, Object> responseData = new HashMap<String, Object>();
        // String bucketName = "odp-ranger-test";
        S3Client s3 = getS3client(configs);
        String bucketName = configs.get(RangerS3Constants.BUCKET_NAME);

        try {
            // Using ListObjectsV2 approach
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .maxKeys(1) // Just need to check if bucket is accessible
                    .build();
            ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

            // If we get here without exception, bucket exists and is accessible
            connectivityStatus = true;
            String successMsg = "ConnectionTest Successful - Bucket '" + bucketName + "' is accessible";
            generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
        } catch (NoSuchBucketException e) {
            String failureMsg = "Bucket '" + bucketName + "' does not exist";
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== S3ClientConnectionMgr.testConnection Error: " + e.getMessage(), e);
        } catch (S3Exception e) {
            String failureMsg = "Unable to connect to S3 using given parameters: " + e.awsErrorDetails().errorMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== S3ClientConnectionMgr.testConnection Error: " + e.getMessage(), e);
        } catch (Exception e) {
            String failureMsg = "Unexpected error during connection test: " + e.getMessage();
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg, null, null, responseData);
            LOG.error("<== S3ClientConnectionMgr.testConnection Error: " + e.getMessage(), e);
        }

            /*ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
            ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);

            if (listBucketsResponse != null) {
                connectivityStatus = true;
                String successMsg = "ConnectionTest Successful";
                generateResponseDataMap(connectivityStatus, successMsg, successMsg,
                        null, null, responseData);
            } else {
                String failureMsg = "Unable to connect to S3 using given parameters.";
                generateResponseDataMap(connectivityStatus, failureMsg, failureMsg,
                        null, null, responseData);
            }
        } catch (S3Exception e) {
            String failureMsg = "Unable to connect to S3 using given parameters.";
            generateResponseDataMap(connectivityStatus, failureMsg, failureMsg,
                    null, null, responseData);
            LOG.error("<== S3ClientConnectionMgr.testConnection Error: " + e.getMessage(),  e);
        } */

        LOG.debug("<== S3ClientConnectionMgr.connectionTest Result : " + responseData);
        return responseData;
    }

    public static S3Client getS3client(Map<String, String> configs) {
        String accessKey = configs.get(RangerS3Constants.ACCESS_KEY);
        String secretKey = configs.get(RangerS3Constants.SECRET_KEY);
        String endPointOCE = configs.get(RangerS3Constants.ENDPOINT);
        String regionstr = configs.get(RangerS3Constants.REGION);
        AwsBasicCredentials awsCreds3 = AwsBasicCredentials.create(accessKey, secretKey);
        Region region = Region.of(regionstr);
        return S3Client.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds3))
                .endpointOverride(URI.create(endPointOCE))
                .build();
    }

    public static IamClient getIamClient(Map<String, String> configs) {
        String accessKey = configs.get(RangerS3Constants.ACCESS_KEY);
        String secretKey = configs.get(RangerS3Constants.SECRET_KEY);
        AwsBasicCredentials awsCreds3 = AwsBasicCredentials.create(accessKey, secretKey);
        return IamClient.builder()
                .region(Region.AWS_GLOBAL)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds3))
                .build();
    }
}
