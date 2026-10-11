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

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.services.s3.RangerS3Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class S3ResourceMgr {
    private static final Logger LOG = LoggerFactory.getLogger(S3ResourceMgr.class);
    public static final String PATH = "path";

    private S3ResourceMgr() {
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws Exception {
        Map<String, Object> ret;
        LOG.debug("==> S3ResourceMgr.connectionTest ServiceName: " + serviceName);

        try {
            ret = S3ClientConnectionMgr.connectionTest(serviceName, configs);
        } catch (HadoopException e) {
            LOG.error("<== S3ResourceMgr.testConnection Error: " + e.getMessage(), e);
            throw e;
        }

        LOG.debug("<== S3ResourceMgr.connectionTest Result : " + ret);
        return ret;
    }

    public static List<String> getS3Resources(String serviceName, Map<String, String> configs, ResourceLookupContext context, long timeLookup) throws Exception {
        List<String> resultList = new ArrayList<>();
        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        final List<String> pathList = new ArrayList<>();

        LOG.debug("==> S3ResourceMgr.getS3Resources ServiceName:{}", serviceName);

        if (resourceMap != null && resource != null && resourceMap.get(PATH) != null && !resourceMap.get(PATH).isEmpty()) {
            pathList.addAll(resourceMap.get(PATH));
        }

        if (serviceName != null && userInput != null && !pathList.contains("*") && !userInput.equals("*")) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("<== S3ResourceMgr.getS3Resources()  UserInput: \"" + userInput + "\" resource : " + resource +
                              " resourceMap: " + resourceMap);
                }
                S3Client s3 = S3ClientConnectionMgr.getS3client(configs);

                if (s3 != null) {
                    final Callable<List<String>> callableObj = new Callable<List<String>>() {
                        @Override
                        public List<String> call() throws Exception {
                            List<String> resultListInner = new ArrayList<>();
                            if (userInput.contains("/")) {
                                String bucketName = userInput.substring(0, userInput.indexOf("/"));
                                String path = userInput.substring(userInput.indexOf("/") + 1);
                                ListObjectsV2Request.Builder listObjectsRequestBuilder = ListObjectsV2Request.builder()
                                        .bucket(bucketName)
                                        .maxKeys(RangerS3Constants.S3_LIST_OBJECTS_MAX_KEYS);
                                if (!path.isEmpty()) {
                                    listObjectsRequestBuilder.prefix(path.replace("*", ""));  // Replace '*' to use only the prefix part
                                }
                                ListObjectsV2Request listObjectsRequest = listObjectsRequestBuilder.build();
                                ListObjectsV2Response listObjectsResponse;
                                do {
                                    listObjectsResponse = s3.listObjectsV2(listObjectsRequest);
                                    for (var s3Object : listObjectsResponse.contents()) {
                                        // Check if we've reached the limit before adding more results
                                        if (resultListInner.size() >= RangerS3Constants.MAX_AUTOCOMPLETE_RESULTS) {
                                            if (LOG.isDebugEnabled()) {
                                                LOG.debug("Reached maximum autocomplete results limit of {}, stopping pagination",
                                                        RangerS3Constants.MAX_AUTOCOMPLETE_RESULTS);
                                            }
                                            break;
                                        }
                                        String key = s3Object.key();
                                        String prefixPath = path.replace("*", "");
                                        if (key.startsWith(prefixPath) && !pathList.contains(prefixPath)) {
                                            resultListInner.add(bucketName + "/" + key);
                                        }
                                    }

                                    // Short-circuit if we've reached the maximum number of results
                                    if (resultListInner.size() >= RangerS3Constants.MAX_AUTOCOMPLETE_RESULTS) {
                                        break;
                                    }

                                    // If the response is truncated, set the next continuation token for the next request
                                    String nextContinuationToken = listObjectsResponse.nextContinuationToken();
                                    if (nextContinuationToken != null) {
                                        listObjectsRequest = listObjectsRequest.toBuilder().continuationToken(nextContinuationToken).build();
                                    }
                                } while (listObjectsResponse.isTruncated());
                            }
                            return resultListInner;
                        }
                    };
                    if (callableObj != null) {
                        synchronized (s3) {
                            resultList = TimedEventUtil.timedTask(callableObj, timeLookup,
                                    TimeUnit.MILLISECONDS);
                        }
                    }
                }
            } catch (S3Exception e) {
                LOG.error("Failed to list objects for bucket: {}", e.getMessage());
            } catch (Exception e) {
                LOG.error("Unable to get S3 resources.", e);
                throw e;
            }
        }

        LOG.info("<== S3ResourceMgr.getS3Resources()");
        return resultList;
    }
}
