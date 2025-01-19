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

package org.apache.ranger.amazon.cloudwatch;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.apache.ranger.audit.destination.AmazonCloudWatchAuditDestination.CONFIG_PREFIX;
import static org.apache.ranger.audit.destination.AmazonCloudWatchAuditDestination.PROP_REGION;

/**
 * This class initializes the CloudWatch client
 */
@Component
public class CloudWatchMgr {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudWatchMgr.class);

    private AWSLogs client;
    private String  regionName;

    public AWSLogs getClient() {
        AWSLogs me = client;

        if (me == null) {
            me = connect();
        }

        return me;
    }

    synchronized AWSLogs connect() {
        AWSLogs me = client;

        if (me == null) {
            synchronized (CloudWatchMgr.class) {
                me = client;

                if (me == null) {
                    try {
                        me     = newClient();
                        client = me;
                    } catch (Throwable t) {
                        LOGGER.error("Can't connect to CloudWatch region:{} ", regionName, t);
                    }
                }
            }
        }

        return me;
    }

    private AWSLogs newClient() {
        regionName = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + PROP_REGION);

        if (StringUtils.isBlank(regionName)) {
            return AWSLogsClientBuilder.standard().build();
        }

        return AWSLogsClientBuilder.standard().withRegion(regionName).build();
    }
}
