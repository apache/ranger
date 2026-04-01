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

package org.apache.ranger.audit.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class AuditServerUtils {
    private AuditServerUtils() {
    }

    public static boolean waitUntilTopicReady(Admin admin, String topic, Duration totalWait) throws Exception {
        long endTime     = System.nanoTime() + totalWait.toNanos();
        long baseSleepMs = 100L;
        long maxSleepMs  = 2000L;

        while (System.nanoTime() < endTime) {
            try {
                DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(topic));
                TopicDescription     topicDescription     = describeTopicsResult.values().get(topic).get();
                boolean              allHaveLeader        = topicDescription.partitions().stream().allMatch(partitionInfo -> partitionInfo.leader() != null);
                boolean              allHaveISR           = topicDescription.partitions().stream().allMatch(partitionInfo -> !partitionInfo.isr().isEmpty());

                if (allHaveLeader && allHaveISR) {
                    return true;
                }
            } catch (Exception e) {
                // If topic hasn't propagated yet, you'll see UnknownTopicOrPartitionException
                // continue to wait for topic availability
                if (!(rootCause(e) instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }

            // Sleep until the created topic is available for metadata fetch
            baseSleepMs = Math.min(maxSleepMs, baseSleepMs * 2);

            long sleep = baseSleepMs + ThreadLocalRandom.current().nextLong(0, baseSleepMs / 2 + 1);

            Thread.sleep(sleep);
        }

        return false;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable throwable = t;

        while (throwable.getCause() != null) {
            throwable = throwable.getCause();
        }

        return throwable;
    }
}
