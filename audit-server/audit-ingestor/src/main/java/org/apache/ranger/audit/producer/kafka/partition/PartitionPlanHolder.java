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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Hot-path in-memory plan for {@code AuditPartitioner} and the background watcher. */
public class PartitionPlanHolder {
    private static final PartitionPlanHolder INSTANCE = new PartitionPlanHolder();

    private final AtomicReference<PartitionPlan> planRef = new AtomicReference<>();
    private volatile int lastInstalledVersion;

    private PartitionPlanHolder() {
    }

    public static PartitionPlanHolder getInstance() {
        return INSTANCE;
    }

    public PartitionPlan getPlan() {
        return planRef.get();
    }

    public int getLastInstalledVersion() {
        return lastInstalledVersion;
    }

    /** Validates and atomically installs the plan used by the Kafka partitioner. */
    public void install(PartitionPlan plan, Integer kafkaPartitionCount) {
        PartitionPlanValidator.validate(plan, kafkaPartitionCount);
        planRef.set(plan);
        lastInstalledVersion = plan.getVersion();
        AuthToLocalRuleComposer.getInstance().applyForPlan(plan);
    }

    /**
     * Returns allowed short usernames for a service repo from the in-memory registry document.
     * {@code null} when the plan has no {@code services} block, or when the repo is not present
     * in the plan (caller should fall back to static XML).
     * Returns an empty set when the repo is present with an explicit empty allowlist (deny all).
     *
     * <p>Used by {@link ServiceAllowlistResolver} for per-repo POST authorization — not for the global
     * allowlist union ({@link AuthToLocalRuleComposer#collectAllowedUserShortNames}).
     */
    public Set<String> getAllowedUsersForService(String serviceName) {
        PartitionPlan plan = planRef.get();
        if (plan == null || plan.getServices() == null || plan.getServices().isEmpty()) {
            return null;
        }
        ServiceAllowlistEntry entry = plan.getServices().get(serviceName);
        if (entry == null) {
            return null;
        }
        if (entry.getAllowedUsers().isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(entry.getAllowedUsers()));
    }

    /** Clears holder state between unit tests. */
    public void resetForTests() {
        planRef.set(null);
        lastInstalledVersion = 0;
        AuthToLocalRuleComposer.getInstance().resetForTests();
    }
}
