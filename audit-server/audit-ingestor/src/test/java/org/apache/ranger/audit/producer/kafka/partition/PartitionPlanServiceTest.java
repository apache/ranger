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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanServiceTest {
    private static final String TOPIC = "ranger_audits";

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testDynamicDisabledWhenFlagOff() {
        PartitionPlanService service = new PartitionPlanService(disabledConfig(), PartitionPlanHolder.getInstance(), new NoOpPartitionPlanRegistryFactory(), new KafkaAuditTopicPartitionGrower());
        assertFalse(service.isDynamicPartitionPlanEnabled());
    }

    @Test
    public void testDynamicEnabledWhenFlagOn() {
        PartitionPlanService service = new PartitionPlanService(enabledConfig(), PartitionPlanHolder.getInstance(), new NoOpPartitionPlanRegistryFactory(), new KafkaAuditTopicPartitionGrower());
        assertTrue(service.isDynamicPartitionPlanEnabled());
    }

    @Test
    public void testGetFromMemoryReturnsInstalledPlan() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs"}, 3, 3));
        PartitionPlanHolder.getInstance().install(plan, 6);
        PartitionPlanService service = new PartitionPlanService(enabledConfig(), PartitionPlanHolder.getInstance(), new NoOpPartitionPlanRegistryFactory(), new KafkaAuditTopicPartitionGrower());

        PartitionPlan loaded = service.getPartitionPlan();

        assertEquals(plan, loaded);
        assertEquals(plan.toJson(), service.getPartitionPlan().toJson());
    }

    @Test
    public void testGetFromMemoryFailsWhenPlanNotLoaded() {
        PartitionPlanService service = new PartitionPlanService(enabledConfig(), PartitionPlanHolder.getInstance(), new NoOpPartitionPlanRegistryFactory(), new KafkaAuditTopicPartitionGrower());
        assertThrows(PartitionPlanException.class, () -> service.getPartitionPlan());
    }

    private static Properties disabledConfig() {
        Properties props = baseConfig();
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "false");
        return props;
    }

    private static Properties enabledConfig() {
        Properties props = baseConfig();
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "true");
        return props;
    }

    private static Properties baseConfig() {
        Properties props = new Properties();
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_NAME, TOPIC);
        return props;
    }

    private static final class NoOpPartitionPlanRegistryFactory extends PartitionPlanRegistryFactory {
        @Override
        public PartitionPlanRegistry open(Properties props, String propPrefix) {
            return null;
        }
    }
}
