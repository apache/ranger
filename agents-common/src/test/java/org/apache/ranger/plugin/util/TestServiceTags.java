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

package org.apache.ranger.plugin.util;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestServiceTags {
    private static final RangerServiceResource[] RESOURCES = {
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db1"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db2"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db3"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db4"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db5"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db6"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db7"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db8"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db9"))),
            new RangerServiceResource("dev_hive", Collections.singletonMap("database", new RangerPolicyResource("db10")))
    };

    @Test
    public void testDedupEmpty() {
        ServiceTags svcTags = new ServiceTags();

        assertEquals(0, svcTags.dedupTags());
    }

    @Test
    public void testDedup_NoDupTagsNoAttr() {
        RangerTag[] tags = {
                new RangerTag("PII", Collections.emptyMap()),
                new RangerTag("PHI", Collections.emptyMap()),
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);

        assertEquals(0, svcTags.dedupTags());
    }

    @Test
    public void testDedup_NoDupTagsWithAttr() {
        RangerTag[] tags = {
                new RangerTag("PII", Collections.emptyMap()),
                new RangerTag("PII", Collections.singletonMap("type", "email")),
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);

        assertEquals(0, svcTags.dedupTags());
    }

    @Test
    public void testDedup_DupTagsNoAttr() {
        RangerTag[] tags = {
                new RangerTag("PII", Collections.emptyMap()),
                new RangerTag("PII", Collections.emptyMap()),
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);

        assertEquals(1, svcTags.dedupTags());
    }

    @Test
    public void testDedup_DupTagsWithAttr() {
        RangerTag[] tags = {
                new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PII", Collections.singletonMap("type", "email")),
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);

        assertEquals(1, svcTags.dedupTags());
    }

    @Test
    public void testDedup_DupTagsWithAttr_MultipleCalls() {
        RangerTag[] tags = {
            new RangerTag("PII", Collections.singletonMap("type", "email")),
            new RangerTag("PII", Collections.singletonMap("type", "email")),
            new RangerTag("PCI", Collections.emptyMap()),
            new RangerTag("PCI", Collections.emptyMap())
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);

        assertEquals(2, svcTags.dedupTags());
        assertEquals(0, svcTags.dedupTags());
    }


    private ServiceTags createServiceTags(RangerTag[] tags, RangerServiceResource[] resources) {
        ServiceTags ret = new ServiceTags();

        // add tags
        for (int i = 0; i < tags.length; i++) {
            RangerTag tag = tags[i];

            tag.setId(i + 1L);
            ret.getTags().put(tag.getId(), tag);
        }

        // add resources
        for (int i = 0; i < resources.length; i++) {
            RangerServiceResource resource = resources[i];

            resource.setId(i + 1L);
            ret.getServiceResources().add(resource);
        }

        //  set resourceToTagIds
        ret.getServiceResources().forEach(resource -> {
            List<Long> tagIds = new ArrayList<>(ret.getTags().keySet()); // add all available tags

            ret.getResourceToTagIds().put(resource.getId(), tagIds);
        });

        return ret;
    }
}
