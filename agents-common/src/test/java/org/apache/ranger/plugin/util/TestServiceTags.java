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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testDedupTags_DuplicateTagWithHigherId() {
        // Create ServiceTags with duplicate tags
        RangerTag[] tags = {
                new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PCI", Collections.emptyMap()),
                new RangerTag("PCI", Collections.emptyMap()),
                new RangerTag("PII", Collections.singletonMap("type", "email"))
        };

        ServiceTags svcTags = createServiceTags(tags, RESOURCES);
        assertEquals(5, svcTags.getTags().size());
        // Should remove 3 duplicates (2 PII, 1 PCI)
        assertEquals(3, svcTags.dedupTags());

        // Verify: 2 tags remain (one PII, one PCI)
        assertEquals(2, svcTags.getTags().size());
        // Find retained PII tag ID
        Long piiTagId = svcTags.getTags().entrySet().stream()
                .filter(e -> e.getValue().getType().equals("PII"))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("PII tag not found"));
        RangerTag cachedTag = svcTags.getTags().get(piiTagId);

        // Verify resource mappings
        assertTrue(svcTags.getResourceToTagIds().values().stream()
                .allMatch(tagIds -> tagIds.contains(piiTagId)));

        // Add a new PII tag with higher tag ID
        ServiceTags svcTags1 = new ServiceTags(svcTags);
        svcTags1.getTags().remove(piiTagId);
        RangerTag newTag = new RangerTag("PII", Collections.singletonMap("type", "email"));
        long newTagId = 23L;
        newTag.setId(newTagId);
        svcTags1.getTags().put(newTagId, newTag);
        svcTags1.getResourceToTagIds().get(1L).add(newTagId);

        assertEquals(0, svcTags1.dedupTags());
        assertEquals(2, svcTags1.getTags().size());
        assertEquals(cachedTag, svcTags1.getTags().get(piiTagId));
        assertFalse(svcTags1.getTags().containsKey(newTagId));

        // Verify resource mappings still include piiTagId
        assertTrue(svcTags1.getResourceToTagIds().values().stream()
                .allMatch(tagIds -> tagIds.contains(piiTagId)));

        // Simulate resource deletion
        svcTags1.getResourceToTagIds().remove(0L);
        assertEquals(0, svcTags1.dedupTags());
        assertEquals(cachedTag, svcTags1.getTags().get(piiTagId));
        assertTrue(svcTags1.getResourceToTagIds().get(1L).contains(piiTagId));
    }

    @Test
    public void testDedupTags_HigherIdTagAfterLowerIdRemoval() {
        // Create ServiceTags with one PII tag
        RangerTag[] tags = {new RangerTag("PII", Collections.singletonMap("type", "email"))};
        ServiceTags svcTags = createServiceTags(tags, RESOURCES);
        assertEquals(1, svcTags.getTags().size());

        Long piiTagId = svcTags.getTags().entrySet().stream()
                .filter(e -> e.getValue().getType().equals("PII"))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("PII tag not found"));
        RangerTag cachedTag = svcTags.getTags().get(piiTagId);

        // calling dedupTags() make sure that cachedTags contains PII tag.
        svcTags.dedupTags();
        svcTags.getTags().remove(piiTagId);

        ServiceTags svcTags1 = new ServiceTags(svcTags);
        RangerTag newTag = new RangerTag("PII", Collections.singletonMap("type", "email"));
        long newTagId = 7L; // Higher tagID
        newTag.setId(newTagId);
        svcTags1.getTags().put(newTagId, newTag);
        svcTags1.getResourceToTagIds().get(1L).add(newTagId);

        // Call dedupTags (should fail with buggy code)
        assertEquals(0, svcTags1.dedupTags());

        // With buggy dedupTags(), newTagId (7) is removed, and no PII tag remains
        // With fixed dedupTags(), newTagId is replaced with a valid ID(piiTagId)
        assertEquals(1, svcTags1.getTags().size());
        assertEquals(cachedTag, svcTags1.getTags().get(piiTagId));
        assertFalse(svcTags1.getTags().containsKey(newTagId));
    }

    @Test
    public void testDedupTags_DuplicateResourceToTagIds() {
        RangerTag[] tags = {
                new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PII", Collections.singletonMap("type", "email")),
                new RangerTag("PCI", Collections.emptyMap()),
                new RangerTag("PCI", Collections.emptyMap()),
                new RangerTag("PII", Collections.singletonMap("type", "email"))
        };
        ServiceTags svcTags = createServiceTags(tags, RESOURCES);
        assertEquals(5, svcTags.getTags().size());

        int dedupCount = svcTags.dedupTags();
        assertEquals(3, dedupCount);
        assertEquals(2, svcTags.getTags().size());

        ServiceTags svcTags1 = new ServiceTags(svcTags);
        // Simulate resource1 deletion (like delete table_1)
        svcTags1.getResourceToTagIds().remove(0L);
        // Clear tags to simulate new sync
        svcTags1.getTags().clear();

        RangerTag tag1 = new RangerTag("PII", Collections.singletonMap("type", "email"));
        RangerTag tag2 = new RangerTag("PII", Collections.singletonMap("type", "email"));
        tag1.setId(200L);
        tag2.setId(201L);
        svcTags1.getTags().put(200L, tag1);
        svcTags1.getTags().put(201L, tag2);

        // Set resource mappings with duplicate tag IDs
        svcTags1.getResourceToTagIds().put(0L, new ArrayList<>(Arrays.asList(200L, 200L, 201L)));
        svcTags1.getResourceToTagIds().put(1L, new ArrayList<>(Arrays.asList(200L, 200L, 201L, 201L)));

        dedupCount = svcTags1.dedupTags();
        assertEquals(1, dedupCount);
        assertEquals(1, svcTags1.getTags().size());

        // Verify resource1 has no duplicate tag IDs
        List<Long> resource1Tags = svcTags1.getResourceToTagIds().get(0L);
        Set<Long> uniqueTags = new HashSet<>(resource1Tags);
        assertEquals("Duplicate tag IDs should be removed from resource1", uniqueTags.size(), resource1Tags.size());

        // Verify resource2 has no duplicate tag IDs
        List<Long> resource2Tags = svcTags1.getResourceToTagIds().get(1L);
        uniqueTags = new HashSet<>(resource2Tags);
        assertEquals("Duplicate tag IDs should be removed from resource2", uniqueTags.size(), resource2Tags.size());
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
