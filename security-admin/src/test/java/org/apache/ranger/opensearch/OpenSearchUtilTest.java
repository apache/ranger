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

package org.apache.ranger.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class OpenSearchUtilTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @InjectMocks
    OpenSearchUtil openSearchUtil;

    @Mock
    StringUtil stringUtil;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        when(stringUtil.isEmpty(anyString())).thenAnswer(inv -> {
            String s = inv.getArgument(0);
            return s == null || s.trim().isEmpty();
        });
    }

    @Test
    void buildSearchBody_emptyParams() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(25);
        criteria.setStartIndex(0);

        List<SearchField> searchFields = new ArrayList<>();
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);
        JsonNode          root         = MAPPER.readTree(body);

        assertNotNull(root.get("query"));
        assertEquals(0, root.get("from").asInt());
        assertEquals(25, root.get("size").asInt());
    }

    @Test
    void buildSearchBody_partialStringSearch() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(0);
        criteria.addParam("requestUser", "testuser");

        List<SearchField> searchFields = List.of(new SearchField("requestUser", "reqUser", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL));
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);
        JsonNode          root         = MAPPER.readTree(body);
        JsonNode          mustClauses  = root.at("/query/bool/must");
        String            bodyStr      = body.toLowerCase();

        assertTrue(mustClauses.isArray());
        assertTrue(mustClauses.size() > 0);

        assertTrue(bodyStr.contains("testuser"));
        assertTrue(bodyStr.contains("query_string"));
    }

    @Test
    void buildSearchBody_fullStringMatch() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(0);
        criteria.addParam("accessType", "read");

        List<SearchField> searchFields = List.of(new SearchField("accessType", "access", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);

        assertTrue(body.contains("match_phrase"));
        assertTrue(body.contains("access"));
        assertTrue(body.contains("read"));
    }

    @Test
    void buildSearchBody_dateRange() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(0);
        criteria.addParam("startDate", new Date(1700000000000L));
        criteria.addParam("endDate", new Date(1700100000000L));

        List<SearchField> searchFields = List.of(
                new SearchField("startDate", "evtTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN),
                new SearchField("endDate", "evtTime", SearchField.DATA_TYPE.DATE, SearchField.SEARCH_TYPE.LESS_EQUAL_THAN));
        List<SortField> sortFields = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));

        String body = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);

        assertTrue(body.contains("range"));
        assertTrue(body.contains("evtTime"));
        assertTrue(body.contains("gte"));
        assertTrue(body.contains("lte"));
    }

    @Test
    void buildSearchBody_collectionOrQuery() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(0);
        criteria.addParam("requestUser", Arrays.asList("user1", "user2", "user3"));

        List<SearchField> searchFields = List.of(new SearchField("requestUser", "reqUser", SearchField.DATA_TYPE.STR_LIST, SearchField.SEARCH_TYPE.FULL));
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);

        assertTrue(body.contains("query_string"));
        assertTrue(body.contains("OR"));
        assertTrue(body.contains("user1"));
        assertTrue(body.contains("user2"));
        assertTrue(body.contains("user3"));
    }

    @Test
    void buildSearchBody_negation() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(0);
        criteria.addParam("excludeUser", "serviceuser");

        List<SearchField> searchFields = List.of(new SearchField("excludeUser", "-reqUser", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);

        assertTrue(body.contains("must_not"));
        assertTrue(body.contains("reqUser"));
    }

    @Test
    void buildSearchBody_sorting() throws Exception {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setMaxRows(10);
        criteria.setStartIndex(5);
        criteria.setSortBy("eventTime");
        criteria.setSortType("asc");

        List<SearchField> searchFields = new ArrayList<>();
        List<SortField>   sortFields   = List.of(new SortField("eventTime", "evtTime", true, SortField.SORT_ORDER.DESC));
        String            body         = openSearchUtil.buildSearchBody(criteria, searchFields, sortFields);
        JsonNode          root         = MAPPER.readTree(body);
        JsonNode          sortNode     = root.get("sort");

        assertEquals(5, root.get("from").asInt());
        assertEquals(10, root.get("size").asInt());

        assertNotNull(sortNode);
        assertTrue(sortNode.isArray());
        assertTrue(sortNode.get(0).has("evtTime"));
        assertEquals("asc", sortNode.get(0).at("/evtTime/order").asText());
    }

    @Test
    void escapeLucene_specialCharacters() {
        String input   = "test+value:with*special?chars";
        String escaped = OpenSearchUtil.escapeLucene(input);

        assertTrue(escaped.contains("\\+"));
        assertTrue(escaped.contains("\\:"));
        assertTrue(escaped.contains("\\*"));
        assertTrue(escaped.contains("\\?"));
    }

    @Test
    void escapeLucene_noSpecialChars() {
        String input   = "simplevalue";
        String escaped = OpenSearchUtil.escapeLucene(input);

        assertEquals("simplevalue", escaped);
    }
}
