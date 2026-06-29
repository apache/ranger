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
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.StringUtil;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

@Component
public class OpenSearchUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchUtil.class);

    private static final ObjectMapper MAPPER               = new ObjectMapper();
    private static final String       LUCENE_SPECIAL_CHARS = "+-=&|><!(){}[]^\"~*?:\\/";
    private static final String       DATE_FORMAT_STR      = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final ThreadLocal<DateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STR);
        String           timeZone   = PropertiesUtil.getProperty("xa.elasticSearch.timezone");

        if (timeZone != null) {
            LOG.info("Setting timezone to {}", timeZone);

            try {
                dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            } catch (Throwable t) {
                LOG.error("Error setting timezone. TimeZone = {}", timeZone);
            }
        }

        return dateFormat;
    });

    @Autowired
    StringUtil stringUtil;

    public OpenSearchUtil() {
    }

    public OpenSearchSearchResult searchResources(SearchCriteria searchCriteria, List<SearchField> searchFields, List<SortField> sortFields, RestClient client, String index) throws IOException {
        String body = buildSearchBody(searchCriteria, searchFields, sortFields);

        LOG.debug("OpenSearch query on index [{}]: {}", index, body);

        Request request = new Request("POST", "/" + index + "/_search");

        request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));

        Response                  response  = client.performRequest(request);
        String                    json      = EntityUtils.toString(response.getEntity());
        JsonNode                  root      = MAPPER.readTree(json);
        long                      totalHits = root.at("/hits/total/value").asLong(0);
        JsonNode                  hitsArray = root.at("/hits/hits");
        List<Map<String, Object>> sources   = new ArrayList<>();

        if (hitsArray.isArray()) {
            for (JsonNode hit : hitsArray) {
                JsonNode sourceNode = hit.get("_source");

                if (sourceNode != null) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source = MAPPER.convertValue(sourceNode, Map.class);

                    sources.add(source);
                }
            }
        }

        return new OpenSearchSearchResult(totalHits, sources);
    }

    public List<Map<String, Object>> fetchByIds(RestClient client, String index, List<String> ids) throws IOException {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, Object> mgetBody = new HashMap<>();

        mgetBody.put("ids", ids);

        String   body   = MAPPER.writeValueAsString(mgetBody);
        Request request = new Request("POST", "/" + index + "/_mget");

        request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));

        Response                  response = client.performRequest(request);
        String                    json     = EntityUtils.toString(response.getEntity());
        JsonNode                  root     = MAPPER.readTree(json);
        JsonNode                  docsNode = root.get("docs");
        List<Map<String, Object>> results  = new ArrayList<>();

        if (docsNode != null && docsNode.isArray()) {
            for (JsonNode doc : docsNode) {
                if (doc.has("found") && doc.get("found").asBoolean()) {
                    JsonNode sourceNode = doc.get("_source");

                    if (sourceNode != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> source = MAPPER.convertValue(sourceNode, Map.class);

                        results.add(source);
                    }
                }
            }
        }

        return results;
    }

    String buildSearchBody(SearchCriteria searchCriteria, List<SearchField> searchFields, List<SortField> sortFields) {
        List<Map<String, Object>> mustClauses  = new ArrayList<>();
        Date                     fromDate      = null;
        Date                     toDate        = null;
        String                   dateFieldName = null;

        if (searchCriteria.getParamList() != null) {
            for (SearchField field : searchFields) {
                String clientFieldName = field.getClientFieldName();
                Object paramValue      = searchCriteria.getParamValue(clientFieldName);

                if (paramValue == null || paramValue.toString().isEmpty()) {
                    continue;
                }

                String                  fieldName  = field.getFieldName();
                SearchField.DATA_TYPE   dataType   = field.getDataType();
                SearchField.SEARCH_TYPE searchType = field.getSearchType();

                if (dataType == SearchField.DATA_TYPE.DATE) {
                    if (paramValue instanceof Date) {
                        if (searchType == SearchField.SEARCH_TYPE.GREATER_EQUAL_THAN || searchType == SearchField.SEARCH_TYPE.GREATER_THAN) {
                            fromDate      = (Date) paramValue;
                            dateFieldName = fieldName;
                        } else if (searchType == SearchField.SEARCH_TYPE.LESS_EQUAL_THAN || searchType == SearchField.SEARCH_TYPE.LESS_THAN) {
                            toDate        = (Date) paramValue;
                            dateFieldName = fieldName;
                        }
                    }

                    continue;
                }

                Map<String, Object> clause = buildClause(fieldName, dataType, searchType, paramValue);

                if (clause != null) {
                    mustClauses.add(clause);
                }
            }

            if (fromDate != null || toDate != null) {
                mustClauses.add(buildDateRange(dateFieldName, fromDate, toDate));
            }
        }

        Map<String, Object> query = new LinkedHashMap<>();
        Map<String, Object> bool  = new HashMap<>();

        bool.put("must", mustClauses.isEmpty() ? List.of(Map.of("match_all", Map.of())) : mustClauses);
        query.put("query", Map.of("bool", bool));
        query.put("from", searchCriteria.getStartIndex());
        query.put("size", searchCriteria.getMaxRows());

        String[] sortResolved = resolveSortField(searchCriteria, sortFields);

        if (sortResolved != null) {
            query.put("sort", List.of(Map.of(sortResolved[0], Map.of("order", sortResolved[1]))));
        }

        try {
            return MAPPER.writeValueAsString(query);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize OpenSearch query", e);
        }
    }

    private Map<String, Object> buildClause(String fieldName, SearchField.DATA_TYPE dataType, SearchField.SEARCH_TYPE searchType, Object paramValue) {
        if (fieldName.startsWith("-")) {
            Map<String, Object> inner = buildClause(fieldName.substring(1), dataType, searchType, paramValue);

            if (inner != null) {
                return Map.of("bool", Map.of("must_not", List.of(inner)));
            }
        } else if (paramValue instanceof Collection) {
            Collection<?> valueList = (Collection<?>) paramValue;

            if (!valueList.isEmpty()) {
                String queryString = valueList.stream().map(v -> "(" + escapeLucene(v.toString().trim().toLowerCase()) + ")").collect(Collectors.joining(" OR "));

                return Map.of("query_string", Map.of("query", queryString, "default_field", fieldName));
            }
        } else if (searchType == SearchField.SEARCH_TYPE.PARTIAL) {
            String value = paramValue.toString().trim();

            if (!value.isEmpty()) {
                return Map.of("query_string", Map.of("query", "*" + escapeLucene(value.toLowerCase()) + "*", "default_field", fieldName));
            }
        } else {
            String value = paramValue.toString().trim();

            if (!value.isEmpty()) {
                return Map.of("match_phrase", Map.of(fieldName, escapeLucene(value.toLowerCase())));
            }
        }

        return null;
    }

    private Map<String, Object> buildDateRange(String fieldName, Date fromDate, Date toDate) {
        Map<String, Object> rangeParams = new LinkedHashMap<>();

        rangeParams.put("format", DATE_FORMAT_STR);

        if (fromDate != null) {
            rangeParams.put("gte", DATE_FORMAT.get().format(fromDate));
        }

        if (toDate != null) {
            rangeParams.put("lte", DATE_FORMAT.get().format(toDate));
        }

        return Map.of("range", Map.of(fieldName, rangeParams));
    }

    private String[] resolveSortField(SearchCriteria searchCriteria, List<SortField> sortFields) {
        String sortBy      = searchCriteria.getSortBy();
        String querySortBy = null;

        if (sortBy != null && !sortBy.trim().isEmpty()) {
            sortBy = sortBy.trim();

            for (SortField sortField : sortFields) {
                if (sortBy.equalsIgnoreCase(sortField.getParamName())) {
                    querySortBy = sortField.getFieldName();

                    searchCriteria.setSortBy(sortField.getParamName());

                    break;
                }
            }
        }

        if (querySortBy == null) {
            for (SortField sortField : sortFields) {
                if (sortField.isDefault()) {
                    querySortBy = sortField.getFieldName();

                    searchCriteria.setSortBy(sortField.getParamName());
                    searchCriteria.setSortType(sortField.getDefaultOrder().name());

                    break;
                }
            }
        }

        if (querySortBy != null) {
            String order = "desc".equalsIgnoreCase(searchCriteria.getSortType()) ? "desc" : "asc";

            return new String[] {querySortBy, order};
        }

        return null;
    }

    static String escapeLucene(String value) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (LUCENE_SPECIAL_CHARS.indexOf(c) >= 0) {
                sb.append('\\');
            }

            sb.append(c);
        }

        return sb.toString();
    }

    public static class OpenSearchSearchResult {
        private final long                      totalHits;
        private final List<Map<String, Object>> sources;

        public OpenSearchSearchResult(long totalHits, List<Map<String, Object>> sources) {
            this.totalHits = totalHits;
            this.sources   = sources;
        }

        public long getTotalHits() {
            return totalHits;
        }

        public List<Map<String, Object>> getSources() {
            return sources;
        }
    }
}
