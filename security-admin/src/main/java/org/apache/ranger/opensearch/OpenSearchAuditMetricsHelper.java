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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.audit.metrics.AuditMetricsHelper;
import org.apache.ranger.audit.metrics.AuditMetricsHelper.FilterParams;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class OpenSearchAuditMetricsHelper {
    private static final Logger     LOGGER = LoggerFactory.getLogger(OpenSearchAuditMetricsHelper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FACET_REPO     = "per_repo";
    private static final String FACET_AGENT    = "per_agent";
    private static final String FACET_CLIIP    = "per_cliip";
    private static final String FACET_CLUSTER  = "per_cluster";
    private static final int    AGG_SIZE       = 10000;
    private static final String MISSING_BUCKET = "";

    @Autowired
    OpenSearchMgr openSearchMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    AuditMetricsHelper auditMetricsHelper;

    public RangerAuditMetrics getLatestAuditMetrics(String serviceType, String serviceName, String timezone) {
        SearchFilter              filter  = auditMetricsHelper.buildSearchFilter(serviceType, serviceName, null, null, null);
        List<Map<String, Object>> filters = buildMetricsFilters(filter);

        applyLatestRangeFilter(filters, timezone);

        Map<String, Object> queryBody = buildMetricsQueryBody(filters, null);
        JsonNode            response  = runMetricsSearch(queryBody, "latest audit metrics");
        long                count     = response.at("/hits/total/value").asLong(0L);

        return auditMetricsHelper.buildAuditMetrics(serviceType, serviceName, null, null, null, count);
    }

    public List<RangerAuditMetrics> getLatestAuditMetricsList(SearchFilter filter, String timezone) {
        List<Map<String, Object>> filters = buildMetricsFilters(filter);

        applyLatestRangeFilter(filters, timezone);

        Map<String, Object> aggregations = buildAuditMetricsListAggregation();
        Map<String, Object> queryBody    = buildMetricsQueryBody(filters, aggregations);
        JsonNode            response     = runMetricsSearch(queryBody, "audit metrics list");

        return extractAuditMetricsList(response, filter);
    }

    public List<RangerAuditMetricsByDays> getAuditMetricsByDays(int olderThanInDays, SearchFilter filter, String timezone) {
        List<Map<String, Object>> filters = buildMetricsFilters(filter);

        applyDaysRangeFilter(filters, olderThanInDays, timezone);

        Map<String, Object> aggregations = buildDayAggregation(timezone);
        Map<String, Object> queryBody    = buildMetricsQueryBody(filters, aggregations);
        JsonNode            response     = runMetricsSearch(queryBody, "audit metrics by days");

        return extractAuditMetricsByDays(response, filter, timezone);
    }

    public String resolveServiceType(XXService service) {
        return auditMetricsHelper.resolveServiceType(service);
    }

    public List<RangerAuditMetricsByHours> getAuditMetricsByHours(SearchFilter filter, String timezone) {
        List<Map<String, Object>> filters = buildMetricsFilters(filter);

        applyTodayRangeFilter(filters, timezone);

        Map<String, Object> aggregations = buildHourAggregation(timezone);
        Map<String, Object> queryBody    = buildMetricsQueryBody(filters, aggregations);
        JsonNode            response     = runMetricsSearch(queryBody, "audit metrics by hours");

        return extractAuditMetricsByHours(response, filter, timezone);
    }

    private Map<String, Object> buildMetricsQueryBody(List<Map<String, Object>> filters, Map<String, Object> aggregations) {
        Map<String, Object> queryBody = new LinkedHashMap<>();

        queryBody.put("size", 0);

        Map<String, Object> boolQuery = new LinkedHashMap<>();

        boolQuery.put("filter", filters.isEmpty() ? List.of(Map.of("match_all", Map.of())) : filters);
        queryBody.put("query", Map.of("bool", boolQuery));

        if (aggregations != null) {
            queryBody.put("aggs", aggregations);
        }

        return queryBody;
    }

    private List<Map<String, Object>> buildMetricsFilters(SearchFilter filter) {
        List<Map<String, Object>> filters = new ArrayList<>();

        if (filter == null) {
            return filters;
        }

        addMatchFilter(filters, "repo", filter.getParam(SearchFilter.SERVICE_NAME));
        addMatchFilter(filters, "cluster", filter.getParam(SearchFilter.CLUSTER_NAME));
        addMatchFilter(filters, "cliIP", filter.getParam(SearchFilter.CLIENT_IP));
        addMatchFilter(filters, "agent", filter.getParam(SearchFilter.PLUGIN_APP_ID));

        String serviceType = filter.getParam(SearchFilter.SERVICE_TYPE);

        if (StringUtils.isNotBlank(serviceType)) {
            long repoType = auditMetricsHelper.resolveRepoType(serviceType);

            if (repoType == AuditMetricsHelper.MISSING_REPO_TYPE_SENTINEL) {
                filters.add(Map.of("term", Map.of("repoType", -1)));
            } else {
                filters.add(Map.of("term", Map.of("repoType", repoType)));
            }
        }

        return filters;
    }

    private void addMatchFilter(List<Map<String, Object>> filters, String field, String value) {
        if (filters == null || StringUtils.isBlank(value)) {
            return;
        }

        filters.add(Map.of("match_phrase", Map.of(field, value.trim().toLowerCase())));
    }

    private void applyLatestRangeFilter(List<Map<String, Object>> filters, String timezone) {
        applyRangeFilter(filters, "now-1d", "now", timezone);
    }

    private void applyTodayRangeFilter(List<Map<String, Object>> filters, String timezone) {
        applyRangeFilter(filters, "now/d", "now", timezone);
    }

    private void applyDaysRangeFilter(List<Map<String, Object>> filters, int olderThanInDays, String timezone) {
        int daysBack = olderThanInDays - 1;

        applyRangeFilter(filters, "now-" + daysBack + "d/d", "now", timezone);
    }

    private void applyRangeFilter(List<Map<String, Object>> filters, String from, String to, String timezone) {
        if (filters == null) {
            return;
        }

        Map<String, Object> rangeParams = new LinkedHashMap<>();

        rangeParams.put("from", from);
        rangeParams.put("to", to);

        ZoneId zoneId = auditMetricsHelper.parseZoneId(timezone);

        if (zoneId != null) {
            rangeParams.put("time_zone", zoneId.getId());
        }

        filters.add(Map.of("range", Map.of("evtTime", rangeParams)));
    }

    private Map<String, Object> buildAuditMetricsListAggregation() {
        Map<String, Object> perClusterTerms = new LinkedHashMap<>();

        perClusterTerms.put("field", "cluster");
        perClusterTerms.put("size", AGG_SIZE);
        perClusterTerms.put("missing", MISSING_BUCKET);

        Map<String, Object> perCluster = new LinkedHashMap<>();

        perCluster.put("terms", perClusterTerms);

        Map<String, Object> perCliIpAggs = new LinkedHashMap<>();

        perCliIpAggs.put(FACET_CLUSTER, perCluster);

        Map<String, Object> perCliIpTerms = new LinkedHashMap<>();

        perCliIpTerms.put("field", "cliIP");
        perCliIpTerms.put("size", AGG_SIZE);
        perCliIpTerms.put("missing", MISSING_BUCKET);

        Map<String, Object> perCliIp = new LinkedHashMap<>();

        perCliIp.put("terms", perCliIpTerms);
        perCliIp.put("aggs", perCliIpAggs);

        Map<String, Object> perAgentAggs = new LinkedHashMap<>();

        perAgentAggs.put(FACET_CLIIP, perCliIp);

        Map<String, Object> perAgentTerms = new LinkedHashMap<>();

        perAgentTerms.put("field", "agent");
        perAgentTerms.put("size", AGG_SIZE);
        perAgentTerms.put("missing", MISSING_BUCKET);

        Map<String, Object> perAgent = new LinkedHashMap<>();

        perAgent.put("terms", perAgentTerms);
        perAgent.put("aggs", perAgentAggs);

        Map<String, Object> perRepoAggs = new LinkedHashMap<>();

        perRepoAggs.put(FACET_AGENT, perAgent);

        Map<String, Object> perRepoTerms = new LinkedHashMap<>();

        perRepoTerms.put("field", "repo");
        perRepoTerms.put("size", AGG_SIZE);
        perRepoTerms.put("order", Map.of("_count", "desc"));

        Map<String, Object> perRepo = new LinkedHashMap<>();

        perRepo.put("terms", perRepoTerms);
        perRepo.put("aggs", perRepoAggs);

        Map<String, Object> aggregations = new LinkedHashMap<>();

        aggregations.put(FACET_REPO, perRepo);

        return aggregations;
    }

    private Map<String, Object> buildDayAggregation(String timezone) {
        Map<String, Object> dateHistogram = new LinkedHashMap<>();

        dateHistogram.put("field", "evtTime");
        dateHistogram.put("calendar_interval", "1d");

        ZoneId zoneId = auditMetricsHelper.parseZoneId(timezone);

        if (zoneId != null) {
            dateHistogram.put("time_zone", zoneId.getId());
        }

        return Map.of("per_day", Map.of("date_histogram", dateHistogram));
    }

    private Map<String, Object> buildHourAggregation(String timezone) {
        Map<String, Object> dateHistogram = new LinkedHashMap<>();

        dateHistogram.put("field", "evtTime");
        dateHistogram.put("fixed_interval", "1h");

        ZoneId zoneId = auditMetricsHelper.parseZoneId(timezone);

        if (zoneId != null) {
            dateHistogram.put("time_zone", zoneId.getId());
        }

        return Map.of("per_hour", Map.of("date_histogram", dateHistogram));
    }

    private JsonNode runMetricsSearch(Map<String, Object> queryBody, String context) {
        RestClient client = openSearchMgr.getClient();

        if (client == null) {
            LOGGER.warn("OpenSearch client is null, so not running the query.");

            throw restErrorUtil.createRESTException("Error connecting to search engine", MessageEnums.ERROR_SYSTEM);
        }

        JsonNode result;

        try {
            String  body    = MAPPER.writeValueAsString(queryBody);
            Request request = new Request("POST", "/" + openSearchMgr.getIndex() + "/_search");

            request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));

            Response response    = client.performRequest(request);
            int      statusCode  = response.getStatusLine().getStatusCode();

            if (statusCode < 200 || statusCode >= 300) {
                LOGGER.warn("OpenSearch query failed for {} with status {}", context, statusCode);

                throw restErrorUtil.createRESTException("Error querying search engine", MessageEnums.ERROR_SYSTEM);
            }

            String json = EntityUtils.toString(response.getEntity());

            result = MAPPER.readTree(json);
        } catch (IOException e) {
            LOGGER.warn("OpenSearch query failed for {}: {}", context, e.getMessage());

            throw restErrorUtil.createRESTException("Error querying search engine", MessageEnums.ERROR_SYSTEM);
        }

        return result;
    }

    private List<RangerAuditMetrics> extractAuditMetricsList(JsonNode response, SearchFilter filter) {
        JsonNode buckets = response.at("/aggregations/per_repo/buckets");

        if (!buckets.isArray()) {
            return Collections.emptyList();
        }

        String serviceTypeFilter = filter != null ? filter.getParam(SearchFilter.SERVICE_TYPE) : null;
        List<RangerAuditMetrics> metrics = new ArrayList<>();

        for (JsonNode repoBucket : buckets) {
            processRepoBucket(repoBucket, metrics, serviceTypeFilter);
        }

        return metrics;
    }

    private void processRepoBucket(JsonNode repoBucket, List<RangerAuditMetrics> metrics, String serviceTypeFilter) {
        String serviceName = bucketKeyToString(repoBucket);

        if (serviceName == null) {
            return;
        }

        JsonNode agentBuckets = getSubBuckets(repoBucket, FACET_AGENT);

        if (agentBuckets == null) {
            metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, null, null, null, bucketDocCount(repoBucket)));
        } else {
            processAgentBuckets(agentBuckets, metrics, serviceTypeFilter, serviceName);
        }
    }

    private void processAgentBuckets(JsonNode agentBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName) {
        for (JsonNode agentBucket : agentBuckets) {
            String appId = bucketKeyToString(agentBucket);
            JsonNode cliIpBuckets = getSubBuckets(agentBucket, FACET_CLIIP);

            if (cliIpBuckets == null) {
                metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, null, bucketDocCount(agentBucket)));
            } else {
                processCliIpBuckets(cliIpBuckets, metrics, serviceTypeFilter, serviceName, appId);
            }
        }
    }

    private void processCliIpBuckets(JsonNode cliIpBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName, String appId) {
        for (JsonNode cliIpBucket : cliIpBuckets) {
            String clientIP = bucketKeyToString(cliIpBucket);
            JsonNode clusterBuckets = getSubBuckets(cliIpBucket, FACET_CLUSTER);

            if (clusterBuckets == null) {
                metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, clientIP, bucketDocCount(cliIpBucket)));
            } else {
                processClusterBuckets(clusterBuckets, metrics, serviceTypeFilter, serviceName, appId, clientIP);
            }
        }
    }

    private void processClusterBuckets(JsonNode clusterBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName, String appId, String clientIP) {
        for (JsonNode clusterBucket : clusterBuckets) {
            String clusterName = bucketKeyToString(clusterBucket);

            metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, clusterName, clientIP, bucketDocCount(clusterBucket)));
        }
    }

    private JsonNode getSubBuckets(JsonNode parentBucket, String aggName) {
        JsonNode buckets = parentBucket.path(aggName).path("buckets");

        if (!buckets.isArray() || buckets.isEmpty()) {
            return null;
        }

        return buckets;
    }

    private String bucketKeyToString(JsonNode bucket) {
        String ret = null;

        if (bucket != null && bucket.has("key")) {
            ret = bucket.get("key").asText(null);

            if (StringUtils.isBlank(ret)) {
                ret = null;
            }
        }

        return ret;
    }

    private long bucketDocCount(JsonNode bucket) {
        long ret = 0L;

        if (bucket != null && bucket.has("doc_count")) {
            ret = bucket.get("doc_count").asLong();
        }

        return ret;
    }

    private List<RangerAuditMetricsByDays> extractAuditMetricsByDays(JsonNode response, SearchFilter filter, String timezone) {
        List<RangerAuditMetricsByDays> metrics = new ArrayList<>();
        JsonNode                       buckets = response.at("/aggregations/per_day/buckets");

        if (!buckets.isArray()) {
            return Collections.emptyList();
        }

        FilterParams params = auditMetricsHelper.getFilterParams(filter);

        for (JsonNode bucket : buckets) {
            Long auditDate = parseBucketTime(bucket, timezone);

            if (auditDate == null) {
                continue;
            }

            metrics.add(auditMetricsHelper.buildAuditMetricsByDays(params, auditDate, bucket.get("doc_count").asLong()));
        }

        return metrics;
    }

    private List<RangerAuditMetricsByHours> extractAuditMetricsByHours(JsonNode response, SearchFilter filter, String timezone) {
        List<RangerAuditMetricsByHours> metrics = new ArrayList<>();
        JsonNode                        buckets = response.at("/aggregations/per_hour/buckets");

        if (!buckets.isArray()) {
            return Collections.emptyList();
        }

        FilterParams params = auditMetricsHelper.getFilterParams(filter);
        ZoneId         zoneId = auditMetricsHelper.resolveZoneId(timezone);

        for (JsonNode bucket : buckets) {
            Long auditDate = parseBucketTime(bucket, timezone);

            if (auditDate == null) {
                continue;
            }

            ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(auditDate), zoneId);
            int           hour     = dateTime.getHour();

            metrics.add(auditMetricsHelper.buildAuditMetricsByHours(params, hour, bucket.get("doc_count").asLong()));
        }

        return metrics;
    }

    private Long parseBucketTime(JsonNode bucket, String timezone) {
        Long result = null;

        if (bucket != null) {
            String keyAsString = bucket.get("key_as_string").asText(null);

            if (keyAsString == null) {
                keyAsString = bucket.get("key").asText(null);
            }

            if (keyAsString != null) {
                result = auditMetricsHelper.parseAuditDate(keyAsString);
            }
        }

        return result;
    }
}
