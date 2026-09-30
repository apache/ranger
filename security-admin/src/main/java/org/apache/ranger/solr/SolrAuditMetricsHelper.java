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

package org.apache.ranger.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.metrics.AuditMetricsHelper;
import org.apache.ranger.audit.metrics.AuditMetricsHelper.FilterParams;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class SolrAuditMetricsHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrAuditMetricsHelper.class);

    private static final String FACET_REPO = "per_repo";
    private static final String FACET_AGENT = "per_agent";
    private static final String FACET_CLUSTER = "per_cluster";
    private static final String FACET_CLIIP = "per_cliip";
    private static final String BUCKET_VAL = "val";
    private static final String FACETS_KEY = "facets";

    @Autowired
    SolrMgr solrMgr;

    @Autowired
    SolrUtil solrUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    AuditMetricsHelper auditMetricsHelper;

    public RangerAuditMetrics getLatestAuditMetrics(String serviceType, String serviceName, String timezone) {
        SearchFilter filter = auditMetricsHelper.buildSearchFilter(serviceType, serviceName, null, null, null);

        SolrQuery query = buildMetricsQuery();
        applyAuditMetricsFilters(query, filter);
        addLatestMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        QueryResponse response = runMetricsQuery(query, "latest audit metrics");
        long count = response.getResults() != null ? response.getResults().getNumFound() : 0L;
        return auditMetricsHelper.buildAuditMetrics(serviceType, serviceName, null, null, null, count);
    }

    public List<RangerAuditMetrics> getLatestAuditMetricsList(SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditMetricsListFacet());

        applyAuditMetricsFilters(query, filter);
        addLatestMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        return extractAuditMetricsList(runMetricsQuery(query, "audit metrics list"), filter);
    }

    public List<RangerAuditMetricsByDays> getAuditMetricsByDays(int olderThanInDays, SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditAccessMetricsFacet(olderThanInDays));

        applyAuditMetricsFilters(query, filter);
        applyTimezone(query, timezone);

        return extractAuditMetricsByDays(runMetricsQuery(query, "audit metrics by days"), filter);
    }

    public List<RangerAuditMetricsByHours> getAuditMetricsByHours(SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditMetricsByHourFacet());

        applyAuditMetricsFilters(query, filter);
        addTodayMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        return extractAuditMetricsByHours(runMetricsQuery(query, "audit metrics by hours"), filter, timezone);
    }

    public String resolveServiceType(XXService service) {
        return auditMetricsHelper.resolveServiceType(service);
    }

    private SolrQuery buildMetricsQuery() {
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.setRows(0);
        return query;
    }

    private QueryResponse runMetricsQuery(SolrQuery query, String context) {
        SolrClient solrClient = solrMgr.getSolrClient();
        if (solrClient == null) {
            LOGGER.warn("Solr client is null, so not running the query.");
            throw restErrorUtil.createRESTException("Error connecting to search engine", MessageEnums.ERROR_SYSTEM);
        }

        QueryResponse response;
        try {
            response = solrUtil.runQuery(solrClient, query);
        } catch (Throwable e) {
            LOGGER.error("Error running Solr query for {}.", context, e);
            throw restErrorUtil.createRESTException("Error running Solr query, please check solr configs. " + e.getMessage(), MessageEnums.ERROR_SYSTEM);
        }

        if (response == null || response.getStatus() != 0) {
            LOGGER.error("Error running Solr query for {}. Query = {}, response = {}", context, query, response);
            throw restErrorUtil.createRESTException("Unable to connect to Audit store !!", MessageEnums.ERROR_SYSTEM);
        }

        return response;
    }

    private String buildAuditAccessMetricsFacet(int olderThanInDays) {
        int daysBack = olderThanInDays - 1;

        return String.format("{per_day:{type:range,field:evtTime,start:\"NOW-%dDAYS/DAY\",end:\"NOW\",gap:\"+1DAY\",mincount:1}}", daysBack);
    }

    private String buildAuditMetricsListFacet() {
        return "{per_repo:{type:terms,field:repo,limit:-1,sort:\"count desc\","
                + "facet:{per_agent:{type:terms,field:agent,limit:-1,missing:true,"
                + "facet:{per_cliip:{type:terms,field:cliIP,limit:-1,missing:true,"
                + "facet:{per_cluster:{type:terms,field:cluster,limit:-1,missing:true"
                + "}}}}}}}}";
    }

    private String buildAuditMetricsByHourFacet() {
        return "{per_hour:{type:range,field:evtTime,start:\"NOW/DAY\",end:\"NOW\",gap:\"+1HOUR\"}}";
    }

    private void addLatestMetricsRangeFilter(SolrQuery query) {
        if (query == null) {
            return;
        }

        query.addFilterQuery("evtTime:[NOW-1DAY TO NOW]");
    }

    private void addTodayMetricsRangeFilter(SolrQuery query) {
        if (query == null) {
            return;
        }

        query.addFilterQuery("evtTime:[NOW/DAY TO NOW]");
    }

    private void applyAuditMetricsFilters(SolrQuery query, SearchFilter filter) {
        if (query == null || filter == null) {
            return;
        }

        String serviceName = filter.getParam(SearchFilter.SERVICE_NAME);
        addFilterQuery(query, "repo", serviceName);

        String serviceType = filter.getParam(SearchFilter.SERVICE_TYPE);
        if (StringUtils.isNotBlank(serviceType)) {
            long repoType = auditMetricsHelper.resolveRepoType(serviceType);
            if (repoType == AuditMetricsHelper.MISSING_REPO_TYPE_SENTINEL) {
                query.addFilterQuery("repoType:-1");
            } else {
                query.addFilterQuery("repoType:" + repoType);
            }
        }

        addFilterQuery(query, "cluster", filter.getParam(SearchFilter.CLUSTER_NAME));
        addFilterQuery(query, "cliIP", filter.getParam(SearchFilter.CLIENT_IP));
        addFilterQuery(query, "agent", filter.getParam(SearchFilter.PLUGIN_APP_ID));
    }

    private void addFilterQuery(SolrQuery query, String field, String value) {
        if (query == null || StringUtils.isBlank(value)) {
            return;
        }

        String escapedValue = ClientUtils.escapeQueryChars(value.trim().toLowerCase());
        query.addFilterQuery(field + ":" + escapedValue);
    }

    private void applyTimezone(SolrQuery query, String timezone) {
        if (query == null || StringUtils.isBlank(timezone)) {
            return;
        }

        String tz = timezone.trim();
        ZoneId zoneId = auditMetricsHelper.parseZoneId(tz);

        if (zoneId != null) {
            query.set("TZ", zoneId.getId());
        } else {
            query.set("TZ", "UTC");
        }
    }

    private List<RangerAuditMetrics> extractAuditMetricsList(QueryResponse response, SearchFilter filter) {
        NamedList<Object> responseList = response.getResponse();
        if (responseList == null || !(responseList.get(FACETS_KEY) instanceof NamedList)) {
            return Collections.emptyList();
        }

        NamedList<?> facets = (NamedList<?>) responseList.get(FACETS_KEY);
        List<?> repoBuckets = extractBuckets(facets.get(FACET_REPO));

        if (repoBuckets == null || repoBuckets.isEmpty()) {
            return Collections.emptyList();
        }

        String serviceTypeFilter = filter != null ? filter.getParam(SearchFilter.SERVICE_TYPE) : null;
        List<RangerAuditMetrics> metrics = new ArrayList<>();

        processRepoBuckets(repoBuckets, metrics, serviceTypeFilter);

        return metrics;
    }

    private void processRepoBuckets(List<?> repoBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter) {
        for (Object repoBucket : repoBuckets) {
            Object repoVal = getBucketValue(repoBucket, BUCKET_VAL);
            if (repoVal == null) {
                continue;
            }

            String serviceName = repoVal.toString();
            List<?> agentBuckets = extractBuckets(getBucketValue(repoBucket, FACET_AGENT));

            if (agentBuckets == null || agentBuckets.isEmpty()) {
                metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, null, null, null, bucketCount(repoBucket)));
            } else {
                processAgentBuckets(agentBuckets, metrics, serviceTypeFilter, serviceName);
            }
        }
    }

    private void processAgentBuckets(List<?> agentBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName) {
        for (Object agentBucket : agentBuckets) {
            String appId = bucketValToString(getBucketValue(agentBucket, BUCKET_VAL));

            List<?> cliIpBuckets = extractBuckets(getBucketValue(agentBucket, FACET_CLIIP));

            if (cliIpBuckets == null || cliIpBuckets.isEmpty()) {
                metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, null, bucketCount(agentBucket)));
            } else {
                processCliIpBuckets(cliIpBuckets, metrics, serviceTypeFilter, serviceName, appId);
            }
        }
    }

    private void processCliIpBuckets(List<?> cliIpBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName, String appId) {
        for (Object cliIpBucket : cliIpBuckets) {
            String clientIP = bucketValToString(getBucketValue(cliIpBucket, BUCKET_VAL));
            List<?> clusterBuckets = extractBuckets(getBucketValue(cliIpBucket, FACET_CLUSTER));

            if (clusterBuckets == null || clusterBuckets.isEmpty()) {
                metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, clientIP, bucketCount(cliIpBucket)));
            } else {
                processClusterBuckets(clusterBuckets, metrics, serviceTypeFilter, serviceName, appId, clientIP);
            }
        }
    }

    private void processClusterBuckets(List<?> clusterBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName, String appId, String clientIP) {
        for (Object clusterBucket : clusterBuckets) {
            String clusterName = bucketValToString(getBucketValue(clusterBucket, BUCKET_VAL));
            metrics.add(auditMetricsHelper.buildAuditMetrics(serviceTypeFilter, serviceName, appId, clusterName, clientIP, bucketCount(clusterBucket)));
        }
    }

    private long bucketCount(Object bucketObj) {
        Object countObj = getBucketValue(bucketObj, "count");
        return countObj instanceof Number ? ((Number) countObj).longValue() : 0L;
    }

    private String bucketValToString(Object value) {
        String ret = null;

        if (value != null) {
            ret = value.toString();
            if (StringUtils.isBlank(ret)) {
                ret = null;
            }
        }

        return ret;
    }

    private List<RangerAuditMetricsByDays> extractAuditMetricsByDays(QueryResponse response, SearchFilter filter) {
        NamedList<Object> responseList = response.getResponse();
        if (responseList == null) {
            return Collections.emptyList();
        }

        Object facetsObj = responseList.get(FACETS_KEY);
        if (!(facetsObj instanceof NamedList)) {
            return Collections.emptyList();
        }

        NamedList<?> facets = (NamedList<?>) facetsObj;
        Object perDayObj = facets.get("per_day");
        List<?> buckets = extractBuckets(perDayObj);

        if (buckets == null || buckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<RangerAuditMetricsByDays> metrics = new ArrayList<>();
        FilterParams params = auditMetricsHelper.getFilterParams(filter);

        for (Object bucketObj : buckets) {
            Object dateValue = getBucketValue(bucketObj, BUCKET_VAL);
            Long auditDate = auditMetricsHelper.parseAuditDate(dateValue);
            if (auditDate == null) {
                continue;
            }

            Object countObj = getBucketValue(bucketObj, "count");
            long count = countObj instanceof Number ? ((Number) countObj).longValue() : 0L;

            metrics.add(auditMetricsHelper.buildAuditMetricsByDays(params, auditDate, count));
        }

        return metrics;
    }

    private List<RangerAuditMetricsByHours> extractAuditMetricsByHours(QueryResponse response, SearchFilter filter, String timezone) {
        NamedList<Object> responseList = response.getResponse();
        if (responseList == null) {
            return Collections.emptyList();
        }

        Object facetsObj = responseList.get(FACETS_KEY);
        if (!(facetsObj instanceof NamedList)) {
            return Collections.emptyList();
        }

        NamedList<?> facets = (NamedList<?>) facetsObj;
        Object perHourObj = facets.get("per_hour");
        List<?> buckets = extractBuckets(perHourObj);

        if (buckets == null || buckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<RangerAuditMetricsByHours> metrics = new ArrayList<>();
        FilterParams params = auditMetricsHelper.getFilterParams(filter);
        ZoneId zoneId = auditMetricsHelper.resolveZoneId(timezone);

        for (Object bucketObj : buckets) {
            Object dateValue = getBucketValue(bucketObj, BUCKET_VAL);
            Long auditDate = auditMetricsHelper.parseAuditDate(dateValue);
            if (auditDate == null) {
                continue;
            }

            Object countObj = getBucketValue(bucketObj, "count");
            long count = countObj instanceof Number ? ((Number) countObj).longValue() : 0L;

            ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(auditDate), zoneId);
            int hour = dateTime.getHour();

            metrics.add(auditMetricsHelper.buildAuditMetricsByHours(params, hour, count));
        }

        return metrics;
    }

    private List<?> extractBuckets(Object perDayObj) {
        List<?> ret = Collections.emptyList();

        if (perDayObj instanceof NamedList) {
            Object bucketsObj = ((NamedList<?>) perDayObj).get("buckets");
            if (bucketsObj instanceof List) {
                ret = (List<?>) bucketsObj;
            }
        } else if (perDayObj instanceof Map) {
            Object bucketsObj = ((Map<?, ?>) perDayObj).get("buckets");
            if (bucketsObj instanceof List) {
                ret = (List<?>) bucketsObj;
            }
        }

        return ret;
    }

    private Object getBucketValue(Object bucketObj, String key) {
        Object ret = null;

        if (bucketObj instanceof NamedList) {
            ret = ((NamedList<?>) bucketObj).get(key);
        } else if (bucketObj instanceof Map) {
            ret = ((Map<?, ?>) bucketObj).get(key);
        }

        return ret;
    }
}
