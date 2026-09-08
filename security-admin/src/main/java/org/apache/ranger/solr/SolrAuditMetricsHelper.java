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
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SolrAuditMetricsHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrAuditMetricsHelper.class);
    private static final long   MISSING_REPO_TYPE_SENTINEL = -1L;

    private static final String FACET_REPO = "per_repo";
    private static final String FACET_AGENT = "per_agent";
    private static final String FACET_CLUSTER = "per_cluster";
    private static final String FACET_CLIIP = "per_cliip";
    private static final String BUCKET_VAL = "val";
    private static final String FACETS_KEY = "facets";

    private final Map<String, Long> repoTypeByServiceType = new ConcurrentHashMap<>();

    @Autowired
    SolrMgr solrMgr;

    @Autowired
    SolrUtil solrUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RangerDaoManager daoManager;

    public RangerAuditMetrics getLatestAuditMetrics(String serviceType, String serviceName, String timezone) {
        SearchFilter filter = buildSearchFilter(serviceType, serviceName, null, null, null);

        SolrQuery query = buildMetricsQuery();
        applyAuditMetricsFilters(query, filter);
        addLatestMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        QueryResponse response = runMetricsQuery(query, "latest audit metrics");
        long count = response.getResults() != null ? response.getResults().getNumFound() : 0L;
        return buildAuditMetrics(serviceType, serviceName, null, null, null, count);
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
        String ret = null;

        if (service != null && daoManager != null && daoManager.getXXServiceDef() != null) {
            XXServiceDef serviceDef = daoManager.getXXServiceDef().getById(service.getType());
            if (serviceDef != null) {
                ret = serviceDef.getName();
            }
        }

        return ret;
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
        // If olderThanInDays is 7, we go back 6 days from today to include today
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

    private SearchFilter buildSearchFilter(String serviceType, String serviceName, String appId, String clusterName, String clientIP) {
        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.SERVICE_TYPE, serviceType);
        filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
        filter.setParam(SearchFilter.PLUGIN_APP_ID, appId);
        filter.setParam(SearchFilter.CLUSTER_NAME, clusterName);
        filter.setParam(SearchFilter.CLIENT_IP, clientIP);
        return filter;
    }

    private void applyAuditMetricsFilters(SolrQuery query, SearchFilter filter) {
        if (query == null || filter == null) {
            return;
        }

        String serviceName = filter.getParam(SearchFilter.SERVICE_NAME);
        addFilterQuery(query, "repo", serviceName);

        String serviceType = filter.getParam(SearchFilter.SERVICE_TYPE);
        if (StringUtils.isNotBlank(serviceType)) {
            long repoType = resolveRepoType(serviceType);
            if (repoType == MISSING_REPO_TYPE_SENTINEL) {
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
        try {
            ZoneId.of(tz);
            query.set("TZ", tz);
        } catch (DateTimeException e) {
            query.set("TZ", "UTC");
        }
    }

    private long resolveRepoType(String serviceType) {
        if (StringUtils.isBlank(serviceType)) {
            return MISSING_REPO_TYPE_SENTINEL;
        }

        String cacheKey = serviceType.trim().toLowerCase();
        Long cached = repoTypeByServiceType.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        long resolved = MISSING_REPO_TYPE_SENTINEL;
        if (daoManager != null && daoManager.getXXServiceDef() != null) {
            XXServiceDef serviceDef = daoManager.getXXServiceDef().findByName(serviceType);
            if (serviceDef != null && serviceDef.getId() != null) {
                resolved = serviceDef.getId();
            }
        }

        repoTypeByServiceType.put(cacheKey, resolved);
        return resolved;
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
                metrics.add(buildAuditMetrics(serviceTypeFilter, serviceName, null, null, null, bucketCount(repoBucket)));
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
                metrics.add(buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, null, bucketCount(agentBucket)));
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
                metrics.add(buildAuditMetrics(serviceTypeFilter, serviceName, appId, null, clientIP, bucketCount(cliIpBucket)));
            } else {
                processClusterBuckets(clusterBuckets, metrics, serviceTypeFilter, serviceName, appId, clientIP);
            }
        }
    }

    private void processClusterBuckets(List<?> clusterBuckets, List<RangerAuditMetrics> metrics, String serviceTypeFilter, String serviceName, String appId, String clientIP) {
        for (Object clusterBucket : clusterBuckets) {
            String clusterName = bucketValToString(getBucketValue(clusterBucket, BUCKET_VAL));
            metrics.add(buildAuditMetrics(serviceTypeFilter, serviceName, appId, clusterName, clientIP, bucketCount(clusterBucket)));
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

    private RangerAuditMetrics buildAuditMetrics(String serviceType, String serviceName, String appId, String clusterName, String clientIP, long count) {
        RangerAuditMetrics metric = new RangerAuditMetrics();
        metric.setServiceName(serviceName);
        metric.setServiceType(resolveServiceType(serviceType, serviceName));
        metric.setAppId(appId);
        metric.setClusterName(clusterName);
        metric.setClientIP(clientIP);
        metric.setNumberOfAudits(count);

        Long serviceId = resolveServiceId(serviceName);
        if (serviceId != null) {
            metric.setId(serviceId);
        }

        return metric;
    }

    private Long resolveServiceId(String serviceName) {
        Long ret = null;

        if (StringUtils.isNotBlank(serviceName) && daoManager != null && daoManager.getXXService() != null) {
            XXService service = daoManager.getXXService().findByName(serviceName);
            if (service != null) {
                ret = service.getId();
            }
        }

        return ret;
    }

    private String resolveServiceType(String serviceType, String serviceName) {
        String ret = serviceType;

        if (StringUtils.isBlank(ret)) {
            if (StringUtils.isNotBlank(serviceName) && daoManager != null && daoManager.getXXService() != null) {
                XXService service = daoManager.getXXService().findByName(serviceName);
                ret = resolveServiceType(service);
            } else {
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
        String serviceType = filter != null ? filter.getParam(SearchFilter.SERVICE_TYPE) : null;
        String serviceName = filter != null ? filter.getParam(SearchFilter.SERVICE_NAME) : null;
        String appId = filter != null ? filter.getParam(SearchFilter.PLUGIN_APP_ID) : null;
        String clusterName = filter != null ? filter.getParam(SearchFilter.CLUSTER_NAME) : null;
        String clientIP = filter != null ? filter.getParam(SearchFilter.CLIENT_IP) : null;

        for (Object bucketObj : buckets) {
            Object dateValue = getBucketValue(bucketObj, BUCKET_VAL);
            Long auditDate = parseAuditDate(dateValue);
            if (auditDate == null) {
                continue;
            }

            Object countObj = getBucketValue(bucketObj, "count");
            long count = countObj instanceof Number ? ((Number) countObj).longValue() : 0L;

            RangerAuditMetricsByDays metric = new RangerAuditMetricsByDays(serviceType, serviceName, appId, clusterName, clientIP, auditDate, count);
            metrics.add(metric);
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
        String serviceType = filter != null ? filter.getParam(SearchFilter.SERVICE_TYPE) : null;
        String serviceName = filter != null ? filter.getParam(SearchFilter.SERVICE_NAME) : null;
        String appId = filter != null ? filter.getParam(SearchFilter.PLUGIN_APP_ID) : null;
        String clusterName = filter != null ? filter.getParam(SearchFilter.CLUSTER_NAME) : null;
        String clientIP = filter != null ? filter.getParam(SearchFilter.CLIENT_IP) : null;

        for (Object bucketObj : buckets) {
            Object dateValue = getBucketValue(bucketObj, BUCKET_VAL);
            Long auditDate = parseAuditDate(dateValue);
            if (auditDate == null) {
                continue;
            }

            Object countObj = getBucketValue(bucketObj, "count");
            long count = countObj instanceof Number ? ((Number) countObj).longValue() : 0L;

            ZoneId zoneId = resolveZoneId(timezone);
            ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(auditDate), zoneId);
            int hour = dateTime.getHour();

            RangerAuditMetricsByHours metric = new RangerAuditMetricsByHours(serviceType, serviceName, appId, clusterName, clientIP, hour, count);
            metrics.add(metric);
        }

        return metrics;
    }

    private ZoneId resolveZoneId(String timezone) {
        ZoneId ret = ZoneOffset.UTC;

        if (StringUtils.isNotBlank(timezone)) {
            try {
                ret = ZoneId.of(timezone.trim());
            } catch (DateTimeException e) {
                LOGGER.warn("Invalid timezone '{}', using UTC", timezone, e);
            }
        }

        return ret;
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

    private Long parseAuditDate(Object value) {
        Long ret = null;

        if (value instanceof Date) {
            ret = ((Date) value).getTime();
        } else if (value instanceof Number) {
            ret = ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                ret = Instant.parse((String) value).toEpochMilli();
            } catch (DateTimeParseException e) {
                LOGGER.warn("Unable to parse audit metric date value: {}", value, e);
            }
        }

        return ret;
    }
}
