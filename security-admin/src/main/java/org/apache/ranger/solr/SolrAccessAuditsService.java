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
import org.apache.ranger.AccessAuditsService;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.VXAccessAudit;
import org.apache.ranger.view.VXAccessAuditList;
import org.apache.ranger.view.VXLong;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
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

@Service
@Scope("singleton")
public class SolrAccessAuditsService extends AccessAuditsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolrAccessAuditsService.class);
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

    public VXAccessAuditList searchXAccessAudits(SearchCriteria searchCriteria) {
        // Make call to Solr
        SolrClient    solrClient          = solrMgr.getSolrClient();
        final boolean hiveQueryVisibility = PropertiesUtil.getBooleanProperty("ranger.audit.hive.query.visibility", true);

        if (solrClient == null) {
            LOGGER.warn("Solr client is null, so not running the query.");

            throw restErrorUtil.createRESTException("Error connecting to search engine", MessageEnums.ERROR_SYSTEM);
        }

        List<VXAccessAudit> xAccessAuditList = new ArrayList<>();
        Map<String, Object> paramList        = searchCriteria.getParamList();
        Object              eventIdObj       = paramList.get("eventId");

        if (eventIdObj != null) {
            paramList.put("id", eventIdObj.toString());
        }

        updateUserExclusion(paramList);

        QueryResponse    response = solrUtil.searchResources(searchCriteria, searchFields, sortFields, solrClient);
        SolrDocumentList docs     = response.getResults();

        for (SolrDocument doc : docs) {
            VXAccessAudit vXAccessAudit = populateViewBean(doc);

            if (vXAccessAudit != null) {
                if (!hiveQueryVisibility && "hive".equalsIgnoreCase(vXAccessAudit.getServiceType())) {
                    vXAccessAudit.setRequestData(null);
                } else if ("hive".equalsIgnoreCase(vXAccessAudit.getServiceType()) && ("grant".equalsIgnoreCase(vXAccessAudit.getAccessType()) || "revoke".equalsIgnoreCase(vXAccessAudit.getAccessType()))) {
                    try {
                        if (vXAccessAudit.getRequestData() != null) {
                            vXAccessAudit.setRequestData(java.net.URLDecoder.decode(vXAccessAudit.getRequestData(), "UTF-8"));
                        } else {
                            LOGGER.warn("Error in request data of audit from solr. AuditData: {}", vXAccessAudit);
                        }
                    } catch (UnsupportedEncodingException e) {
                        LOGGER.warn("Error while encoding request data");
                    }
                }
            }

            xAccessAuditList.add(vXAccessAudit);
        }

        VXAccessAuditList returnList = new VXAccessAuditList();

        returnList.setPageSize(searchCriteria.getMaxRows());
        returnList.setResultSize(docs.size());
        returnList.setTotalCount((int) docs.getNumFound());
        returnList.setStartIndex((int) docs.getStart());
        returnList.setVXAccessAudits(xAccessAuditList);

        return returnList;
    }

    /**
     * @param searchCriteria
     * @return
     */
    public VXLong getXAccessAuditSearchCount(SearchCriteria searchCriteria) {
        long   count  = 100;
        VXLong vXLong = new VXLong();

        vXLong.setValue(count);

        return vXLong;
    }

    public RangerAuditMetrics getLatestAuditMetrics(String serviceType, String serviceName) {
        return getLatestAuditMetrics(serviceType, serviceName, null);
    }

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

    public RangerAuditMetrics getAuditMetrics(Long serviceId) {
        return getAuditMetrics(serviceId, null);
    }

    public RangerAuditMetrics getAuditMetrics(Long serviceId, String timezone) {
        if (serviceId == null) {
            throw restErrorUtil.createRESTException("AuditMetrics id is required");
        }

        if (daoManager == null || daoManager.getXXService() == null) {
            throw restErrorUtil.createRESTException("Service lookup is not available");
        }

        XXService service = daoManager.getXXService().getById(serviceId);
        if (service == null) {
            throw restErrorUtil.createRESTException("AuditMetrics with Id: " + serviceId + " does not exist");
        }

        String serviceName = service.getName();
        String serviceType = resolveServiceType(service);

        RangerAuditMetrics metric = getLatestAuditMetrics(serviceType, serviceName, timezone);
        metric.setId(serviceId);
        return metric;
    }

    public List<RangerAuditMetrics> getLatestAuditMetricsList(SearchFilter filter) {
        return getLatestAuditMetricsList(filter, null);
    }

    public List<RangerAuditMetrics> getLatestAuditMetricsList(SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditMetricsListFacet());

        applyAuditMetricsFilters(query, filter);
        addLatestMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        return extractAuditMetricsList(runMetricsQuery(query, "audit metrics list"), filter);
    }

    public List<RangerAuditMetricsByDays> getAuditMetricsByDays(int olderThanInDays, SearchFilter filter) {
        return getAuditMetricsByDays(olderThanInDays, filter, null);
    }

    public List<RangerAuditMetricsByDays> getAuditMetricsByDays(int olderThanInDays, SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditAccessMetricsFacet(olderThanInDays));

        applyAuditMetricsFilters(query, filter);
        applyTimezone(query, timezone);

        return extractAuditMetricsByDays(runMetricsQuery(query, "audit metrics by days"), filter);
    }

    public List<RangerAuditMetricsByHours> getAuditMetricsByHours(SearchFilter filter) {
        return getAuditMetricsByHours(filter, null);
    }

    public List<RangerAuditMetricsByHours> getAuditMetricsByHours(SearchFilter filter, String timezone) {
        SolrQuery query = buildMetricsQuery();
        query.set("json.facet", buildAuditMetricsByHourFacet());

        applyAuditMetricsFilters(query, filter);
        addTodayMetricsRangeFilter(query);
        applyTimezone(query, timezone);

        return extractAuditMetricsByHours(runMetricsQuery(query, "audit metrics by hours"), filter, timezone);
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
        filter.setParam(SearchFilter.APP_ID, appId);
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
        addFilterQuery(query, "agent", filter.getParam(SearchFilter.APP_ID));
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

        query.set("TZ", timezone.trim());
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
            if (appId == null) {
                continue;
            }

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
        if (value == null) {
            return null;
        }

        String ret = value.toString();
        return StringUtils.isBlank(ret) ? null : ret;
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
        if (StringUtils.isBlank(serviceName) || daoManager == null || daoManager.getXXService() == null) {
            return null;
        }

        XXService service = daoManager.getXXService().findByName(serviceName);
        return service != null ? service.getId() : null;
    }

    private String resolveServiceType(XXService service) {
        if (service == null || daoManager == null || daoManager.getXXServiceDef() == null) {
            return null;
        }

        XXServiceDef serviceDef = daoManager.getXXServiceDef().getById(service.getType());
        return serviceDef != null ? serviceDef.getName() : null;
    }

    private String resolveServiceType(String serviceType, String serviceName) {
        if (StringUtils.isNotBlank(serviceType)) {
            return serviceType;
        }

        if (StringUtils.isBlank(serviceName) || daoManager == null || daoManager.getXXService() == null) {
            return null;
        }

        XXService service = daoManager.getXXService().findByName(serviceName);
        return resolveServiceType(service);
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
        String appId = filter != null ? filter.getParam(SearchFilter.APP_ID) : null;
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
        String appId = filter != null ? filter.getParam(SearchFilter.APP_ID) : null;
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
        if (StringUtils.isBlank(timezone)) {
            return ZoneOffset.UTC;
        }

        try {
            return ZoneId.of(timezone.trim());
        } catch (DateTimeException e) {
            LOGGER.warn("Invalid timezone '{}', using UTC", timezone, e);
            return ZoneOffset.UTC;
        }
    }

    private List<?> extractBuckets(Object perDayObj) {
        if (perDayObj instanceof NamedList) {
            Object bucketsObj = ((NamedList<?>) perDayObj).get("buckets");
            if (bucketsObj instanceof List) {
                return (List<?>) bucketsObj;
            }
        } else if (perDayObj instanceof Map) {
            Object bucketsObj = ((Map<?, ?>) perDayObj).get("buckets");
            if (bucketsObj instanceof List) {
                return (List<?>) bucketsObj;
            }
        }

        return Collections.emptyList();
    }

    private Object getBucketValue(Object bucketObj, String key) {
        if (bucketObj instanceof NamedList) {
            return ((NamedList<?>) bucketObj).get(key);
        }

        if (bucketObj instanceof Map) {
            return ((Map<?, ?>) bucketObj).get(key);
        }

        return null;
    }

    private Long parseAuditDate(Object value) {
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            try {
                return Instant.parse((String) value).toEpochMilli();
            } catch (DateTimeParseException e) {
                LOGGER.warn("Unable to parse audit metric date value: {}", value, e);
            }
        }

        return null;
    }

    /**
     * @param doc
     * @return
     */
    private VXAccessAudit populateViewBean(SolrDocument doc) {
        LOGGER.debug("doc={}", doc);

        VXAccessAudit accessAudit = new VXAccessAudit();

        Object value = doc.getFieldValue("id");
        if (value != null) {
            // TODO: Converting ID to hashcode for now
            accessAudit.setId((long) value.hashCode());
            accessAudit.setEventId(value.toString());
        }

        value = doc.getFieldValue("cluster");
        if (value != null) {
            accessAudit.setClusterName(value.toString());
        }

        value = doc.getFieldValue("zoneName");
        if (value != null) {
            accessAudit.setZoneName(value.toString());
        }

        value = doc.getFieldValue("agentHost");
        if (value != null) {
            accessAudit.setAgentHost(value.toString());
        }

        value = doc.getFieldValue("policyVersion");
        if (value != null) {
            accessAudit.setPolicyVersion(MiscUtil.toLong(value));
        }

        value = doc.getFieldValue("access");
        if (value != null) {
            accessAudit.setAccessType(value.toString());
        }

        value = doc.getFieldValue("enforcer");
        if (value != null) {
            accessAudit.setAclEnforcer(value.toString());
        }

        value = doc.getFieldValue("agent");
        if (value != null) {
            accessAudit.setAgentId(value.toString());
        }

        value = doc.getFieldValue("repo");
        if (value != null) {
            accessAudit.setRepoName(value.toString());
            XXService xxService = daoManager.getXXService().findByName(accessAudit.getRepoName());

            if (xxService != null) {
                accessAudit.setRepoDisplayName(xxService.getDisplayName());
            }
        }

        value = doc.getFieldValue("sess");
        if (value != null) {
            accessAudit.setSessionId(value.toString());
        }

        value = doc.getFieldValue("reqUser");
        if (value != null) {
            accessAudit.setRequestUser(value.toString());
        }

        value = doc.getFieldValue("reqData");
        if (value != null) {
            accessAudit.setRequestData(value.toString());
        }

        value = doc.getFieldValue("resource");
        if (value != null) {
            accessAudit.setResourcePath(value.toString());
        }

        value = doc.getFieldValue("cliIP");
        if (value != null) {
            accessAudit.setClientIP(value.toString());
        }

        value = doc.getFieldValue("logType");
        //if (value != null) {
        //    TODO: Need to see what logType maps to in UI
        //    accessAudit.setAuditType(solrUtil.toInt(value));
        //}

        value = doc.getFieldValue("result");
        if (value != null) {
            accessAudit.setAccessResult(MiscUtil.toInt(value));
        }

        value = doc.getFieldValue("policy");
        if (value != null) {
            accessAudit.setPolicyId(MiscUtil.toLong(value));
        }

        value = doc.getFieldValue("repoType");
        if (value != null) {
            accessAudit.setRepoType(MiscUtil.toInt(value));

            XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById((long) accessAudit.getRepoType());

            if (xServiceDef != null) {
                accessAudit.setServiceType(xServiceDef.getName());
                accessAudit.setServiceTypeDisplayName(xServiceDef.getDisplayName());
            }
        }

        value = doc.getFieldValue("resType");
        if (value != null) {
            accessAudit.setResourceType(value.toString());
        }

        value = doc.getFieldValue("reason");
        if (value != null) {
            accessAudit.setResultReason(value.toString());
        }

        value = doc.getFieldValue("action");
        if (value != null) {
            accessAudit.setAction(value.toString());
        }

        value = doc.getFieldValue("evtTime");
        if (value != null) {
            accessAudit.setEventTime(MiscUtil.toLocalDate(value));
        }

        value = doc.getFieldValue("seq_num");
        if (value != null) {
            accessAudit.setSequenceNumber(MiscUtil.toLong(value));
        }

        value = doc.getFieldValue("event_count");
        if (value != null) {
            accessAudit.setEventCount(MiscUtil.toLong(value));
        }

        value = doc.getFieldValue("event_dur_ms");
        if (value != null) {
            accessAudit.setEventDuration(MiscUtil.toLong(value));
        }

        value = doc.getFieldValue("tags");
        if (value != null) {
            accessAudit.setTags(value.toString());
        }

        value = doc.getFieldValue("datasets");
        if (value != null) {
            try {
                accessAudit.setDatasets(JsonUtilsV2.nonSerializableObjToJson(value));
            } catch (Exception e) {
                LOGGER.warn("Failed to convert datasets to json", e);
            }
        }

        value = doc.getFieldValue("projects");
        if (value != null) {
            try {
                accessAudit.setProjects(JsonUtilsV2.nonSerializableObjToJson(value));
            } catch (Exception e) {
                LOGGER.warn("Failed to convert projects to json", e);
            }
        }

        value = doc.getFieldValue("datasetIds");
        if (value != null) {
            try {
                accessAudit.setDatasetIds(JsonUtilsV2.nonSerializableObjToJson(value));
            } catch (Exception e) {
                LOGGER.warn("Failed to convert datasetIds to json", e);
            }
        }

        return accessAudit;
    }
}
