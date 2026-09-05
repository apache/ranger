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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.ranger.plugin.client.BaseClient.generateResponseDataMap;

/**
 * Ranger service plugin for Apache Drill (service / Admin side).
 *
 * <p>Deployed into Ranger Admin. Provides:
 * <ul>
 *   <li>{@link #validateConfig()} - tests connectivity to the Drill cluster by
 *       issuing a probe query through the Drill REST API.</li>
 *   <li>{@link #lookupResource(ResourceLookupContext)} - enumerates
 *       datasource / schema / table / column resources via
 *       {@code INFORMATION_SCHEMA} queries through the REST API, so the Ranger
 *       policy editor can auto-complete resource paths.</li>
 * </ul>
 *
 * <p>Uses Drill's REST API (default port 8047) instead of JDBC so this module
 * can be compiled with the same JDK as Ranger Admin and does not pull in
 * Drill's JDK 21 dependencies.
 *
 * <h3>Lookup semantics</h3>
 * <ul>
 *   <li>userInput empty (null or "")  -> list all under the upper-level constraints;</li>
 *   <li>userInput non-empty -> fuzzy match with {@code LIKE '%kw%'};</li>
 *   <li>any upper-level constraint missing (null or empty) -> return empty list
 *       (do not query); datasource is the only top-level resource;</li>
 * </ul>
 */
public class RangerServiceDrill extends RangerBaseService {

    private static final Logger logger = LoggerFactory.getLogger(RangerServiceDrill.class);

    // Service config keys (must match ranger-servicedef-drill.json)
    private static final String CONFIG_USERNAME    = "username";
    private static final String CONFIG_PASSWORD    = "password";
    private static final String CONFIG_DRILL_URL   = "drill.connection.url";

    // Resource names (must match ranger-servicedef-drill.json, lowercase per Ranger naming rules)
    private static final String RESOURCE_DATASOURCE = "datasource";
    private static final String RESOURCE_SCHEMA     = "schema";
    private static final String RESOURCE_TABLE      = "table";
    private static final String RESOURCE_COLUMN     = "column";

    // HTTP connect / read timeout (milliseconds)
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS    = 10_000;
    private static final int LOOKUP_TIMEOUT_SECONDS = 5;
    private static final int MAX_DATASOURCE = 50;
    private static final int MAX_SCHEMA     = 50;
    private static final int MAX_TABLE_COLUMN = 100;

    // Access type used when injecting the read-only lookup user policy
    private static final String ACCESS_TYPE_SELECT = "select";

    // SQL templates. TABLES / COLUMNS are reserved keywords in Drill SQL and must be backtick-quoted.
    // Runtime values are inserted via String.format("%s"); all user-supplied values are pre-escaped
    // with escapeSql(...) to defuse single quotes.
    private static final String SQL_VALIDATE = "SELECT 1";

    // datasource: distinct first segment of SCHEMA_NAME. Two variants — list-all vs fuzzy match.
    private static final String SQL_LOOKUP_DATASOURCE_ALL =
            "SELECT DISTINCT SPLIT_PART(SCHEMA_NAME, '.', 1) AS DATASOURCE "
          + "FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY 1";
    private static final String SQL_LOOKUP_DATASOURCE_LIKE =
            "SELECT DISTINCT SPLIT_PART(SCHEMA_NAME, '.', 1) AS DATASOURCE "
          + "FROM INFORMATION_SCHEMA.SCHEMATA "
          + "WHERE SPLIT_PART(SCHEMA_NAME, '.', 1) LIKE '%s' ORDER BY 1 '%s'";

    // schema: list schemas under a datasource (LIKE '<ds>%' so single-segment sources like es still hit).
    private static final String SQL_LOOKUP_SCHEMA =
            "SELECT SCHEMA_NAME AS SCHEMA FROM INFORMATION_SCHEMA.SCHEMATA "
          + "WHERE SCHEMA_NAME LIKE '%s' ORDER BY 1 LIMIT %s";

    // table: list tables under a qualified (datasource[.schema]) name.
    // args: %1$s = qualified name, %2$s = NOT IN(...) clause (may be empty), %3$s = LIKE/AND clause (may be empty).
    private static final String SQL_LOOKUP_TABLE =
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.`TABLES` "
          + "WHERE TABLE_SCHEMA = '%1$s'%2$s%3$s ORDER BY 1 LIMIT %4$s";

    // column: list columns under (qualified name, table).
    // args: %1$s = qualified name, %2$s = table, %3$s = NOT IN(...) clause, %4$s = LIKE/AND clause.
    private static final String SQL_LOOKUP_COLUMN =
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.`COLUMNS` "
          + "WHERE TABLE_SCHEMA = '%1$s' AND TABLE_NAME = '%2$s'%3$s%4$s ORDER BY 1 LIMIT %5$s";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
        logger.debug("RangerServiceDrill initialized for service={}",
                service != null ? service.getName() : "null");
    }

    /**
     * Validates the service configuration by testing connectivity to the Drill
     * cluster via a probe query through the Drill REST API.
     * @return a map with {@code connectivityStatus=true} on success
     * @throws HadoopException on any configuration or connectivity failure
     */
    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> responseData = new HashMap<>();
        String responseMsg = "";
        String description = "";
        String response = "";
        String username = getConfig(CONFIG_USERNAME);
        if (isBlank(username)) {
            throw hadoopException("Drill user name is required", CONFIG_USERNAME);
        }
        String baseUrl = getConfig(CONFIG_DRILL_URL);
        if (isBlank(baseUrl)) {
            throw hadoopException("Drill connection URL is required", CONFIG_DRILL_URL);
        }
        String cryptPassword = getConfig(CONFIG_PASSWORD);
        // Password may be stored encrypted (Ranger Admin auto-encrypts the "password" config key);
        // decrypt it, and fall back to the raw value when it is already plaintext.
        String decryptpassword = PasswordUtils.getDecryptPassword(cryptPassword);

        final String normalizedUrl;
        try {
            normalizedUrl = buildBaseUrl(baseUrl);
        } catch (IllegalArgumentException e) {
            throw hadoopException("Invalid drill.connection.url: " + e.getMessage(), CONFIG_DRILL_URL);
        }

        logger.info("Validating Drill service connection to {}", normalizedUrl);
        try {
            response = queryWithTimeout(normalizedUrl, username, decryptpassword, SQL_VALIDATE);
            JsonNode root = MAPPER.readTree(response);
            if (root != null && root.has("rows") && root.get("rows").isArray()) {
                responseMsg = "Drill connection test succeeded";
                description = "Connected to Drill at " + normalizedUrl;
                generateResponseDataMap(true, responseMsg, description,
                        null, null, responseData);
                logger.info("Drill connection validation succeeded for {}", normalizedUrl);
                return responseData;
            }
            throw hadoopException("Unexpected response from Drill: " + truncate(response, 200), null);
        } catch (Exception e) {
            responseMsg = "Drill connection validation failed for url: " + normalizedUrl;
            description = e.getMessage();
            generateResponseDataMap(false, responseMsg, description,
                    null, null, responseData);
        }
        return responseData;
    }

    private static HadoopException hadoopException(String message, String fieldName) {
        HadoopException he = new HadoopException(message);
        he.generateResponseDataMap(false, message, message, null, fieldName);
        return he;
    }

    /**
     * Lists Drill resources for the Ranger policy editor autocomplete.
     *
     * <p>Each resource level is constrained by the already-selected parent
     * resources carried in {@link ResourceLookupContext#getResources()}.
     * when {@code userInput} is empty,Drill lists everything under the constraints.
     * @param context carries the requested resource name, the already-selected
     *               parents, and the optional user input
     * @return a list of matching resource names.
     */
    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        if (context == null) {
            return Collections.emptyList();
        }

        String userInput = context.getUserInput() == null ? "" : context.getUserInput().trim();
        Map<String, List<String>> resources = context.getResources() != null
                ? context.getResources() : Collections.emptyMap();
        String resourceName = context.getResourceName();

        if (isBlank(resourceName)) {
            return Collections.emptyList();
        }

        List<String> dsList = resources.get(RESOURCE_DATASOURCE);
        List<String> schemaList = resources.get(RESOURCE_SCHEMA);
        List<String> tableList  = resources.get(RESOURCE_TABLE);

        try {
            switch (resourceName) {
                case RESOURCE_DATASOURCE:
                    // Top-level resource: no parent constraint required.
                    return lookupDatasource(userInput);

                case RESOURCE_SCHEMA:
                    // D8: missing datasource -> do not query, return empty.
                    if (isEmpty(dsList)) {
                        return Collections.emptyList();
                    }
                    return lookupSchema(dsList, userInput);

                case RESOURCE_TABLE:
                    // D8: missing datasource or schema -> do not query.
                    if (isEmpty(dsList) || isEmpty(schemaList)) {
                        return Collections.emptyList();
                    }
                    return lookupTable(dsList, schemaList, userInput, resources.get(RESOURCE_TABLE));

                case RESOURCE_COLUMN:
                    if (isEmpty(dsList) || isEmpty(schemaList) || isEmpty(tableList)) {
                        return Collections.emptyList();
                    }
                    return lookupColumn(dsList, schemaList, tableList, userInput, resources.get(RESOURCE_COLUMN));

                default:
                    logger.warn("Unknown resource name: {}", resourceName);
                    return Collections.emptyList();
            }
        } catch (Exception e) {
            logger.error("lookupResource failed for resource={}, userInput={}", resourceName, userInput, e);
            throw new RuntimeException("Resource lookup failed for " + resourceName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerServiceDrill.getDefaultRangerPolicies()");
        }
        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        // inject a read-only "select" policy item for the lookup user
        // into every default "all" policy, so the browse account can run lookups
        // but cannot be made a delegate admin.
        for (RangerPolicy defaultPolicy : ret) {
            if (defaultPolicy.getName() != null
                    && defaultPolicy.getName().contains("all")
                    && StringUtils.isNotBlank(lookUpUser)) {
                List<RangerPolicyItemAccess> accessList = new ArrayList<>();
                RangerPolicyItem policyItem = new RangerPolicyItem();
                accessList.add(new RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
                policyItem.setUsers(Collections.singletonList(lookUpUser));
                policyItem.setAccesses(accessList);
                policyItem.setDelegateAdmin(false);

                List<RangerPolicyItem> policyItems = defaultPolicy.getPolicyItems();
                if (policyItems == null || policyItems.isEmpty()) {
                    policyItems = new ArrayList<>();
                }
                policyItems.add(policyItem);
                defaultPolicy.setPolicyItems(policyItems);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerServiceDrill.getDefaultRangerPolicies()");
        }
        return ret;
    }

    /** datasource: distinct storage-plugin names = first segment of SCHEMA_NAME. */
    private List<String> lookupDatasource(String userInput) throws Exception {
        String sql;
        if (userInput.isEmpty()) {
            sql = SQL_LOOKUP_DATASOURCE_ALL;
        } else {
            sql = String.format(SQL_LOOKUP_DATASOURCE_LIKE, "%" + escapeSql(userInput) + "%", MAX_DATASOURCE);
        }
        return executeQuery(sql, "DATASOURCE");
    }

    /**
     * schema: for each selected datasource, query {@code INFORMATION_SCHEMA.SCHEMATA}
     * with {@code LIKE '<ds>%'} (so single-segment sources like {@code es} still hit),
     * then split the SCHEMA_NAME by dots:
     * <ul>
     *   <li>{@code a}      (e.g. {@code es})       -> display {@code a};</li>
     *   <li>{@code a.b}    (e.g. {@code dfs.tmp})  -> display {@code b} (second segment);</li>
     *   <li>{@code a.b.c}  -> display {@code b} (second segment, no truncation);</li>
     *   <li>no match -> empty, never synthesised (fixes the earlier isEmpty-synthesise bug, plan P13).</li>
     * </ul>
     */
    private List<String> lookupSchema(List<String> dsList, String userInput) throws Exception {
        List<String> out = new ArrayList<>();
        String kw = userInput.toLowerCase();
        for (String ds : dsList) {
            String sql = String.format(SQL_LOOKUP_SCHEMA, escapeSql(ds) + "%", MAX_SCHEMA);
            List<String> rows = executeQuery(sql, "SCHEMA");
            for (String schemaName : rows) {
                String[] parts = schemaName.split("\\.");
                String display = (parts.length == 1) ? parts[0] : parts[1];
                if (userInput.isEmpty() || display.toLowerCase().contains(kw)) {
                    out.add(display);
                }
            }
            if (out.size() >= MAX_SCHEMA) {
                break;
            }
        }
        return out;
    }

    /** table: for each (datasource, schema) pair, query under the qualified name, capped at {@link #MAX_TABLE_COLUMN}. */
    private List<String> lookupTable(List<String> dsList, List<String> schemaList,
                                     String userInput, List<String> excluded) throws Exception {
        List<String> out = new ArrayList<>();
        String notIn = (excluded != null && !excluded.isEmpty())
                ? " AND TABLE_NAME NOT IN (" + sqlIn(excluded) + ")" : "";
        String like = userInput.isEmpty() ? "" : " AND TABLE_NAME LIKE '%" + escapeSql(userInput) + "%'";
        for (String ds : dsList) {
            for (String schema : schemaList) {
                String qualified = ds.equals(schema) ? ds : ds + "." + schema;   // es: es == es -> "es"
                String sql = String.format(SQL_LOOKUP_TABLE, escapeSql(qualified), notIn, like, MAX_TABLE_COLUMN);
                for (String t : executeQuery(sql, "TABLE_NAME")) {
                    if (out.size() >= MAX_TABLE_COLUMN) {
                        return out;
                    }
                    out.add(t);
                }
            }
        }
        return out;
    }

    /** column: for each selected table, query under (qualified name, table), cross-table total capped at {@link #MAX_TABLE_COLUMN}. */
    private List<String> lookupColumn(List<String> dsList, List<String> schemaList, List<String> tableList,
                                      String userInput, List<String> excluded) throws Exception {
        List<String> out = new ArrayList<>();
        String notIn = (excluded != null && !excluded.isEmpty())
                ? " AND COLUMN_NAME NOT IN (" + sqlIn(excluded) + ")" : "";
        String like = userInput.isEmpty() ? "" : " AND COLUMN_NAME LIKE '%" + escapeSql(userInput) + "%'";
        for (String ds : dsList) {
            for (String schema : schemaList) {
                String qualified = ds.equals(schema) ? ds : ds + "." + schema;
                for (String tbl : tableList) {
                    String sql = String.format(SQL_LOOKUP_COLUMN,
                            escapeSql(qualified), escapeSql(tbl), notIn, like, MAX_TABLE_COLUMN);
                    for (String c : executeQuery(sql, "COLUMN_NAME")) {
                        if (out.size() >= MAX_TABLE_COLUMN) {
                            return out;
                        }
                        out.add(c);
                    }
                }
            }
        }
        return out;
    }

    // ========================================================================
    // REST API helpers
    // ========================================================================

    /**
     * Executes a SQL query via the Drill REST API and returns the first-column
     * values. The whole round-trip is bounded by {@link #LOOKUP_TIMEOUT_SECONDS}
     * using {@link TimedEventUtil}.
     */
    private List<String> executeQuery(String sql, String columnAlias) throws Exception {
        String baseUrl = buildBaseUrl(getConfig(CONFIG_DRILL_URL));
        String json = queryWithTimeout(baseUrl, getConfig(CONFIG_USERNAME),
                PasswordUtils.getDecryptPassword(getConfig(CONFIG_PASSWORD)), sql);
        return extractFirstColumnValues(json, columnAlias);
    }

    private String queryWithTimeout(String baseUrl, String username, String password, String sql) throws Exception {
        return TimedEventUtil.timedTask(
                () -> executeRaw(baseUrl, username, password, sql),
                LOOKUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Executes a SQL query via the Drill REST API.
     * <p>POSTs to {@code <baseUrl>/query.json} with a JSON body
     * {@code {"queryType":"SQL","query":"<sql>"}} using HTTP Basic auth.</p>
     */
    private static String executeRaw(String baseUrl, String username, String password, String sql)
            throws Exception {
        String endpoint = baseUrl + "/query.json";
        Map<String, String> payload = new HashMap<>();
        payload.put("queryType", "SQL");
        payload.put("query", sql);
        String body = MAPPER.writeValueAsString(payload);

        HttpURLConnection conn = null;
        try {
            URL url = new URL(endpoint);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");

            // HTTP Basic auth
            String creds = username + ":" + (password == null ? "" : password);
            String encoded = Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + encoded);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            String response = readAll(is);

            if (code < 200 || code >= 300) {
                throw new RuntimeException("Drill REST API returned HTTP " + code + ": " + truncate(response, 500));
            }
            return response;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Extracts the values of the named column from a Drill REST API response.
     * <p>Drill returns JSON like:
     * <pre>{@code {"columns":["COL1"],"rows":[{"COL1":"value1"},{"COL1":"value2"}]}}</pre>
     */
    private static List<String> extractFirstColumnValues(String jsonResponse, String columnAlias) throws Exception {
        JsonNode root = MAPPER.readTree(jsonResponse);
        String queryState = root.get("queryState").asText();
        if (root == null || !root.has("rows") || !root.get("rows").isArray() || "FAILED".equals(queryState)) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (JsonNode row : root.get("rows")) {
            if (row.has(columnAlias)) {
                String value = row.get(columnAlias).asText();
                if (value != null && !value.trim().isEmpty()) {
                    result.add(value.trim());
                }
            } else if (row.isObject() && row.size() > 0) {
                // Fallback: first field of the row
                String value = row.elements().next().asText();
                if (value != null && !value.trim().isEmpty()) {
                    result.add(value.trim());
                }
            }
        }
        return result;
    }

    static String buildBaseUrl(String configuredUrl) {
        if (isBlank(configuredUrl)) {
            throw new IllegalArgumentException("drill.connection.url is empty");
        }
        String url = configuredUrl.trim();
        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }

    private String getConfig(String key) {
        Map<String, String> cfg = getConfigs();
        if (cfg == null) {
            return null;
        }
        Object value = cfg.get(key);
        return value == null ? null : value.toString().trim();
    }

    private static String escapeSql(String value) {
        // Only single-quote escaping is applied; '%' / '_' are intentionally
        // allowed as LIKE wildcards. For stricter behaviour add an
        // ESCAPE clause and escape those characters here.
        return value == null ? "" : value.replace("'", "''");
    }

    private static String sqlIn(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("'").append(escapeSql(values.get(i))).append("'");
        }
        return sb.toString();
    }

    private static List<String> cap(List<String> rows, int limit) {
        if (rows.size() <= limit) {
            return rows;
        }
        return new ArrayList<>(rows.subList(0, limit));
    }

    private static boolean isEmpty(List<String> list) {
        return list == null || list.isEmpty();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) {
            return "";
        }
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }

    private static String readAll(InputStream is) throws Exception {
        if (is == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }
}
