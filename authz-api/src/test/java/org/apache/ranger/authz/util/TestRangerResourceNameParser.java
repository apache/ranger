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

package org.apache.ranger.authz.util;

import org.apache.ranger.authz.api.RangerAuthzErrorCode;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_EMPTY_VALUE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_TYPE_NOT_VALID;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_RESOURCE_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

public class TestRangerResourceNameParser {
    @Test
    public void testValidTemplates() throws Exception {
        Object[][] testData = {
                {"database", "database", 1, "database"},
                {"database/table", "table", 2, "database", "table"},
                {"database/table/column", "column", 3, "database", "table", "column"},
                {"bucket", "bucket", 1, "bucket"},
                {"bucket/path", "path", 2, "bucket", "path"},
                {"storageaccount", "storageaccount", 1, "storageaccount"},
                {"storageaccount/container", "container", 2, "storageaccount", "container"},
                {"storageaccount/container/relativepath", "relativepath", 3, "storageaccount", "container", "relativepath"},
                {"catalog", "catalog", 1, "catalog"},
                {"catalog/schema", "schema", 2, "catalog", "schema"},
                {"catalog/schema/table", "table", 3, "catalog", "schema", "table"},
                {"catalog/schema/table/column", "column", 4, "catalog", "schema", "table", "column"},
                {"catalog/schema/procedure", "procedure", 3, "catalog", "schema", "procedure"},
                {"catalog/schema/schemafunction", "schemafunction", 3, "catalog", "schema", "schemafunction"},
                {"catalog/sessionproperty", "sessionproperty", 2, "catalog", "sessionproperty"},
        };

        for (Object[] test : testData) {
            String                   template         = (String) test[0];
            String                   resourceType     = (String) test[1];
            int                      resourceCount    = (Integer) test[2];
            RangerResourceNameParser resourceTemplate = new RangerResourceNameParser(template);

            assertEquals(resourceType, resourceTemplate.getResourceType(), template);
            assertEquals(resourceCount, resourceTemplate.count(), template);
            assertEquals(template, resourceTemplate.getTemplate(), template);

            for (int i = 0; i < resourceCount; i++) {
                assertEquals(test[i + 3], resourceTemplate.resourceAt(i), template + " at " + i);
            }
        }
    }

    @Test
    public void testInvalidTemplates() throws Exception {
        String[] templates = {
                null,
                "",
                "  ",
        };

        for (String template : templates) {
            RangerAuthzException excp = assertThrowsExactly(RangerAuthzException.class, () -> new RangerResourceNameParser(template), template);

            assertEquals(INVALID_RESOURCE_TEMPLATE_EMPTY_VALUE.getCode(), excp.getErrorCode().getCode(), template);
        }
    }

    @Test
    public void testParseHiveResources() throws Exception {
        Map<String, RangerResourceNameParser> templates = getHiveTemplates();

        TestData[] tests = {
                new TestData("database:db1", "database", "db1"),
                new TestData("database:db1/", "database", "db1/"),
                new TestData("table:db1/tbl1", "database", "db1", "table", "tbl1"),
                new TestData("table:db\\/1/tbl1", "database", "db/1", "table", "tbl1"), // escape '/' in database name "db/1"
                new TestData("column:db1/tbl1/col1", "database", "db1", "table", "tbl1", "column", "col1"),
                new TestData("column:db1//col1", "database", "db1", "table", "", "column", "col1"), // empty table name
                new TestData("column:db1//", "database", "db1", "table", "", "column", ""), // empty table and column names
                new TestData("column://", "database", "", "table", "", "column", ""), // empty database, table and column names
                new TestData("udf:db1/myUDF", "database", "db1", "udf", "myUDF"),
                new TestData("url:s3a://mybucket/db1/tbl1", "url", "s3a://mybucket/db1/tbl1"),
                new TestData("hiveservice:server1", "hiveservice", "server1"),
                new TestData("global:*", "global", "*"),
                // invalid values
                new TestData("db1/tbl1/col1", INVALID_RESOURCE_TYPE_NOT_VALID),                     // no resource-type
                new TestData(":db1/tbl1/col1", INVALID_RESOURCE_TYPE_NOT_VALID),                    // empty resource-type
                new TestData("invalidResourceType:db1/tbl1/col1", INVALID_RESOURCE_TYPE_NOT_VALID), // unknown resource-type
                new TestData("database:", INVALID_RESOURCE_EMPTY_VALUE),
                new TestData("table:db1_tbl1", INVALID_RESOURCE_VALUE),
                new TestData("column:db1_tbl1/col1", INVALID_RESOURCE_VALUE),
                new TestData("udf:db1_myUDF", INVALID_RESOURCE_VALUE),
        };

        for (TestData test : tests) {
            if (test.errorCode != null) {
                RangerAuthzException excp = assertThrowsExactly(RangerAuthzException.class, () -> parseToMap(test.resource, templates), test.resource);

                assertEquals(test.errorCode.getCode(), excp.getErrorCode().getCode(), test.resource);
            } else {
                assertEquals(test.expected, parseToMap(test.resource, templates), test.resource);
            }
        }
    }

    @Test
    public void testParseS3Resources() throws Exception {
        Map<String, RangerResourceNameParser> templates = getS3Templates();

        TestData[] tests = {
                new TestData("bucket:mybucket", "bucket", "mybucket"),
                new TestData("path:mybucket/myfolder/myfile.txt", "bucket", "mybucket", "path", "myfolder/myfile.txt"),    // no escape needed for '/' in the last resource
                new TestData("path:mybucket/myfolder\\/myfile.txt", "bucket", "mybucket", "path", "myfolder/myfile.txt"),  // escape in the last resource should be ignored
                new TestData("path:mybucket/\\/myfolder/myfile.txt", "bucket", "mybucket", "path", "/myfolder/myfile.txt"), // escape in the last resource should be ignored
                new TestData("path:mybucket/", "bucket", "mybucket", "path", ""),
                // invalid values
                new TestData("mybucket/myfolder/myfile.txt", INVALID_RESOURCE_TYPE_NOT_VALID),                     // no resource-type
                new TestData(":mybucket/myfolder/myfile.txt", INVALID_RESOURCE_TYPE_NOT_VALID),                    // empty resource-type
                new TestData("invalidResourceType:mybucket/myfolder/myfile.txt", INVALID_RESOURCE_TYPE_NOT_VALID), // unknown resource-type
                new TestData("bucket:", INVALID_RESOURCE_EMPTY_VALUE),
                new TestData("path:mybucket_myfolder_myfile.txt", INVALID_RESOURCE_VALUE),
        };

        for (TestData test : tests) {
            if (test.errorCode != null) {
                RangerAuthzException excp = assertThrowsExactly(RangerAuthzException.class, () -> parseToMap(test.resource, templates), test.resource);

                assertEquals(test.errorCode.getCode(), excp.getErrorCode().getCode(), test.resource);
            } else {
                assertEquals(test.expected, parseToMap(test.resource, templates), test.resource);
            }
        }
    }

    @Test
    public void testParseAdlsGen2Resources() throws Exception {
        Map<String, RangerResourceNameParser> templates = getAdlsGen2Templates();

        TestData[] tests = {
                new TestData("container:myaccount/mycontainer", "storageaccount", "myaccount", "container", "mycontainer"),
                new TestData("relativepath:myaccount/mycontainer/p1/p2/f1.txt", "storageaccount", "myaccount", "container", "mycontainer", "relativepath", "p1/p2/f1.txt"),
        };

        for (TestData test : tests) {
            if (test.errorCode != null) {
                RangerAuthzException excp = assertThrowsExactly(RangerAuthzException.class, () -> parseToMap(test.resource, templates), test.resource);

                assertEquals(test.errorCode.getCode(), excp.getErrorCode().getCode(), test.resource);
            } else {
                assertEquals(test.expected, parseToMap(test.resource, templates), test.resource);
            }
        }
    }

    @Test
    public void testParseTrinoResources() throws Exception {
        Map<String, RangerResourceNameParser> templates = getTrinoTemplates();

        TestData[] tests = {
                new TestData("catalog:mycatalog", "catalog", "mycatalog"),
                new TestData("schema:mycatalog/myschema", "catalog", "mycatalog", "schema", "myschema"),
                new TestData("table:mycatalog/myschema/mytable", "catalog", "mycatalog", "schema", "myschema", "table", "mytable"),
                new TestData("column:mycatalog/myschema/mytable/mycolumn", "catalog", "mycatalog", "schema", "myschema", "table", "mytable", "column", "mycolumn"),
                new TestData("trinouser:myuser", "trinouser", "myuser"),
                new TestData("systemproperty:myproperty", "systemproperty", "myproperty"),
                new TestData("sessionproperty:mycatalog/mysessionproperty", "catalog", "mycatalog", "sessionproperty", "mysessionproperty"),
                new TestData("function:myfunction", "function", "myfunction"),
                new TestData("procedure:mycatalog/myschema/myprocedure", "catalog", "mycatalog", "schema", "myschema", "procedure", "myprocedure"),
                new TestData("schemafunction:mycatalog/myschema/myschemafunction", "catalog", "mycatalog", "schema", "myschema", "schemafunction", "myschemafunction"),
                new TestData("queryid:12345-67890-abcdefg-hijklmnopqrs-tuvwxyz", "queryid", "12345-67890-abcdefg-hijklmnopqrs-tuvwxyz"),
                new TestData("sysinfo:systeminfo", "sysinfo", "systeminfo"),
                new TestData("role:myrole", "role", "myrole"),
                // invalid values
                new TestData("mycatalog/myschema/mytable/mycolumn", INVALID_RESOURCE_TYPE_NOT_VALID),                     // no resource-type
                new TestData(":mycatalog/myschema/mytable/mycolumn", INVALID_RESOURCE_TYPE_NOT_VALID),                    // empty resource-type
                new TestData("invalidResourceType:mycatalog/myschema/mytable/mycolumn", INVALID_RESOURCE_TYPE_NOT_VALID), // unknown resource-type
                new TestData("catalog:", INVALID_RESOURCE_EMPTY_VALUE),
                new TestData("schema:mycatalog_myschema", INVALID_RESOURCE_VALUE),
                new TestData("table:mycatalog_myschema_mytable", INVALID_RESOURCE_VALUE),
                new TestData("column:mycatalog_myschema_mytable_mycolumn", INVALID_RESOURCE_VALUE),
                new TestData("trinouser:", INVALID_RESOURCE_EMPTY_VALUE),
                new TestData("sessionproperty:mycatalog_mysessionproperty", INVALID_RESOURCE_VALUE),
                new TestData("procedure:mycatalog_myschema_myprocedure", INVALID_RESOURCE_VALUE),
        };

        for (TestData test : tests) {
            if (test.errorCode != null) {
                RangerAuthzException excp = assertThrowsExactly(RangerAuthzException.class, () -> parseToMap(test.resource, templates), test.resource);

                assertEquals(test.errorCode.getCode(), excp.getErrorCode().getCode(), test.resource);
            } else {
                assertEquals(test.expected, parseToMap(test.resource, templates), test.resource);
            }
        }
    }

    @Test
    public void testResourceNameFromMapHive() throws Exception {
        Map<String, RangerResourceNameParser> templates = getHiveTemplates();

        assertEquals("db1", templates.get("database").toResourceName(toMap("database", "db1")));
        assertEquals("db1/tbl1", templates.get("table").toResourceName(toMap("database", "db1", "table", "tbl1")));
        assertEquals("db1/tbl1/col1", templates.get("column").toResourceName(toMap("database", "db1", "table", "tbl1", "column", "col1")));
        assertEquals("db1/myUDF", templates.get("udf").toResourceName(toMap("database", "db1", "udf", "myUDF")));
        assertEquals("s3a://mybucket/db1/tbl1", templates.get("url").toResourceName(toMap("url", "s3a://mybucket/db1/tbl1")));
        assertEquals("server1", templates.get("hiveservice").toResourceName(toMap("hiveservice", "server1")));
        assertEquals("*", templates.get("global").toResourceName(toMap("global", "*")));
        assertEquals("db1/tbl\\/1/col1", templates.get("column").toResourceName(toMap("database", "db1", "table", "tbl/1", "column", "col1")));
        assertEquals("db1/tbl\\/1/col/1", templates.get("column").toResourceName(toMap("database", "db1", "table", "tbl/1", "column", "col/1")));

        // validate missing entries in map
        assertEquals("", templates.get("database").toResourceName((Map<String, String>) null));
        assertEquals("", templates.get("database").toResourceName(Collections.emptyMap()));
        assertEquals("/", templates.get("table").toResourceName((Map<String, String>) null));
        assertEquals("/", templates.get("table").toResourceName(Collections.emptyMap()));
        assertEquals("/tbl1", templates.get("table").toResourceName(toMap("table", "tbl1"))); // missing database name
        assertEquals("db1/", templates.get("table").toResourceName(toMap("database", "db1"))); // missing table name
        assertEquals("//", templates.get("column").toResourceName((Map<String, String>) null)); // null map
        assertEquals("//", templates.get("column").toResourceName(Collections.emptyMap())); // empty map
        assertEquals("/tbl1/col1", templates.get("column").toResourceName(toMap("table", "tbl1", "column", "col1"))); // missing database name
        assertEquals("db1//col1", templates.get("column").toResourceName(toMap("database", "db1", "column", "col1"))); // missing table name
        assertEquals("db1/tbl1/", templates.get("column").toResourceName(toMap("database", "db1", "table", "tbl1"))); // missing column name
        assertEquals("//", templates.get("column").toResourceName(toMap("database", null, "table", null))); // all names  null or missing

        // validate unknown entries in map
        assertEquals("db1", templates.get("database").toResourceName(toMap("database", "db1", "unknown", "ignore")));
    }

    @Test
    public void testResourceNameFromMapS3() throws Exception {
        Map<String, RangerResourceNameParser> templates = getS3Templates();

        assertEquals("mybucket", templates.get("bucket").toResourceName(toMap("bucket", "mybucket")));
        assertEquals("mybucket/myfolder/myfile.txt", templates.get("path").toResourceName(toMap("bucket", "mybucket", "path", "myfolder/myfile.txt")));
        assertEquals("mybucket//myfolder/myfile.txt", templates.get("path").toResourceName(toMap("bucket", "mybucket", "path", "/myfolder/myfile.txt")));
    }

    @Test
    public void testResourceNameFromArrayHive() throws Exception {
        Map<String, RangerResourceNameParser> templates = getHiveTemplates();

        assertEquals("db1", templates.get("database").toResourceName(toArray("db1")));
        assertEquals("db1/tbl1", templates.get("table").toResourceName(toArray("db1", "tbl1")));
        assertEquals("db1/tbl1/col1", templates.get("column").toResourceName(toArray("db1", "tbl1", "col1")));
        assertEquals("db1/myUDF", templates.get("udf").toResourceName(toArray("db1", "myUDF")));
        assertEquals("s3a://mybucket/db1/tbl1", templates.get("url").toResourceName(toArray("s3a://mybucket/db1/tbl1")));
        assertEquals("server1", templates.get("hiveservice").toResourceName(toArray("server1")));
        assertEquals("*", templates.get("global").toResourceName(toArray("*")));
        assertEquals("db1/tbl\\/1/col1", templates.get("column").toResourceName(toArray("db1", "tbl/1", "col1")));
        assertEquals("db1/tbl\\/1/col/1", templates.get("column").toResourceName(toArray("db1", "tbl/1", "col/1")));

        // validate null or missing entries in array
        assertEquals("", templates.get("database").toResourceName((Map<String, String>) null));
        assertEquals("", templates.get("database").toResourceName(Collections.emptyMap()));
        assertEquals("/", templates.get("table").toResourceName((Map<String, String>) null));
        assertEquals("/", templates.get("table").toResourceName(Collections.emptyMap()));
        assertEquals("/tbl1", templates.get("table").toResourceName(toArray(null, "tbl1"))); // null database name
        assertEquals("db1/", templates.get("table").toResourceName(toArray("db1"))); // missing table name
        assertEquals("//", templates.get("column").toResourceName((String[]) null)); // null array
        assertEquals("//", templates.get("column").toResourceName(new String[0])); // empty array
        assertEquals("/tbl1/col1", templates.get("column").toResourceName(toArray(null, "tbl1", "col1"))); // null database name
        assertEquals("db1//col1", templates.get("column").toResourceName(toArray("db1", null, "col1"))); // null table name
        assertEquals("db1/tbl1/", templates.get("column").toResourceName(toArray("db1", "tbl1", null))); // null column name
        assertEquals("db1/tbl1/", templates.get("column").toResourceName(toArray("db1", "tbl1"))); // missing column name

        // validate unknown entries in map
        assertEquals("db1", templates.get("database").toResourceName(toArray("db1", "ignore")));
    }

    @Test
    public void testResourceNameFromArrayS3() throws Exception {
        Map<String, RangerResourceNameParser> templates = getS3Templates();

        assertEquals("mybucket", templates.get("bucket").toResourceName(toArray("mybucket")));
        assertEquals("mybucket/myfolder/myfile.txt", templates.get("path").toResourceName(toArray("mybucket", "myfolder/myfile.txt")));
        assertEquals("mybucket//myfolder/myfile.txt", templates.get("path").toResourceName(toArray("mybucket", "/myfolder/myfile.txt")));
    }

    private static Map<String, RangerResourceNameParser> getHiveTemplates() throws Exception {
        Map<String, RangerResourceNameParser> ret = new HashMap<>();

        ret.put("database", new RangerResourceNameParser("database"));
        ret.put("table", new RangerResourceNameParser("database/table"));
        ret.put("column", new RangerResourceNameParser("database/table/column"));
        ret.put("udf", new RangerResourceNameParser("database/udf"));
        ret.put("url", new RangerResourceNameParser("url"));
        ret.put("hiveservice", new RangerResourceNameParser("hiveservice"));
        ret.put("global", new RangerResourceNameParser("global"));

        return ret;
    }

    private static Map<String, RangerResourceNameParser> getS3Templates() throws Exception {
        Map<String, RangerResourceNameParser> ret = new HashMap<>();

        ret.put("bucket", new RangerResourceNameParser("bucket"));
        ret.put("path", new RangerResourceNameParser("bucket/path"));

        return ret;
    }

    private static Map<String, RangerResourceNameParser> getAdlsGen2Templates() throws Exception {
        Map<String, RangerResourceNameParser> ret = new HashMap<>();

        ret.put("container", new RangerResourceNameParser("storageaccount/container"));
        ret.put("relativepath", new RangerResourceNameParser("storageaccount/container/relativepath"));

        return ret;
    }

    private static Map<String, RangerResourceNameParser> getTrinoTemplates() throws Exception {
        Map<String, RangerResourceNameParser> ret = new HashMap<>();

        ret.put("catalog", new RangerResourceNameParser("catalog"));
        ret.put("schema", new RangerResourceNameParser("catalog/schema"));
        ret.put("table", new RangerResourceNameParser("catalog/schema/table"));
        ret.put("column", new RangerResourceNameParser("catalog/schema/table/column"));
        ret.put("trinouser", new RangerResourceNameParser("trinouser"));
        ret.put("systemproperty", new RangerResourceNameParser("systemproperty"));
        ret.put("sessionproperty", new RangerResourceNameParser("catalog/sessionproperty"));
        ret.put("function", new RangerResourceNameParser("function"));
        ret.put("procedure", new RangerResourceNameParser("catalog/schema/procedure"));
        ret.put("schemafunction", new RangerResourceNameParser("catalog/schema/schemafunction"));
        ret.put("queryid", new RangerResourceNameParser("queryid"));
        ret.put("sysinfo", new RangerResourceNameParser("sysinfo"));
        ret.put("role", new RangerResourceNameParser("role"));

        return ret;
    }

    private Map<String, String> parseToMap(String resource, Map<String, RangerResourceNameParser> templates) throws RangerAuthzException {
        String[]                 resourceParts = resource.split(":", 2);
        String                   resourceType  = resourceParts.length > 0 ? resourceParts[0] : null;
        String                   resourceValue = resourceParts.length > 1 ? resourceParts[1] : null;
        RangerResourceNameParser template      = templates.get(resourceType);

        if (template == null) {
            throw new RangerAuthzException(INVALID_RESOURCE_TYPE_NOT_VALID, resourceType);
        }

        return template.parseToMap(resourceValue);
    }

    private static class TestData {
        public final String               resource;
        public final Map<String, String>  expected;
        public final RangerAuthzErrorCode errorCode;

        public TestData(String resource, String...values) {
            this.resource = resource;

            if (values.length > 1) {
                this.expected  = new HashMap<>();
                this.errorCode = null;

                for (int i = 1; i < values.length; i += 2) {
                    expected.put(values[i - 1], values[i]);
                }
            } else {
                this.expected  = null;
                this.errorCode = null;
            }
        }

        public TestData(String resource, RangerAuthzErrorCode errorCode) {
            this.resource  = resource;
            this.expected  = null;
            this.errorCode = errorCode;
        }
    }

    private static Map<String, String> toMap(String... values) {
        Map<String, String> ret = new HashMap<>();

        for (int i = 1; i < values.length; i += 2) {
            ret.put(values[i - 1], values[i]);
        }

        return ret;
    }

    private static String[] toArray(String... values) {
        return values;
    }
}
