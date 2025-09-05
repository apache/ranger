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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRangerResourceTemplate {
    @Test
    public void testHiveResources() throws Exception {
        Map<String, RangerResourceTemplate> templates = getHiveTemplates();

        TestData[] tests = {
                new TestData("database:db1", "database", "db1"),
                new TestData("table:db1.tbl1", "database", "db1", "table", "tbl1"),
                new TestData("column:db1.tbl1.col1", "database", "db1", "table", "tbl1", "column", "col1"),
                new TestData("udf:db1.myUDF", "database", "db1", "udf", "myUDF"),
                new TestData("url:s3a://mybucket/db1/tbl1", "url", "s3a://mybucket/db1/tbl1"),
                new TestData("hiveservice:server1", "hiveservice", "server1"),
                new TestData("global:*", "global", "*"),
                // invalid values
                new TestData("db1.tbl1.col1"),                     // no resource-type
                new TestData(":db1.tbl1.col1"),                    // empty resource-type
                new TestData("invalidResourceType:db1.tbl1.col1"), // unknown resource-type
                new TestData("database:"),
                new TestData("table:db1_tbl1"),
                new TestData("column:db1_tbl1.col1"),
                new TestData("udf:db1_myUDF"),
        };

        for (TestData test : tests) {
            Map<String, String> actual = getResourceAsMap(test.resource, templates);

            assertEquals(test.expected, actual, test.resource);
        }
    }

    @Test
    public void testS3Resources() throws Exception {
        Map<String, RangerResourceTemplate> templates = getS3Templates();

        TestData[] tests = {
                new TestData("bucket:mybucket", "bucket", "mybucket"),
                new TestData("path:mybucket/myfolder/myfile.txt", "bucket", "mybucket", "path", "myfolder/myfile.txt"),
                new TestData("path:mybucket/", "bucket", "mybucket", "path", ""),
                // invalid values
                new TestData("mybucket/myfolder/myfile.txt"),                     // no resource-type
                new TestData(":mybucket/myfolder/myfile.txt"),                    // empty resource-type
                new TestData("invalidResourceType:mybucket/myfolder/myfile.txt"), // unknown resource-type
                new TestData("bucket:"),
                new TestData("path:mybucket_myfolder_myfile.txt"),
        };

        for (TestData test : tests) {
            Map<String, String> actual = getResourceAsMap(test.resource, templates);

            assertEquals(test.expected, actual, test.resource);
        }
    }

    @Test
    public void testAdlsGen2Resources() throws Exception {
        Map<String, RangerResourceTemplate> templates = getAdlsGen2Templates();

        TestData[] tests = {
                new TestData("container:mycontainer@myaccount", "storageaccount", "myaccount", "container", "mycontainer"),
                new TestData("relativepath:mycontainer@myaccount/p1/p2/f1.txt", "storageaccount", "myaccount", "container", "mycontainer", "relativepath", "p1/p2/f1.txt"),
        };

        for (TestData test : tests) {
            Map<String, String> actual = getResourceAsMap(test.resource, templates);

            assertEquals(test.expected, actual, test.resource);
        }
    }

    @Test
    public void testTrinoResources() throws Exception {
        Map<String, RangerResourceTemplate> templates = getTrinoTemplates();

        TestData[] tests = {
                new TestData("catalog:mycatalog", "catalog", "mycatalog"),
                new TestData("schema:mycatalog.myschema", "catalog", "mycatalog", "schema", "myschema"),
                new TestData("table:mycatalog.myschema.mytable", "catalog", "mycatalog", "schema", "myschema", "table", "mytable"),
                new TestData("column:mycatalog.myschema.mytable.mycolumn", "catalog", "mycatalog", "schema", "myschema", "table", "mytable", "column", "mycolumn"),
                new TestData("trinouser:myuser", "trinouser", "myuser"),
                new TestData("systemproperty:myproperty", "systemproperty", "myproperty"),
                new TestData("sessionproperty:mycatalog.mysessionproperty", "catalog", "mycatalog", "sessionproperty", "mysessionproperty"),
                new TestData("function:myfunction", "function", "myfunction"),
                new TestData("procedure:mycatalog.myschema.myprocedure", "catalog", "mycatalog", "schema", "myschema", "procedure", "myprocedure"),
                new TestData("schemafunction:mycatalog.myschema.myschemafunction", "catalog", "mycatalog", "schema", "myschema", "schemafunction", "myschemafunction"),
                new TestData("queryid:12345-67890-abcdefg-hijklmnopqrs-tuvwxyz", "queryid", "12345-67890-abcdefg-hijklmnopqrs-tuvwxyz"),
                new TestData("sysinfo:systeminfo", "sysinfo", "systeminfo"),
                new TestData("role:myrole", "role", "myrole"),
                // invalid values
                new TestData("mycatalog.myschema.mytable.mycolumn"),                     // no resource-type
                new TestData(":mycatalog.myschema.mytable.mycolumn"),                    // empty resource-type
                new TestData("invalidResourceType:mycatalog.myschema.mytable.mycolumn"), // unknown resource-type
                new TestData("catalog:"),
                new TestData("schema:mycatalog_myschema"),
                new TestData("table:mycatalog_myschema_mytable"),
                new TestData("column:mycatalog_myschema_mytable_mycolumn"),
                new TestData("trinouser:"),
                new TestData("sessionproperty:mycatalog_mysessionproperty"),
                new TestData("procedure:mycatalog_myschema_myprocedure"),
        };

        for (TestData test : tests) {
            Map<String, String> actual = getResourceAsMap(test.resource, templates);

            assertEquals(test.expected, actual, test.resource);
        }
    }

    private static Map<String, RangerResourceTemplate> getHiveTemplates() throws Exception {
        Map<String, RangerResourceTemplate> ret = new HashMap<>();

        ret.put("database", new RangerResourceTemplate("{database}"));
        ret.put("table", new RangerResourceTemplate("{database}.{table}"));
        ret.put("column", new RangerResourceTemplate("{database}.{table}.{column}"));
        ret.put("udf", new RangerResourceTemplate("{database}.{udf}"));
        ret.put("url", new RangerResourceTemplate("{url}"));
        ret.put("hiveservice", new RangerResourceTemplate("{hiveservice}"));
        ret.put("global", new RangerResourceTemplate("{global}"));

        return ret;
    }

    private static Map<String, RangerResourceTemplate> getS3Templates() throws Exception {
        Map<String, RangerResourceTemplate> ret = new HashMap<>();

        ret.put("bucket", new RangerResourceTemplate("{bucket}"));
        ret.put("path", new RangerResourceTemplate("{bucket}/{path}"));

        return ret;
    }

    private static Map<String, RangerResourceTemplate> getAdlsGen2Templates() throws Exception {
        Map<String, RangerResourceTemplate> ret = new HashMap<>();

        ret.put("container", new RangerResourceTemplate("{container}@{storageaccount}"));
        ret.put("relativepath", new RangerResourceTemplate("{container}@{storageaccount}/{relativepath}"));

        return ret;
    }

    private static Map<String, RangerResourceTemplate> getTrinoTemplates() throws Exception {
        Map<String, RangerResourceTemplate> ret = new HashMap<>();

        ret.put("catalog", new RangerResourceTemplate("{catalog}"));
        ret.put("schema", new RangerResourceTemplate("{catalog}.{schema}"));
        ret.put("table", new RangerResourceTemplate("{catalog}.{schema}.{table}"));
        ret.put("column", new RangerResourceTemplate("{catalog}.{schema}.{table}.{column}"));
        ret.put("trinouser", new RangerResourceTemplate("{trinouser}"));
        ret.put("systemproperty", new RangerResourceTemplate("{systemproperty}"));
        ret.put("sessionproperty", new RangerResourceTemplate("{catalog}.{sessionproperty}"));
        ret.put("function", new RangerResourceTemplate("{function}"));
        ret.put("procedure", new RangerResourceTemplate("{catalog}.{schema}.{procedure}"));
        ret.put("schemafunction", new RangerResourceTemplate("{catalog}.{schema}.{schemafunction}"));
        ret.put("queryid", new RangerResourceTemplate("{queryid}"));
        ret.put("sysinfo", new RangerResourceTemplate("{sysinfo}"));
        ret.put("role", new RangerResourceTemplate("{role}"));

        return ret;
    }

    private Map<String, String> getResourceAsMap(String resource, Map<String, RangerResourceTemplate> templates) {
        String[]               resourceParts = resource.split(":", 2);
        String                 resourceType  = resourceParts.length > 0 ? resourceParts[0] : null;
        String                 resourceValue = resourceParts.length > 1 ? resourceParts[1] : null;
        RangerResourceTemplate template      = templates.get(resourceType);

        return template != null ? template.parse(resourceValue) : null;
    }

    private static class TestData {
        public final String              resource;
        public final Map<String, String> expected;

        public TestData(String resource, String...values) {
            this.resource = resource;

            if (values.length > 1) {
                this.expected = new HashMap<>();

                for (int i = 1; i < values.length; i += 2) {
                    expected.put(values[i - 1], values[i]);
                }
            } else {
                this.expected = null;
            }
        }
    }
}
