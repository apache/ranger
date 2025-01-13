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
package org.apache.ranger.authorization.presto.authorizer;

import io.prestosql.spi.connector.SchemaTableName;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Optional;

class RangerPrestoResource extends RangerAccessResourceImpl {
    public static final String KEY_CATALOG          = "catalog";
    public static final String KEY_SCHEMA           = "schema";
    public static final String KEY_TABLE            = "table";
    public static final String KEY_COLUMN           = "column";
    public static final String KEY_USER             = "prestouser";
    public static final String KEY_FUNCTION         = "function";
    public static final String KEY_PROCEDURE        = "procedure";
    public static final String KEY_SYSTEM_PROPERTY  = "systemproperty";
    public static final String KEY_SESSION_PROPERTY = "sessionproperty";

    public RangerPrestoResource() {}

    public RangerPrestoResource(String catalogName, Optional<String> schema, Optional<String> table) {
        setValue(KEY_CATALOG, catalogName);

        schema.ifPresent(s -> setValue(KEY_SCHEMA, s));

        table.ifPresent(s -> setValue(KEY_TABLE, s));
    }

    public RangerPrestoResource(String catalogName, Optional<String> schema, Optional<String> table, Optional<String> column) {
        setValue(KEY_CATALOG, catalogName);

        schema.ifPresent(s -> setValue(KEY_SCHEMA, s));

        table.ifPresent(s -> setValue(KEY_TABLE, s));

        column.ifPresent(s -> setValue(KEY_COLUMN, s));
    }

    public String getCatalogName() {
        return (String) getValue(KEY_CATALOG);
    }

    public String getTable() {
        return (String) getValue(KEY_TABLE);
    }

    public String getCatalog() {
        return (String) getValue(KEY_CATALOG);
    }

    public String getSchema() {
        return (String) getValue(KEY_SCHEMA);
    }

    public Optional<SchemaTableName> getSchemaTable() {
        final String schema = getSchema();

        if (StringUtils.isNotEmpty(schema)) {
            return Optional.of(new SchemaTableName(schema, Optional.ofNullable(getTable()).orElse("*")));
        }

        return Optional.empty();
    }
}
