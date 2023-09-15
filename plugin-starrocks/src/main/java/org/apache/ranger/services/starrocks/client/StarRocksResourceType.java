// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.ranger.services.starrocks.client;

public class StarRocksResourceType {
    public static final String CATALOG = "catalog";
    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String COLUMN = "column";
    public static final String VIEW = "view";
    public static final String MATERIALIZED_VIEW = "materialized_view";
    public static final String FUNCTION = "function";
    public static final String GLOBAL_FUNCTION = "global_function";
    public static final String RESOURCE = "resource";
    public static final String RESOURCE_GROUP = "resource_group";
    public static final String STORAGE_VOLUME = "storage_volume";
    public static final String USER = "user";
    public static final String SYSTEM = "system";
}
