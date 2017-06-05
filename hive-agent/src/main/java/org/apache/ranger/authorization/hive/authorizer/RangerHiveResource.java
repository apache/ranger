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

package org.apache.ranger.authorization.hive.authorizer;


import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;



public class RangerHiveResource extends RangerAccessResourceImpl {
	public static final String KEY_DATABASE = "database";
	public static final String KEY_TABLE    = "table";
	public static final String KEY_UDF      = "udf";
	public static final String KEY_COLUMN   = "column";
	public static final String KEY_URL		= "url";

	private HiveObjectType objectType = null;

	public RangerHiveResource(HiveObjectType objectType, String databaseorUrl) {
		this(objectType, databaseorUrl, null, null);
	}

	public RangerHiveResource(HiveObjectType objectType, String databaseorUrl, String tableOrUdf) {
		this(objectType, databaseorUrl, tableOrUdf, null);
	}
	
	public RangerHiveResource(HiveObjectType objectType, String databaseorUrl, String tableOrUdf, String column) {
		this.objectType = objectType;

		switch(objectType) {
			case DATABASE:
				setValue(KEY_DATABASE, databaseorUrl);
			break;
	
			case FUNCTION:
				if (databaseorUrl == null) {
					databaseorUrl = "";
				}
				setValue(KEY_DATABASE, databaseorUrl);
				setValue(KEY_UDF, tableOrUdf);
			break;

			case COLUMN:
				setValue(KEY_DATABASE, databaseorUrl);
				setValue(KEY_TABLE, tableOrUdf);
				setValue(KEY_COLUMN, column);
			break;

			case TABLE:
			case VIEW:
			case INDEX:
			case PARTITION:
				setValue(KEY_DATABASE, databaseorUrl);
				setValue(KEY_TABLE, tableOrUdf);
			break;

			case URI:
				setValue(KEY_URL,databaseorUrl);
			break;

			case NONE:
			default:
			break;
		}
	}

	public HiveObjectType getObjectType() {
		return objectType;
	}

	public String getDatabase() {
		return getValue(KEY_DATABASE);
	}

	public String getTable() {
		return getValue(KEY_TABLE);
	}

	public String getUdf() {
		return getValue(KEY_UDF);
	}

	public String getColumn() {
		return getValue(KEY_COLUMN);
	}

	public String getUrl() {
		return getValue(KEY_URL);
	}
}
