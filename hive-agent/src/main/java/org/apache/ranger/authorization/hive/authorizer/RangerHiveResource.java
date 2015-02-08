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

import java.util.Set;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerResource;

import com.google.common.collect.Sets;


public class RangerHiveResource implements RangerResource {
	public static final String KEY_DATABASE = "database";
	public static final String KEY_TABLE    = "table";
	public static final String KEY_UDF      = "udf";
	public static final String KEY_COLUMN   = "column";

	public static final Set<String> KEYS_DATABASE = Sets.newHashSet(KEY_DATABASE);
	public static final Set<String> KEYS_TABLE    = Sets.newHashSet(KEY_DATABASE, KEY_TABLE);
	public static final Set<String> KEYS_UDF      = Sets.newHashSet(KEY_DATABASE, KEY_UDF);
	public static final Set<String> KEYS_COLUMN   = Sets.newHashSet(KEY_DATABASE, KEY_TABLE, KEY_COLUMN);

	private HiveObjectType objectType = null;
	private String         database   = null;
	private String         tableOrUdf = null;
	private String         column     = null;
	private Set<String>    keys       = null;


	public RangerHiveResource(HiveObjectType objectType, String database) {
		this(objectType, database, null, null);
	}

	public RangerHiveResource(HiveObjectType objectType, String database, String tableOrUdf) {
		this(objectType, database, tableOrUdf, null);
	}
	
	public RangerHiveResource(HiveObjectType objectType, String database, String tableOrUdf, String column) {
		this.objectType = objectType;
		this.database   = database;
		this.tableOrUdf = tableOrUdf;
		this.column     = column;

		switch(objectType) {
			case DATABASE:
				keys = KEYS_DATABASE;
			break;
	
			case FUNCTION:
				keys = KEYS_UDF;
			break;

			case COLUMN:
				keys = KEYS_COLUMN;
			break;

			case TABLE:
			case VIEW:
			case INDEX:
			case PARTITION:
				keys = KEYS_TABLE;
			break;

			case NONE:
			case URI:
			default:
				keys = null;
			break;
		}
	}

	@Override
	public String getOwnerUser() {
		return null; // no owner information available
	}

	@Override
	public boolean exists(String name) {
		return !StringUtils.isEmpty(getValue(name));
	}

	@Override
	public String getValue(String name) {
		if(StringUtils.equalsIgnoreCase(name, KEY_DATABASE)) {
			return database;
		} else if(objectType == HiveObjectType.FUNCTION) {
			if(StringUtils.equalsIgnoreCase(name, KEY_UDF)) {
				return tableOrUdf;
			}
		} else if(StringUtils.equalsIgnoreCase(name, KEY_TABLE)) {
			return tableOrUdf;
		} else  if(StringUtils.equalsIgnoreCase(name, KEY_COLUMN)) {
			return column;
		}

		return null;
	}

	public Set<String> getKeys() {
		return keys;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof RangerHiveResource)) {
			return false;
		}

		if(this == obj) {
			return true;
		}

		RangerHiveResource other = (RangerHiveResource) obj;

		return ObjectUtils.equals(objectType, other.objectType) &&
			   ObjectUtils.equals(database, other.database) &&
			   ObjectUtils.equals(tableOrUdf, other.tableOrUdf) &&
			   ObjectUtils.equals(column, other.column);
	}

	@Override
	public int hashCode() {
		int ret = 7;

		ret = 31 * ret + ObjectUtils.hashCode(objectType);
		ret = 31 * ret + ObjectUtils.hashCode(database);
		ret = 31 * ret + ObjectUtils.hashCode(tableOrUdf);
		ret = 31 * ret + ObjectUtils.hashCode(column);

		return ret;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("objectType={").append(objectType).append("} ");
		sb.append("database={").append(database).append("} ");
		sb.append("tableOrUdf={").append(tableOrUdf).append("} ");
		sb.append("column={").append(column).append("} ");
		
		return sb;
	}

	public HiveObjectType getObjectType() {
		return objectType;
	}

	public String getDatabase() {
		return database;
	}

	public String getTableOrUdf() {
		return tableOrUdf;
	}

	public String getColumn() {
		return column;
	}
}
