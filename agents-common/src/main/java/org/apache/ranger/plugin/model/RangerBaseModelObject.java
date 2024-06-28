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

package org.apache.ranger.plugin.model;

import java.util.Collections;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.authorization.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public static final String NULL_SAFE_SUPPLIER_V2 = "v2";

	private static NullSafeSupplier NULL_SAFE_SUPPLIER = NullSafeSupplierV1.INSTANCE;

	private Long    id;
	private String  guid;
	private Boolean isEnabled;
	private String  createdBy;
	private String  updatedBy;
	private Date    createTime;
	private Date    updateTime;
	private Long    version;

	public RangerBaseModelObject() {
		setIsEnabled(null);
	}

	public void updateFrom(RangerBaseModelObject other) {
		setIsEnabled(other.getIsEnabled());
	}

	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}
	/**
	 * @return the guid
	 */
	public String getGuid() {
		return guid;
	}
	/**
	 * @param guid the guid to set
	 */
	public void setGuid(String guid) {
		this.guid = guid;
	}
	/**
	 * @return the isEnabled
	 */
	public Boolean getIsEnabled() {
		return isEnabled;
	}
	/**
	 * @param isEnabled the isEnabled to set
	 */
	public void setIsEnabled(Boolean isEnabled) {
		this.isEnabled = isEnabled == null ? Boolean.TRUE : isEnabled;
	}
	/**
	 * @return the createdBy
	 */
	public String getCreatedBy() {
		return createdBy;
	}
	/**
	 * @param createdBy the createdBy to set
	 */
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	/**
	 * @return the updatedBy
	 */
	public String getUpdatedBy() {
		return updatedBy;
	}
	/**
	 * @param updatedBy the updatedBy to set
	 */
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	/**
	 * @return the createTime
	 */
	public Date getCreateTime() {
		return createTime;
	}
	/**
	 * @param createTime the createTime to set
	 */
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	/**
	 * @return the updateTime
	 */
	public Date getUpdateTime() {
		return updateTime;
	}
	/**
	 * @param updateTime the updateTime to set
	 */
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	/**
	 * @return the version
	 */
	public Long getVersion() {
		return version;
	}
	/**
	 * @param version the version to set
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	public void dedupStrings(Map<String, String> strTbl) {
		createdBy = StringUtil.dedupString(createdBy, strTbl);
		updatedBy = StringUtil.dedupString(updatedBy, strTbl);
	}

	public static <T> List<T> nullSafeList(List<T> coll) {
		return NULL_SAFE_SUPPLIER.toList(coll);
	}

	public static <T> Set<T> nullSafeSet(Set<T> coll) {
		return NULL_SAFE_SUPPLIER.toSet(coll);
	}

	public static <K, V> Map<K, V> nullSafeMap(Map<K, V> coll) {
		return NULL_SAFE_SUPPLIER.toMap(coll);
	}

	public static <T> List<T> getUpdatableList(List<T> curr) {
		final List<T> ret;

		if (curr instanceof ArrayList) {
			ret = curr;
		} else {
			ret = curr != null ? new ArrayList<>(curr) : new ArrayList<>();
		}

		return ret;
	}

	public static <T> Set<T> getUpdatableSet(Set<T> curr) {
		final Set<T> ret;

		if (curr instanceof HashSet) {
			ret = curr;
		} else {
			ret = curr != null ? new HashSet<>(curr) : new HashSet<>();
		}

		return ret;
	}

	public static <K, V> Map<K, V> getUpdatableMap(Map<K, V> curr) {
		final Map<K, V> ret;

		if (curr instanceof HashMap) {
			ret = curr;
		} else {
			ret = curr != null ? new HashMap<>(curr) : new HashMap<>();
		}

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("id={").append(id).append("} ");
		sb.append("guid={").append(guid).append("} ");
		sb.append("isEnabled={").append(isEnabled).append("} ");
		sb.append("createdBy={").append(createdBy).append("} ");
		sb.append("updatedBy={").append(updatedBy).append("} ");
		sb.append("createTime={").append(createTime).append("} ");
		sb.append("updateTime={").append(updateTime).append("} ");
		sb.append("version={").append(version).append("} ");

		return sb;
	}

	protected NullSafeSupplier getNullSafeSupplier() { return NULL_SAFE_SUPPLIER; }

	public static void setNullSafeSupplier(NullSafeSupplier supplier) {
		NULL_SAFE_SUPPLIER = supplier == null ? NullSafeSupplierV1.INSTANCE : supplier;
	}

	public static void setNullSafeSupplier(String supplier) {
		if (NULL_SAFE_SUPPLIER_V2.equalsIgnoreCase(supplier)) {
			NULL_SAFE_SUPPLIER = NullSafeSupplierV2.INSTANCE;
		} else {
			NULL_SAFE_SUPPLIER = NullSafeSupplierV1.INSTANCE;
		}
	}

	public static abstract class NullSafeSupplier {
		public abstract <T> List<T> toList(List<T> coll);

		public abstract <T> Set<T> toSet(Set<T> coll);

		public abstract <K, V> Map<K, V> toMap(Map<K, V> coll);
	}


	// each call creates a new collection object
	// 1. for a null/empty collection, return a new collection object
	// 2. for a non-null collection, return a copy of the collection
	public static class NullSafeSupplierV1 extends NullSafeSupplier {
		public static final NullSafeSupplierV1 INSTANCE = new NullSafeSupplierV1();

		private NullSafeSupplierV1() { }

		public <T> List<T> toList(List<T> coll) {
			return (coll == null || coll.isEmpty()) ? new ArrayList<>() : new ArrayList<>(coll);
		}

		public <T> Set<T> toSet(Set<T> coll) {
			return (coll == null || coll.isEmpty()) ? new HashSet<>() : new HashSet<>(coll);
		}

		public <K, V> Map<K, V> toMap(Map<K, V> coll) {
			return (coll == null || coll.isEmpty()) ? new HashMap<>() : new HashMap<>(coll);
		}
	}

	// calls do not create collection objects
	// 1. for a null/empty collection, return Collections.empty*()
	// 2. for a non-null collection, return that collection itself
	public static class NullSafeSupplierV2 extends NullSafeSupplier {
		public static final NullSafeSupplierV2 INSTANCE = new NullSafeSupplierV2();

		private NullSafeSupplierV2() { }

		public <T> List<T> toList(List<T> coll) {
			return (coll == null || coll.isEmpty()) ? Collections.emptyList() : coll;
		}

		public <T> Set<T> toSet(Set<T> coll) {
			return (coll == null || coll.isEmpty()) ? Collections.emptySet() : coll;
		}

		public <K, V> Map<K, V> toMap(Map<K, V> coll) {
			return (coll == null || coll.isEmpty()) ? Collections.emptyMap() : coll;
		}
	}
}
