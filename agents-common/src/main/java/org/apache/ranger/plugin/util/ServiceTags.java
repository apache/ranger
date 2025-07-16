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

package org.apache.ranger.plugin.util;


import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonAutoDetect(fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class ServiceTags implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	public static final String OP_ADD_OR_UPDATE = "add_or_update";
	public static final String OP_DELETE        = "delete";
	public static final String OP_REPLACE       = "replace";

	public enum TagsChangeExtent { NONE, TAGS, SERVICE_RESOURCE, ALL }
	public enum TagsChangeType { NONE, SERVICE_RESOURCE_UPDATE, TAG_UPDATE, TAG_RESOURCE_MAP_UPDATE, RANGER_ADMIN_START, INVALIDATE_TAG_DELTAS, ALL }

	private String                      op = OP_ADD_OR_UPDATE;
	private String                      serviceName;
	private Long                        tagVersion;
	private Date                        tagUpdateTime;
	private Map<Long, RangerTagDef>     tagDefinitions;
	private Map<Long, RangerTag>        tags;
	private List<RangerServiceResource> serviceResources;
	private Map<Long, List<Long>>       resourceToTagIds;
	private Boolean					 	isDelta;
	private TagsChangeExtent			tagsChangeExtent;
	private Boolean						isTagsDeduped;

	// MutablePair.left is the tag-id, MutablePair.right is the reference-count
	@JsonIgnore
	Map<RangerTag, MutablePair<Long, Long>> cachedTags = new HashMap<>();

	public ServiceTags() {
		this(OP_ADD_OR_UPDATE, null, 0L, null, null, null, null, null);
	}

	public ServiceTags(String op, String serviceName, Long tagVersion, Date tagUpdateTime, Map<Long, RangerTagDef> tagDefinitions,
					   Map<Long, RangerTag> tags, List<RangerServiceResource> serviceResources, Map<Long, List<Long>> resourceToTagIds) {
		this(op, serviceName, tagVersion, tagUpdateTime, tagDefinitions, tags, serviceResources, resourceToTagIds, false, TagsChangeExtent.ALL, false);
	}
	public ServiceTags(String op, String serviceName, Long tagVersion, Date tagUpdateTime, Map<Long, RangerTagDef> tagDefinitions,
					   Map<Long, RangerTag> tags, List<RangerServiceResource> serviceResources, Map<Long, List<Long>> resourceToTagIds, Boolean isDelta, TagsChangeExtent tagsChangeExtent, Boolean isTagsDeduped) {
		setOp(op);
		setServiceName(serviceName);
		setTagVersion(tagVersion);
		setTagUpdateTime(tagUpdateTime);
		setTagDefinitions(tagDefinitions);
		setTags(tags);
		setServiceResources(serviceResources);
		setResourceToTagIds(resourceToTagIds);
		setIsDelta(isDelta);
		setTagsChangeExtent(tagsChangeExtent);
		setIsTagsDeduped(isTagsDeduped);
	}

	public ServiceTags(ServiceTags other) {
		setOp(other.getOp());
		setServiceName(other.getServiceName());
		setTagVersion(other.getTagVersion());
		setTagUpdateTime(other.getTagUpdateTime());
		setTagDefinitions(other.getTagDefinitions() != null ? new HashMap<>(other.getTagDefinitions()) : null);
		setTags(other.getTags() != null ? new HashMap<>(other.getTags()) : null);
		setServiceResources(other.getServiceResources() != null ? new ArrayList<>(other.getServiceResources()) : null);
		setResourceToTagIds(other.getResourceToTagIds() != null ? new HashMap<>(other.getResourceToTagIds()) : null);
		setIsDelta(other.getIsDelta());
		setIsTagsDeduped(other.getIsTagsDeduped());
		setTagsChangeExtent(other.getTagsChangeExtent());

		this.cachedTags = new HashMap<>(other.cachedTags);
	}

	/**
	 * @return the op
	 */
	public String getOp() {
		return op;
	}

	/**
	 * @return the serviceName
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * @param op the op to set
	 */
	public void setOp(String op) {
		this.op = op;
	}

	/**
	 * @param serviceName the serviceName to set
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	/**
	 * @return the tagVersion
	 */
	public Long getTagVersion() {
		return tagVersion;
	}

	/**
	 * @param tagVersion the version to set
	 */
	public void setTagVersion(Long tagVersion) {
		this.tagVersion = tagVersion;
	}

	/**
	 * @return the tagUpdateTime
	 */
	public Date getTagUpdateTime() {
		return tagUpdateTime;
	}

	/**
	 * @param tagUpdateTime the tagUpdateTime to set
	 */
	public void setTagUpdateTime(Date tagUpdateTime) {
		this.tagUpdateTime = tagUpdateTime;
	}

	public Map<Long, RangerTagDef> getTagDefinitions() {
		return tagDefinitions;
	}

	public void setTagDefinitions(Map<Long, RangerTagDef> tagDefinitions) {
		this.tagDefinitions = tagDefinitions == null ? new HashMap<Long, RangerTagDef>() : tagDefinitions;
	}

	public Map<Long, RangerTag> getTags() {
		return tags;
	}

	public void setTags(Map<Long, RangerTag> tags) {
		this.tags = tags == null ? new HashMap<Long, RangerTag>() : tags;
	}

	public List<RangerServiceResource> getServiceResources() {
		return serviceResources;
	}

	public void setServiceResources(List<RangerServiceResource> serviceResources) {
		this.serviceResources = serviceResources == null ? new ArrayList<RangerServiceResource>() : serviceResources;
	}

	public Map<Long, List<Long>> getResourceToTagIds() {
		return resourceToTagIds;
	}

	public void setResourceToTagIds(Map<Long, List<Long>> resourceToTagIds) {
		this.resourceToTagIds = resourceToTagIds == null ? new HashMap<Long, List<Long>>() : resourceToTagIds;
	}

	public Boolean getIsDelta() {
		return isDelta == null ? Boolean.FALSE : isDelta;
	}

	public void setIsDelta(Boolean isDelta) {
		this.isDelta = isDelta;
	}

	public Boolean getIsTagsDeduped() {
		return isTagsDeduped == null ? Boolean.FALSE : isTagsDeduped;
	}

	public void setIsTagsDeduped(Boolean isTagsDeduped) {
		this.isTagsDeduped = isTagsDeduped;
	}

	public TagsChangeExtent getTagsChangeExtent() {
		return tagsChangeExtent;
	}

	public void setTagsChangeExtent(TagsChangeExtent tagsChangeExtent) {
		this.tagsChangeExtent = tagsChangeExtent;
	}
	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("ServiceTags={")
				.append("op=").append(op).append(", ")
				.append("serviceName=").append(serviceName).append(", ")
				.append("tagVersion=").append(tagVersion).append(", ")
				.append("tagUpdateTime={").append(tagUpdateTime).append("}")
				.append("isDelta={").append(isDelta).append("}")
				.append("tagsChangeExtent={").append(tagsChangeExtent).append("}")
				.append(", serviceResources={").append(serviceResources).append("}")
				.append(", tags={").append(tags).append("}")
				.append(", resourceToTagIds={").append(resourceToTagIds).append("}")
				.append(", isTagsDeduped={").append(isTagsDeduped).append("}")
				.append(", cachedTags={").append(cachedTags).append("}")
				.append("}");

		return sb;
	}

	public int dedupTags() {
		final int             ret;
		final Map<Long, Long> replacedIds      = new HashMap<>();
		final int             initialTagsCount = tags.size();
		final List<Long>      tagIdsToRemove   = new ArrayList<>();
		final Map<Long, RangerTag> tagsToAdd   = new HashMap<>();

		for (Iterator<Map.Entry<Long, RangerTag>> iter = tags.entrySet().iterator(); iter.hasNext(); ) {
			Map.Entry<Long, RangerTag> entry       = iter.next();
			Long                       tagId       = entry.getKey();
			RangerTag                  tag         = entry.getValue();
			MutablePair<Long, Long>    cachedTag   = cachedTags.get(tag);

			if (cachedTag == null) {
				cachedTags.put(tag, new MutablePair<>(tagId, 1L));
			} else if (!tagId.equals(cachedTag.left)) {
				if (tagId < cachedTag.left) {
					replacedIds.put(cachedTag.left, tagId);
					tagIdsToRemove.add(cachedTag.left);
					cachedTag.left = tagId;
				} else {
					replacedIds.put(tagId, cachedTag.left);
					tagsToAdd.put(cachedTag.left, tag);
					iter.remove();
				}
			}
		}

		for (Long tagIdToRemove : tagIdsToRemove) {
			tags.remove(tagIdToRemove);
		}

		// Add all the tags whose tagIds are modified back to tags
		tags.putAll(tagsToAdd);

		final int finalTagsCount = tags.size();

		for (Map.Entry<Long, List<Long>> resourceEntry : resourceToTagIds.entrySet()) {
			Set<Long> uniqueTagIds = new HashSet<>(resourceEntry.getValue().size());
			for (ListIterator<Long> listIter = resourceEntry.getValue().listIterator(); listIter.hasNext(); ) {
				final Long tagId       = listIter.next();
				Long       mappedTagId = null;

				for (Long replacerTagId = replacedIds.get(tagId); replacerTagId != null; replacerTagId = replacedIds.get(mappedTagId)) {
					mappedTagId = replacerTagId;
				}

				if (mappedTagId == null) {
					continue;
				}

				if (!uniqueTagIds.add(mappedTagId)) {
					listIter.remove();
					continue;
				}

				listIter.set(mappedTagId);

				RangerTag tag = tags.get(mappedTagId);

				if (tag != null) {    // This should always be true
					MutablePair<Long, Long> cachedTag = cachedTags.get(tag);

					if (cachedTag != null) { // This should always be true
						cachedTag.right++;
					}
				}
			}
		}

		ret = initialTagsCount - finalTagsCount;

		return ret;
	}

	public void dedupStrings() {
		Map<String, String> strTbl = new HashMap<>();

		op          = StringUtil.dedupString(op, strTbl);
		serviceName = StringUtil.dedupString(serviceName, strTbl);

		if (tagDefinitions != null) {
			for (RangerTagDef tagDef : tagDefinitions.values()) {
				tagDef.dedupStrings(strTbl);
			}
		}

		if (tags != null) {
			for (RangerTag tag : tags.values()) {
				tag.dedupStrings(strTbl);
			}
		}

		if (serviceResources != null) {
			for (RangerServiceResource resource : serviceResources) {
				resource.dedupStrings(strTbl);
			}
		}
	}
}
