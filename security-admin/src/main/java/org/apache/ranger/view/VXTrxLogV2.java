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

package org.apache.ranger.view;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.json.JsonDateSerializer;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class VXTrxLogV2 implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	protected Long             id;
	@JsonSerialize(using=JsonDateSerializer.class)
	protected Date             createDate;
	protected String           createdBy;
	protected int              objectClassType = RangerConstants.CLASS_TYPE_NONE;
	protected Long             objectId;
	protected String           objectName;
	protected int              parentObjectClassType;
	protected Long             parentObjectId;
	protected String           parentObjectName;
	protected String           action;
	protected ObjectChangeInfo changeInfo;
	protected String           requestId;
	protected String           transactionId;
	protected String           sessionId;
	protected String           sessionType;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXTrxLogV2( ) {
	}

	public VXTrxLogV2(VXTrxLog trxLog) {
		if (trxLog != null) {
			this.id                    = trxLog.getId();
			this.createDate            = trxLog.getCreateDate();
			this.createdBy             = trxLog.getOwner();
			this.objectClassType       = trxLog.getObjectClassType();
			this.objectId              = trxLog.getObjectId();
			this.objectName            = trxLog.getObjectName();
			this.parentObjectClassType = trxLog.getParentObjectClassType();
			this.parentObjectId        = trxLog.getParentObjectId();
			this.parentObjectName      = trxLog.getParentObjectName();
			this.action                = trxLog.getAction();
			this.requestId             = trxLog.getRequestId();
			this.transactionId         = trxLog.getTransactionId();
			this.sessionId             = trxLog.getSessionId();
			this.sessionType           = trxLog.getSessionType();

			if (StringUtils.isNotBlank(trxLog.getAttributeName())) {
				this.changeInfo = new ObjectChangeInfo();

				this.changeInfo.addAttribute(new AttributeChangeInfo(trxLog.getAttributeName(), trxLog.getPreviousValue(), trxLog.getNewValue()));
			} else {
				this.changeInfo = null;
			}
		}
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getId( ) {
		return id;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	public Date getCreateDate() {
		return createDate;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setObjectClassType(int objectClassType) {
		this.objectClassType = objectClassType;
	}

	public int getObjectClassType() {
		return this.objectClassType;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public Long getObjectId() {
		return this.objectId;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public String getObjectName() {
		return this.objectName;
	}

	public void setParentObjectClassType(int parentObjectClassType) {
		this.parentObjectClassType = parentObjectClassType;
	}

	public int getParentObjectClassType() {
		return this.parentObjectClassType;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public Long getParentObjectId() {
		return this.parentObjectId;
	}

	public void setParentObjectName(String parentObjectName) {
		this.parentObjectName = parentObjectName;
	}

	public String getParentObjectName() {
		return this.parentObjectName;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getAction() {
		return this.action;
	}

	public void setChangeInfo(ObjectChangeInfo changeInfo) {
		this.changeInfo = changeInfo;
	}

	public ObjectChangeInfo getChangeInfo() {
		return this.changeInfo;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getRequestId() {
		return this.requestId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getTransactionId() {
		return this.transactionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getSessionId() {
		return this.sessionId;
	}

	public void setSessionType(String sessionType) {
		this.sessionType = sessionType;
	}

	public String getSessionType() {
		return this.sessionType;
	}

	@Override
	public String toString( ) {
		return toString(new StringBuilder()).toString();
	}

	public static VXTrxLog toVXTrxLog(VXTrxLogV2 trxLogV2) {
		VXTrxLog ret = new VXTrxLog();

		if (trxLogV2 != null) {
			ret.setId(trxLogV2.getId());
			ret.setCreateDate(trxLogV2.getCreateDate());
			ret.setUpdateDate(trxLogV2.getCreateDate());
			ret.setOwner(trxLogV2.getCreatedBy());
			ret.setUpdatedBy(trxLogV2.getCreatedBy());
			ret.setObjectClassType(trxLogV2.getObjectClassType());
			ret.setObjectId(trxLogV2.getObjectId());
			ret.setObjectName(trxLogV2.getObjectName());
			ret.setParentObjectClassType(trxLogV2.getParentObjectClassType());
			ret.setParentObjectId((trxLogV2.getParentObjectId()));
			ret.setParentObjectName(trxLogV2.getParentObjectName());
			ret.setAction(trxLogV2.getAction());
			ret.setRequestId(trxLogV2.getRequestId());
			ret.setTransactionId(trxLogV2.getTransactionId());
			ret.setSessionId(trxLogV2.getSessionId());
			ret.setSessionType(trxLogV2.getSessionType());
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("VXTrxLogV2={")
		  .append("id={").append(id).append("} ")
		  .append("createDate={").append(createDate).append("} ")
		  .append("createdBy={").append(createdBy).append("} ")
		  .append("objectClassType={").append(objectClassType).append("} ")
		  .append("objectId={").append(objectId).append("} ")
		  .append("objectName={").append(objectName).append("} ")
		  .append("parentObjectId={").append(parentObjectId).append("} ")
		  .append("parentObjectClassType={").append(parentObjectClassType).append("} ")
		  .append("parentObjectName={").append(parentObjectName).append("} ")
		  .append("action={").append(action).append("} ")
		  .append("changeInfo={");

		if (changeInfo != null) {
			changeInfo.toString(sb);
		}

		sb.append("} ")
		  .append("requestId={").append(requestId).append("} ")
		  .append("transactionId={").append(transactionId).append("} ")
		  .append("sessionId={").append(sessionId).append("} ")
		  .append("sessionType={").append(sessionType).append("} ")
		  .append("}");

		return sb;
	}

	@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown=true)
	public static class ObjectChangeInfo implements Serializable {
		private static final long serialVersionUID = 1L;

		private List<AttributeChangeInfo> attributes;

		public List<AttributeChangeInfo> getAttributes() {
			return attributes;
		}

		public void setAttributes(List<AttributeChangeInfo> attributes) {
			this.attributes = attributes;
		}

		public void addAttribute(AttributeChangeInfo changeInfo) {
			if (attributes == null) {
				attributes = new ArrayList<>();
			}

			attributes.add(changeInfo);
		}

		public void addAttribute(String attributeName, String oldValue, String newValue) {
			addAttribute(new AttributeChangeInfo(attributeName, oldValue, newValue));
		}

		@JsonIgnore
		public String toJson() {
			try {
				return JsonUtilsV2.objToJson(this);
			} catch (Exception e) {
				// TODO: log error
				return null;
			}
		}

		@Override
		public String toString( ) {
			return toString(new StringBuilder()).toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("{ attributes=[");

			if (attributes != null) {
				for (AttributeChangeInfo changeInfo : attributes) {
					changeInfo.toString(sb);
				}
			}

			sb.append("] }");

			return sb;
		}
	}

	@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown=true)
	public static class AttributeChangeInfo implements Serializable {
		private static final long serialVersionUID = 1L;

		private String attributeName;
		private String oldValue;
		private String newValue;

		public AttributeChangeInfo() { }

		public AttributeChangeInfo(String attributeName, String oldValue, String newValue) {
			this.attributeName = attributeName;
			this.oldValue      = oldValue;
			this.newValue      = newValue;
		}

		public String getAttributeName() {
			return attributeName;
		}

		public void setAttributeName(String attributeName) {
			this.attributeName = attributeName;
		}

		public String getOldValue() {
			return oldValue;
		}

		public void setOldValue(String oldValue) {
			this.oldValue = oldValue;
		}

		public String getNewValue() {
			return newValue;
		}

		public void setNewValue(String newValue) {
			this.newValue = newValue;
		}

		@Override
		public String toString( ) {
			return toString(new StringBuilder()).toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("{ attributeName={").append(attributeName).append("} ")
			  .append("oldValue={").append(oldValue).append("} ")
			  .append("newValue={").append(newValue).append("} ")
			  .append("}");

			return sb;
		}
	}
}
