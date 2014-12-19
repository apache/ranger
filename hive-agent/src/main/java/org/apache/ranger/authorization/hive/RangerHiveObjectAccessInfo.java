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

 package org.apache.ranger.authorization.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.ranger.authorization.utils.StringUtil;

public class RangerHiveObjectAccessInfo {
	public enum HiveObjectType { NONE, DATABASE, TABLE, VIEW, PARTITION, INDEX, COLUMN, FUNCTION, URI };
	public enum HiveAccessType { NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN };

	private String              mOperType         = null;
	private RangerHiveAccessContext mContext          = null;
	private HiveAccessType      mAccessType       = HiveAccessType.NONE;
	private HiveObjectType      mObjectType       = HiveObjectType.NONE;
	private String              mDatabase         = null;
	private String              mTable            = null;
	private String              mView             = null;
	private String              mPartition        = null;
	private String              mIndex            = null;
	private List<String>        mColumns          = null;
	private String              mFunction         = null;
	private String              mUri              = null;
	private String              mDeniedObjectName = null;

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, String dbName) {
		this(operType, context, accessType, dbName, null, HiveObjectType.DATABASE, dbName);
	}

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, String dbName, String tblName) {
		this(operType, context, accessType, dbName, tblName, HiveObjectType.TABLE, tblName);
	}

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, String dbName, HiveObjectType objType, String objName) {
		this(operType, context, accessType, dbName, null, objType, objName);
	}

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, HiveObjectType objType, String objName) {
		this(operType, context, accessType, null, null, objType, objName);
	}

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, String dbName, String tblOrViewName, List<String> columns) {
		mOperType    = operType;
		mContext     = context;
		mAccessType  = accessType;
		mObjectType  = HiveObjectType.COLUMN;
		mDatabase    = dbName;
		mTable       = tblOrViewName;
		mView        = tblOrViewName;
		mColumns     = columns;
	}

	public RangerHiveObjectAccessInfo(String operType, RangerHiveAccessContext context, HiveAccessType accessType, String dbName, String tblName, HiveObjectType objType, String objName) {
		mOperType    = operType;
		mContext     = context;
		mAccessType  = accessType;
		mObjectType  = objType;
		mDatabase    = dbName;
		mTable       = tblName;
		mView        = tblName;

		if(objName != null && ! objName.trim().isEmpty()) {
			switch(objType) {
				case DATABASE:
					mDatabase = objName;
				break;

				case TABLE:
					mTable = objName;
				break;

				case VIEW:
					mView = objName;
				break;

				case PARTITION:
					mPartition = objName;
				break;

				case INDEX:
					mIndex = objName;
				break;

				case COLUMN:
					mColumns = new ArrayList<String>();
					mColumns.add(objName);
				break;

				case FUNCTION:
					mFunction = objName;
				break;

				case URI:
					mUri = objName;
				break;

				case NONE:
				break;
			}
		}
	}

	public String getOperType() {
		return mOperType;
	}

	public RangerHiveAccessContext getContext() {
		return mContext;
	}

	public HiveAccessType getAccessType() {
		return mAccessType;
	}

	public HiveObjectType getObjectType() {
		return mObjectType;
	}

	public String getDatabase() {
		return mDatabase;
	}

	public String getTable() {
		return mTable;
	}

	public String getView() {
		return mView;
	}

	public String getPartition() { 
		return mPartition;
	}

	public String getIndex() {
		return mIndex;
	}

	public List<String> getColumns() {
		return mColumns;
	}

	public String getFunction() {
		return mFunction;
	}

	public String getUri() {
		return mUri;
	}

	public void setDeinedObjectName(String deniedObjectName) {
		mDeniedObjectName = deniedObjectName;
	}

	public String getDeinedObjectName() {
		return mDeniedObjectName;
	}

	public String getObjectName() {
        String objName = null;

        if(this.mObjectType == HiveObjectType.URI) {
            objName = mUri;
        } else {
            String tblName = null;
            String colName = null;

            if(! StringUtil.isEmpty(mTable))
                tblName = mTable;
            else if(! StringUtil.isEmpty(mView))
                tblName = mView;
            else if(! StringUtil.isEmpty(mFunction))
                tblName = mFunction;

            if(! StringUtil.isEmpty(mColumns))
                colName = StringUtil.toString(mColumns);
            else if(! StringUtil.isEmpty(mIndex))
                colName = mIndex;

            objName = getObjectName(mDatabase, tblName, colName);
        }

		return objName;
	}
	
	public static String getObjectName(String dbName, String tblName, String colName) {
		String objName = StringUtil.isEmpty(dbName) ? "" : dbName;
		
		if(!StringUtil.isEmpty(tblName)) {
			objName += ("/" + tblName);
			
			if(!StringUtil.isEmpty(colName)) {
				objName += ("/" + colName);
			}
		}

		return objName;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != getClass()) {
			return false;
		}
		RangerHiveObjectAccessInfo that = (RangerHiveObjectAccessInfo) obj;
		return new EqualsBuilder()
				.appendSuper(super.equals(obj))
				.append(mAccessType, that.mAccessType)
				.append(mColumns, that.mColumns)
				.append(mContext, that.mContext)
				.append(mDatabase, that.mDatabase)
				.append(mDeniedObjectName, that.mDeniedObjectName)
				.append(mFunction, that.mFunction)
				.append(mIndex, that.mIndex)
				.append(mObjectType, that.mObjectType)
				.append(mOperType, that.mOperType)
				.append(mPartition, that.mPartition)
				.append(mTable, that.mTable)
				.append(mUri, that.mUri)
				.append(mView, that.mView)
				.isEquals();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(37, 41)
		.appendSuper(43)
		.append(mAccessType)
		.append(mColumns)
		.append(mContext)
		.append(mDatabase)
		.append(mDeniedObjectName)
		.append(mFunction)
		.append(mIndex)
		.append(mObjectType)
		.append(mOperType)
		.append(mPartition)
		.append(mTable)
		.append(mUri)
		.append(mView)
		.toHashCode();
	}
}
