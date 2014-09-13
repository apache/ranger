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

 package com.xasecure.authorization.hive;

import java.util.ArrayList;
import java.util.List;

import com.xasecure.authorization.utils.StringUtil;

public class XaHiveObjectAccessInfo {
	public enum HiveObjectType { NONE, DATABASE, TABLE, VIEW, PARTITION, INDEX, COLUMN, FUNCTION, URI };
	public enum HiveAccessType { NONE, CREATE, ALTER, DROP, INDEX, LOCK, INSERT, SELECT, UPDATE, USE, ALL, ADMIN };

	private String              mOperType         = null;
	private XaHiveAccessContext mContext          = null;
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

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName) {
		this(operType, context, accessType, dbName, null, HiveObjectType.DATABASE, dbName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, String tblName) {
		this(operType, context, accessType, dbName, tblName, HiveObjectType.TABLE, tblName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, HiveObjectType objType, String objName) {
		this(operType, context, accessType, dbName, null, objType, objName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, HiveObjectType objType, String objName) {
		this(operType, context, accessType, null, null, objType, objName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, String tblOrViewName, List<String> columns) {
		mOperType    = operType;
		mContext     = context;
		mAccessType  = accessType;
		mObjectType  = HiveObjectType.COLUMN;
		mDatabase    = dbName;
		mTable       = tblOrViewName;
		mView        = tblOrViewName;
		mColumns     = columns;
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, String tblName, HiveObjectType objType, String objName) {
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

	public XaHiveAccessContext getContext() {
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
		boolean ret = false;
		
		if(obj != null && obj instanceof XaHiveObjectAccessInfo) {
			XaHiveObjectAccessInfo that = (XaHiveObjectAccessInfo)obj;
			
			ret =  StringUtil.equalsIgnoreCase(mOperType, that.mOperType)
				&& mAccessType == that.mAccessType
				&& mObjectType == that.mObjectType
				&& StringUtil.equalsIgnoreCase(mDatabase, that.mDatabase)
				&& StringUtil.equalsIgnoreCase(mTable, that.mTable)
				&& StringUtil.equalsIgnoreCase(mView, that.mView)
				&& StringUtil.equalsIgnoreCase(mPartition, that.mPartition)
				&& StringUtil.equalsIgnoreCase(mIndex, that.mIndex)
				&& StringUtil.equalsIgnoreCase(mColumns, that.mColumns)
				  ;
		}
		
		return ret;
	}
}
