package com.xasecure.authorization.hive;

import java.util.ArrayList;
import java.util.List;

import com.xasecure.authorization.utils.StringUtil;

public class XaHiveObjectAccessInfo {
	public enum HiveObjectType { NONE, DATABASE, TABLE, VIEW, PARTITION, INDEX, COLUMN, FUNCTION };
	public enum HiveAccessType { NONE, CREATE, ALTER, DROP, INDEX, LOCK, INSERT, SELECT, UPDATE, USE };

	private String            mOperType;
	private XaHiveAccessContext mContext;
	private HiveAccessType    mAccessType;
	private HiveObjectType    mObjectType;
	private String            mDatabase;
	private String            mTable;
	private String            mView;
	private String            mPartition;
	private String            mIndex;
	private List<String>      mColumns;
	private String            mFunction;
	private String            mDeniedObjectName;

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName) {
		this(operType, context, accessType, dbName, null, HiveObjectType.DATABASE, dbName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, String tblName) {
		this(operType, context, accessType, dbName, tblName, HiveObjectType.TABLE, tblName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, HiveObjectType objType, String objName) {
		this(operType, context, accessType, dbName, null, objType, objName);
	}

	public XaHiveObjectAccessInfo(String operType, XaHiveAccessContext context, HiveAccessType accessType, String dbName, String tblOrViewName, List<String> columns) {
		mOperType    = operType;
		mContext     = context;
		mAccessType  = accessType;
		mObjectType  = HiveObjectType.COLUMN;
		mDatabase    = dbName;
		mTable       = tblOrViewName;
		mView        = tblOrViewName;
		mPartition   = null;
		mIndex       = null;
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
		mPartition   = null;
		mIndex       = null;
		mColumns     = null;

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

	public void setDeinedObjectName(String deniedObjectName) {
		mDeniedObjectName = deniedObjectName;
	}

	public String getDeinedObjectName() {
		return mDeniedObjectName;
	}

	public String getObjectName() {
		String objName = StringUtil.isEmpty(mDatabase) ? "" : mDatabase;

		if(! StringUtil.isEmpty(mTable))
			objName += ("/" + mTable);
		else if(! StringUtil.isEmpty(mView))
			objName += ("/" + mView);
		else if(! StringUtil.isEmpty(mFunction))
			objName += ("/" + mFunction);

		if(! StringUtil.isEmpty(mColumns))
			objName += ("/" + StringUtil.toString(mColumns));
		else if(! StringUtil.isEmpty(mIndex))
			objName += ("/" + mIndex);
		
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
