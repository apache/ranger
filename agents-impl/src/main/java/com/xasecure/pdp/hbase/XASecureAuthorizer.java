/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.pdp.hbase;

import java.util.List;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;

import com.xasecure.authorization.hbase.HBaseAccessController;

public class XASecureAuthorizer implements HBaseAccessController {

	private HBaseAccessController authDB = URLBasedAuthDB.getInstance();
	
	@Override
	public boolean isAccessAllowed(User user, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, accessAction);
		} else {
			return false;
		}
	}

	@Override
	public boolean isAccessAllowed(User user, byte[] tableName, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, tableName, accessAction);
		} else {
			return false;
		}
	}


	@Override
	public boolean isAccessAllowed(User user, byte[] tableName, byte[] columnFamily, byte[] qualifier, Action accessAction) {
		if (authDB != null) {
			return authDB.isAccessAllowed(user, tableName, columnFamily, qualifier, accessAction);
		} else {
			return false;
		}
	}

	@Override
	public boolean isEncrypted(byte[] tableName, byte[] columnFamily, byte[] qualifier) {
		if (authDB != null) {
			return authDB.isEncrypted(tableName, columnFamily, qualifier);
		} else {
			return false;
		}
	}
	
	@Override
	public boolean isTableHasEncryptedColumn(byte[] tableName) {
		if (authDB != null) {
			return authDB.isTableHasEncryptedColumn(tableName);
		} else {
			return false;
		}
	}


	@Override
	public boolean isAudited(byte[] tableName) {
		if (authDB != null) {
			return authDB.isAudited(tableName);
		} else {
			return false;
		}
	}
	
	@Override
	public List<UserPermission> getUserPermissions(User aUser) {
		if (authDB != null) {
			return authDB.getUserPermissions(aUser) ;
		} else {
			return null;
		}
	}

	@Override
	public List<UserPermission> getUserPermissions(User aUser, byte[] aTableName) {
		if (authDB != null) {
			return authDB.getUserPermissions(aUser, aTableName) ;
		} else {
			return null;
		}
	}
	
}
