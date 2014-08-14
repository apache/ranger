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

package com.xasecure.authorization.hbase;

import java.util.List;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;

public interface HBaseAccessController {
	public boolean isAccessAllowed(User user, Action accessAction) ;
	public boolean isAccessAllowed(User user, byte[] tableName, Action accessAction) ;
	public boolean isAccessAllowed(User user, byte[] tableName, byte[] columnFamily, byte[] qualifier, Action accessAction) ;
	public boolean isEncrypted(byte[] tableName, byte[] columnFamily, byte[] qualifier) ;
	public boolean isAudited(byte[] tableName) ;
	public boolean isTableHasEncryptedColumn(byte[] tableName) ;
	public List<UserPermission>  getUserPermissions(User user) ;
	public List<UserPermission>  getUserPermissions(User user, byte[] tableName) ;



}
