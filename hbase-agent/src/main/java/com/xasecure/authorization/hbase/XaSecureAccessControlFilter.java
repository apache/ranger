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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.TablePermission;

public class XaSecureAccessControlFilter extends FilterBase {

	private byte[] table = null;
	private User user = null;

	public XaSecureAccessControlFilter(User ugi, byte[] tableName) {
		table = tableName;
		user = ugi;
	}
	

	@SuppressWarnings("deprecation")
	@Override
	public ReturnCode filterKeyValue(Cell kv) throws IOException {
		HBaseAccessController accessController = HBaseAccessControllerFactory.getInstance();
		if (accessController.isAccessAllowed(user, table, kv.getFamily(), kv.getQualifier(), TablePermission.Action.READ)) {
			return ReturnCode.INCLUDE;
		} else {
			return ReturnCode.NEXT_COL;
		}
	}

}
