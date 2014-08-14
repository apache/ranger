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

package com.xasecure.authorization.hadoop.exceptions;

import org.apache.hadoop.security.AccessControlException;


public class XaSecureAccessControlException extends AccessControlException {

	private static final long serialVersionUID = -4673975720243484927L;

	public XaSecureAccessControlException(String aMsg) {
		super(aMsg) ;
	}

}
