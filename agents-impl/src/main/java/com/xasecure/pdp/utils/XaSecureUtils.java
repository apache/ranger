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

package com.xasecure.pdp.utils;

import java.io.File;
import java.net.URL;

public class XaSecureUtils {

	public static String getFilePathFromClassPath(String aFileName) {
		String pathName = null;
		
		File lf = new File(aFileName) ;
		
		if (lf.exists()) {
			pathName = lf.getAbsolutePath();
		}
		else  {
			URL lurl = XaSecureUtils.class.getResource(aFileName);
			if (lurl == null) {
				if (!aFileName.startsWith("/")) {
					lurl = XaSecureUtils.class.getResource("/" + aFileName);
				}
			}
			if (lurl != null) {
				pathName = lurl.getFile();
			}
		}
		return pathName;

	}
}
