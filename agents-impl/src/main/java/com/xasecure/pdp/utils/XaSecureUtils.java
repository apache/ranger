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

 /**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *

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
