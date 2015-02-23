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

package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class SampleKeyProviderFactory extends KeyProviderFactory {

	private static final String SAMPLE_KEY_PROVIDER_SCHEMA_NAME = "sample" ;
	
	@Override
	public KeyProvider createProvider(URI aURI, Configuration aConf) throws IOException {
		KeyProvider keyprovider = null ;
		if (aURI != null) {
			if (aURI.getScheme().equalsIgnoreCase(SAMPLE_KEY_PROVIDER_SCHEMA_NAME)) {
				keyprovider = new SampleKeyProvider(aConf) ;
			}
		}
		return keyprovider ;
	}

}
