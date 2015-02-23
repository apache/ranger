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
import java.util.List;

import org.apache.hadoop.conf.Configuration;


public class SampleKeyProvider extends KeyProvider {
	
	public SampleKeyProvider(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public KeyVersion getKeyVersion(String versionName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getKeys() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<KeyVersion> getKeyVersions(String name) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Metadata getMetadata(String name) throws IOException {
		// TODO Auto-generated method stub
		
		new KeyProvider.Metadata(null) ;
		
		return null;
	}

	@Override
	public KeyVersion createKey(String name, byte[] material, Options options)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteKey(String name) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public KeyVersion rollNewVersion(String name, byte[] material)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	

}
