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

package org.apache.ranger.plugin.store.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FileStoreUtil {
	private static final Log LOG = LogFactory.getLog(FileStoreUtil.class);

	private Gson   gsonBuilder = null;
	private String dataDir     = null;

	private static final String FILE_SUFFIX_JSON        = ".json";

	public void initStore(String dataDir) {
		this.dataDir = dataDir;

		try {
			gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
		} catch(Throwable excp) {
			LOG.fatal("FileStoreUtil.init(): failed to create GsonBuilder object", excp);
		}
	}
	
	public String getDataDir() {
		return dataDir;
	}

	public String getDataFile(String filePrefix, Long id) {
		String filePath = dataDir + Path.SEPARATOR + filePrefix + id + FILE_SUFFIX_JSON;

		return filePath;
	}

	public String getDataFile(String filePrefix, Long parentId, Long objectId) {
		String filePath = dataDir + Path.SEPARATOR + filePrefix + parentId + "-" + objectId + FILE_SUFFIX_JSON;

		return filePath;
	}

	public <T> T loadFromResource(String resource, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.loadFromResource(" + resource + ")");
		}

		InputStream inStream = this.getClass().getResourceAsStream(resource);

		T ret = loadFromStream(inStream, cls);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.loadFromResource(" + resource + "): " + ret);
		}

		return ret;
	}

	public <T> T loadFromStream(InputStream inStream, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.loadFromStream()");
		}

		InputStreamReader reader = new InputStreamReader(inStream);

		T ret = gsonBuilder.fromJson(reader, cls);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.loadFromStream(): " + ret);
		}

		return ret;
	}

	public <T> T loadFromFile(Path filePath, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.loadFromFile(" + filePath + ")");
		}

		T                 ret    = null;
		InputStreamReader reader = null;

		try {
			FileSystem        fileSystem = getFileSystem(filePath);
			FSDataInputStream inStream   = fileSystem.open(filePath);

			ret = loadFromStream(inStream, cls);
		} finally {
			close(reader);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.loadFromFile(" + filePath + "): " + ret);
		}

		return ret;
	}

	public <T> List<T> loadFromDir(Path dirPath, final String filePrefix, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.loadFromDir()");
		}

		List<T> ret = new ArrayList<T>();

		try {
			FileSystem fileSystem = getFileSystem(dirPath);

			if(fileSystem.exists(dirPath) && fileSystem.isDirectory(dirPath)) {
				PathFilter filter = new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return path.getName().startsWith(filePrefix) &&
							   path.getName().endsWith(FILE_SUFFIX_JSON);
					}
				};

				FileStatus[] sdFiles = fileSystem.listStatus(dirPath, filter);

				if(sdFiles != null) {
					for(FileStatus sdFile : sdFiles) {
						T obj = loadFromFile(sdFile.getPath(), cls);

						if(obj != null) {
							ret.add(obj);
						}
					}
				}
			} else {
				LOG.error(dirPath + ": does not exists or not a directory");
			}
		} catch(IOException excp) {
			LOG.warn("error loading service-def in directory " + dirPath, excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.loadFromDir(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	public <T> T saveToFile(T obj, Path filePath, boolean overWrite) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.saveToFile(" + filePath + ")");
		}

		OutputStreamWriter writer = null;

		try {
			FileSystem         fileSystem = getFileSystem(filePath);
			FSDataOutputStream outStream  = fileSystem.create(filePath, overWrite);

			writer = new OutputStreamWriter(outStream);

			gsonBuilder.toJson(obj, writer);
		} finally {
			close(writer);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.saveToFile(" + filePath + "): " + obj);
		}

		return obj;
	}

	public boolean deleteFile(Path filePath) throws Exception {
		LOG.debug("==> FileStoreUtil.deleteFile(" + filePath + ")");

		FileSystem fileSystem = getFileSystem(filePath);

		boolean ret = false;

		if(fileSystem.exists(filePath)) {
			ret = fileSystem.delete(filePath, false);
		} else {
			ret = true; // nothing to delete
		}

		LOG.debug("<== FileStoreUtil.deleteFile(" + filePath + "): " + ret);

		return ret;
	}

	public boolean renamePath(Path oldPath, Path newPath) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.renamePath(" + oldPath + "," + newPath + ")");
		}

		FileSystem fileSystem = getFileSystem(oldPath);

		boolean ret = false;

		if(fileSystem.exists(oldPath)) {
			if(! fileSystem.exists(newPath)) {
				ret = fileSystem.rename(oldPath, newPath);
			} else {
				LOG.warn("target of rename '" + newPath + "' already exists");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.renamePath(" + oldPath + "," + newPath + "): " + ret);
		}

		return ret;
	}

	public RangerServiceDef saveToFile(RangerServiceDef serviceDef, String filePrefix, boolean overWrite) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> FileStoreUtil.saveToFile(" + serviceDef + "," + overWrite + ")");
		}

		Path filePath = new Path(getDataFile(filePrefix, serviceDef.getId()));

		RangerServiceDef ret = saveToFile(serviceDef, filePath, overWrite);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== FileStoreUtil.saveToFile(" + serviceDef + "," + overWrite + "): ");
		}

		return ret;
	}

	public RangerService saveToFile(RangerService service, String filePrefix,boolean overWrite) throws Exception {
		Path filePath = new Path(getDataFile(filePrefix, service.getId()));

		RangerService ret = saveToFile(service, filePath, overWrite);
		
		return ret;
	}

	public RangerPolicy saveToFile(RangerPolicy policy, String filePrefix, long serviceId, boolean overWrite) throws Exception {
		Path filePath = new Path(getDataFile(filePrefix, serviceId, policy.getId()));

		RangerPolicy ret = saveToFile(policy, filePath, overWrite);

		return ret;
	}

	public FileSystem getFileSystem(Path filePath) throws Exception {
		Configuration conf        = new Configuration();
		FileSystem    fileSystem  = filePath.getFileSystem(conf);
		
		return fileSystem;
	}

	protected void close(FileSystem fs) {
		if(fs != null) {
			try {
				fs.close();
			} catch(IOException excp) {
				// ignore
			}
		}
	}

	protected void close(InputStreamReader reader) {
		if(reader != null) {
			try {
				reader.close();
			} catch(IOException excp) {
				// ignore
			}
		}
	}

	protected void close(OutputStreamWriter writer) {
		if(writer != null) {
			try {
				writer.close();
			} catch(IOException excp) {
				// ignore
			}
		}
	}
}
