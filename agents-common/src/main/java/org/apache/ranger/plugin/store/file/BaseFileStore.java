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
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.AbstractServiceStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class BaseFileStore extends AbstractServiceStore {
	private static final Log LOG = LogFactory.getLog(BaseFileStore.class);

	private Gson   gsonBuilder = null;
	private String dataDir     = null;

	protected static final String FILE_PREFIX_SERVICE_DEF = "ranger-servicedef-";
	protected static final String FILE_PREFIX_SERVICE     = "ranger-service-";
	protected static final String FILE_PREFIX_POLICY      = "ranger-policy-";
	protected static final String FILE_SUFFIX_JSON        = ".json";


	protected void initStore(String dataDir) {
		this.dataDir = dataDir;

		try {
			gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
		} catch(Throwable excp) {
			LOG.fatal("BaseFileStore.init(): failed to create GsonBuilder object", excp);
		}
	}
	
	protected String getDataDir() {
		return dataDir;
	}

	protected String getServiceDefFile(Long id) {
		String filePath = dataDir + Path.SEPARATOR + FILE_PREFIX_SERVICE_DEF + id + FILE_SUFFIX_JSON;

		return filePath;
	}

	protected String getServiceFile(Long id) {
		String filePath = dataDir + Path.SEPARATOR + FILE_PREFIX_SERVICE + id + FILE_SUFFIX_JSON;

		return filePath;
	}

	protected String getPolicyFile(Long serviceId, Long policyId) {
		String filePath = dataDir + Path.SEPARATOR + FILE_PREFIX_POLICY + serviceId + "-" + policyId + FILE_SUFFIX_JSON;

		return filePath;
	}

	protected <T> T loadFromResource(String resource, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.loadFromResource(" + resource + ")");
		}

		InputStream inStream = this.getClass().getResourceAsStream(resource);

		T ret = loadFromStream(inStream, cls);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== BaseFileStore.loadFromResource(" + resource + "): " + ret);
		}

		return ret;
	}

	protected <T> T loadFromStream(InputStream inStream, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.loadFromStream()");
		}

		InputStreamReader reader = new InputStreamReader(inStream);

		T ret = gsonBuilder.fromJson(reader, cls);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== BaseFileStore.loadFromStream(): " + ret);
		}

		return ret;
	}

	protected <T> T loadFromFile(Path filePath, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.loadFromFile(" + filePath + ")");
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
			LOG.debug("<== BaseFileStore.loadFromFile(" + filePath + "): " + ret);
		}

		return ret;
	}

	protected <T> List<T> loadFromDir(Path dirPath, final String filePrefix, Class<T> cls) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.loadFromDir()");
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
			LOG.debug("<== BaseFileStore.loadFromDir(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	protected <T> T saveToFile(T obj, Path filePath, boolean overWrite) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.saveToFile(" + filePath + ")");
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
			LOG.debug("<== BaseFileStore.saveToFile(" + filePath + "): " + obj);
		}

		return obj;
	}

	protected boolean deleteFile(Path filePath) throws Exception {
		LOG.debug("==> BaseFileStore.deleteFile(" + filePath + ")");

		FileSystem fileSystem = getFileSystem(filePath);

		boolean ret = false;

		if(fileSystem.exists(filePath)) {
			ret = fileSystem.delete(filePath, false);
		} else {
			ret = true; // nothing to delete
		}

		LOG.debug("<== BaseFileStore.deleteFile(" + filePath + "): " + ret);

		return ret;
	}

	protected boolean renamePath(Path oldPath, Path newPath) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.renamePath(" + oldPath + "," + newPath + ")");
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
			LOG.debug("<== BaseFileStore.renamePath(" + oldPath + "," + newPath + "): " + ret);
		}

		return ret;
	}

	protected RangerServiceDef saveToFile(RangerServiceDef serviceDef, boolean overWrite) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> BaseFileStore.saveToFile(" + serviceDef + "," + overWrite + ")");
		}

		Path filePath = new Path(getServiceDefFile(serviceDef.getId()));

		RangerServiceDef ret = saveToFile(serviceDef, filePath, overWrite);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== BaseFileStore.saveToFile(" + serviceDef + "," + overWrite + "): ");
		}

		return ret;
	}

	protected RangerService saveToFile(RangerService service, boolean overWrite) throws Exception {
		Path filePath = new Path(getServiceFile(service.getId()));

		RangerService ret = saveToFile(service, filePath, overWrite);
		
		return ret;
	}

	protected RangerPolicy saveToFile(RangerPolicy policy, long serviceId, boolean overWrite) throws Exception {
		Path filePath = new Path(getPolicyFile(serviceId, policy.getId()));

		RangerPolicy ret = saveToFile(policy, filePath, overWrite);

		return ret;
	}

	protected long getMaxId(List<? extends RangerBaseModelObject> objs) {
		long ret = -1;

		if(objs != null) {
			for(RangerBaseModelObject obj : objs) {
				if(obj.getId() > ret) {
					ret = obj.getId();
				}
			}
		}

		return ret;
	}
	protected FileSystem getFileSystem(Path filePath) throws Exception {
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

	protected void preCreate(RangerBaseModelObject obj) {
		obj.setId(new Long(0));
		obj.setGuid(UUID.randomUUID().toString());
		obj.setCreateTime(new Date());
		obj.setUpdateTime(obj.getCreateTime());
		obj.setVersion(new Long(1));
	}

	protected void preCreate(RangerService service) {
		preCreate((RangerBaseModelObject)service);

		service.setPolicyVersion(new Long(0));
		service.setPolicyUpdateTime(service.getCreateTime());
	}

	protected void postCreate(RangerBaseModelObject obj) {
		// TODO:
	}

	protected void preUpdate(RangerBaseModelObject obj) {
		if(obj.getId() == null) {
			obj.setId(new Long(0));
		}

		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}

		if(obj.getCreateTime() == null) {
			obj.setCreateTime(new Date());
		}

		Long version = obj.getVersion();
		
		if(version == null) {
			version = new Long(1);
		} else {
			version = new Long(version.longValue() + 1);
		}
		
		obj.setVersion(version);
		obj.setUpdateTime(new Date());
	}

	protected void postUpdate(RangerBaseModelObject obj) {
		// TODO:
	}

	protected void preDelete(RangerBaseModelObject obj) {
		// TODO:
	}

	protected void postDelete(RangerBaseModelObject obj) {
		// TODO:
	}
}
