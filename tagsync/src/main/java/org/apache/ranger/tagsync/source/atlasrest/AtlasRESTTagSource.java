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

package org.apache.ranger.tagsync.source.atlasrest;

import com.google.gson.Gson;

import com.google.gson.GsonBuilder;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
import org.apache.ranger.tagsync.source.atlas.AtlasNotificationMapper;
import org.apache.ranger.tagsync.source.atlas.AtlasResourceMapperUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class AtlasRESTTagSource extends AbstractTagSource implements Runnable {
	private static final Log LOG = LogFactory.getLog(AtlasRESTTagSource.class);

	private long     sleepTimeBetweenCycleInMillis;
	private String[] restUrls         = null;
	private boolean  isKerberized     = false;
	private String[] userNamePassword = null;
	private Thread   myThread         = null;

	public static void main(String[] args) {
		TagSyncConfig config  = TagSyncConfig.getInstance();
		Properties    props   = config.getProperties();
		TagSink       tagSink = TagSynchronizer.initializeTagSink(props);

		TagSynchronizer.printConfigurationProperties(props);

		if (tagSink != null) {
			AtlasRESTTagSource atlasRESTTagSource = new AtlasRESTTagSource();

			if (atlasRESTTagSource.initialize(props)) {
				try {
					tagSink.start();
					atlasRESTTagSource.setTagSink(tagSink);
					atlasRESTTagSource.synchUp();
				} catch (Exception exception) {
					LOG.error("ServiceTags upload failed : ", exception);
					System.exit(1);
				}
			} else {
				LOG.error("AtlasRESTTagSource initialized failed, exiting.");
				System.exit(1);
			}
		} else {
			LOG.error("TagSink initialialization failed, exiting.");
			System.exit(1);
		}
	}

	@Override
	public boolean initialize(Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasRESTTagSource.initialize()");
		}

		sleepTimeBetweenCycleInMillis = TagSyncConfig.getTagSourceAtlasDownloadIntervalInMillis(properties);

		boolean ret = AtlasResourceMapperUtil.initializeAtlasResourceMappers(properties);

		String  sslConfigFile = TagSyncConfig.getAtlasRESTSslConfigFile(properties);

		this.isKerberized     = TagSyncConfig.getTagsyncKerberosIdentity(properties) != null;
		this.userNamePassword = new String[] { TagSyncConfig.getAtlasRESTUserName(properties), TagSyncConfig.getAtlasRESTPassword(properties) };

		String restEndpoint = TagSyncConfig.getAtlasRESTEndpoint(properties);

		if (LOG.isDebugEnabled()) {
			LOG.debug("restEndpoint=" + restEndpoint);
			LOG.debug("sslConfigFile=" + sslConfigFile);
			LOG.debug("userName=" + userNamePassword[0]);
			LOG.debug("kerberized=" + isKerberized);
		}

		if (StringUtils.isNotEmpty(restEndpoint)) {
			this.restUrls = restEndpoint.split(",");

			for (int i = 0; i < restUrls.length; i++) {
				if (!restUrls[i].endsWith("/")) {
					restUrls[i] += "/";
				}
			}

		} else {
			LOG.info("AtlasEndpoint not specified, Initial download of Atlas-entities cannot be done.");
			ret = false;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasRESTTagSource.initialize(), result=" + ret);
		}

		return ret;
	}

	@Override
	public boolean start() {
		myThread = new Thread(this);
		myThread.setDaemon(true);
		myThread.start();

		return true;
	}

	@Override
	public void stop() {
		if (myThread != null && myThread.isAlive()) {
			myThread.interrupt();
		}
	}

	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasRESTTagSource.run()");
		}

		while (true) {
			synchUp();

			LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");

			try {
				Thread.sleep(sleepTimeBetweenCycleInMillis);
			} catch (InterruptedException exception) {
				LOG.error("Interrupted..: ", exception);
				return;
			}
		}
	}

	public void synchUp() {
		SearchParameters           searchParams = new SearchParameters();
		AtlasTypeRegistry          typeRegistry = new AtlasTypeRegistry();
		AtlasTransientTypeRegistry tty          = null;
		AtlasSearchResult          searchResult = null;

		searchParams.setClassification("*");
		searchParams.setIncludeClassificationAttributes(true);
		searchParams.setOffset(0);
		searchParams.setLimit(Integer.MAX_VALUE);

		try {
			AtlasClientV2 atlasClient = getAtlasClient();

			searchResult = atlasClient.facetedSearch(searchParams);

			AtlasTypesDef typesDef = atlasClient.getAllTypeDefs(new SearchFilter());

			tty = typeRegistry.lockTypeRegistryForUpdate();

			tty.addTypes(typesDef);
		} catch (AtlasServiceException | AtlasBaseException | IOException excp) {
			LOG.error("failed to download tags from Atlas", excp);
		} finally {
			if (tty != null) {
				typeRegistry.releaseTypeRegistryForUpdate(tty, true);
			}
		}

		if (searchResult != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(AtlasType.toJson(searchResult));
			}

			Map<String, ServiceTags> serviceTagsMap = AtlasNotificationMapper.processSearchResult(searchResult, typeRegistry);

			if (MapUtils.isNotEmpty(serviceTagsMap)) {
				for (Map.Entry<String, ServiceTags> entry : serviceTagsMap.entrySet()) {
					if (LOG.isDebugEnabled()) {
						Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
								.setPrettyPrinting()
								.create();
						String serviceTagsString = gsonBuilder.toJson(entry.getValue());

						LOG.debug("serviceTags=" + serviceTagsString);
					}

					updateSink(entry.getValue());
				}
			}
		}

	}

	private AtlasClientV2 getAtlasClient() throws IOException {
		final AtlasClientV2 ret;

		if (isKerberized) {
			UserGroupInformation ugi = UserGroupInformation.getLoginUser();

			ugi.checkTGTAndReloginFromKeytab();

			ret = new AtlasClientV2(ugi, ugi.getShortUserName(), restUrls);
		} else {
			ret = new AtlasClientV2(restUrls, userNamePassword);
		}

		return ret;
	}
}

