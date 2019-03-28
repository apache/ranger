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
package org.apache.ranger.server.tomcat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.util.XMLUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import com.google.protobuf.TextFormat.ParseException;

public class SolrCollectionBoostrapper extends Thread {

	private static final Logger logger = Logger
			.getLogger(SolrCollectionBoostrapper.class.getName());
	final static String SOLR_ZK_HOSTS = "ranger.audit.solr.zookeepers";
	final static String SOLR_COLLECTION_NAME = "ranger.audit.solr.collection.name";
	final static String SOLR_CONFIG_NAME = "ranger.audit.solr.config.name";
	final static String SOLR_NO_SHARDS = "ranger.audit.solr.no.shards";
	final static String SOLR_MAX_SHARD_PER_NODE = "ranger.audit.solr.max.shards.per.node";
	final static String SOLR_NO_REPLICA = "ranger.audit.solr.no.replica";
	final static String SOLR_TIME_INTERVAL = "ranger.audit.solr.time.interval";
	final static String SOLR_ACL_USER_LIST_SASL = "ranger.audit.solr.acl.user.list.sasl";
	final static String PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
	public static final String DEFAULT_COLLECTION_NAME = "ranger_audits";
	public static final String DEFAULT_CONFIG_NAME = "ranger_audits";
	public static final String DEFAULT_SERVICE_NAME = "rangeradmin";
	public static final long DEFAULT_SOLR_TIME_INTERVAL_MS = 60000L;
	private static final String CONFIG_FILE = "ranger-admin-site.xml";
	private static final String CORE_SITE_CONFIG_FILENAME = "core-site.xml";
	private static final String DEFAULT_CONFIG_FILENAME = "ranger-admin-default-site.xml";
	private static final String AUTH_TYPE_KERBEROS = "kerberos";
	private static final String AUTHENTICATION_TYPE = "hadoop.security.authentication";
	private static final String RANGER_SERVICE_HOSTNAME = "ranger.service.host";
	private static final String ADMIN_USER_PRINCIPAL = "ranger.admin.kerberos.principal";
	private static final String SOLR_CONFIG_FILE = "solrconfig.xml";
	private static final String[] configFiles = { "admin-extra.html",
			"admin-extra.menu-bottom.html", "admin-extra.menu-top.html",
			"elevate.xml", "enumsConfig.xml", "managed-schema",
			"solrconfig.xml" };
	private File configSetFolder = null;
	private boolean hasEnumConfig;
	boolean solr_cloud_mode = false;
	boolean is_completed = false;
	boolean isKERBEROS = false;
	String principal = null;
	String hostName;
	String keytab;
	String nameRules;
	String solr_collection_name;
	String solr_config_name;
	Path path_for_cloud_mode;
	int no_of_replicas;
	int no_of_shards;
	int max_node_per_shards;
	Long time_interval;
	SolrClient solrClient = null;
	CloudSolrClient solrCloudClient = null;
	SolrZooKeeper solrZookeeper = null;
	SolrZkClient zkClient = null;

	private Properties serverConfigProperties = new Properties();

	public SolrCollectionBoostrapper() throws IOException {
		logger.info("Starting Solr Setup");
		XMLUtils.loadConfig(DEFAULT_CONFIG_FILENAME, serverConfigProperties);
		XMLUtils.loadConfig(CORE_SITE_CONFIG_FILENAME, serverConfigProperties);
		XMLUtils.loadConfig(CONFIG_FILE, serverConfigProperties);

		logger.info("AUTHENTICATION_TYPE : " + getConfig(AUTHENTICATION_TYPE));
		if (getConfig(AUTHENTICATION_TYPE) != null
				&& getConfig(AUTHENTICATION_TYPE).trim().equalsIgnoreCase(
						AUTH_TYPE_KERBEROS)) {
			isKERBEROS = true;
			hostName = getConfig(RANGER_SERVICE_HOSTNAME);
			try {
				principal = SecureClientLogin.getPrincipal(
						getConfig(ADMIN_USER_PRINCIPAL), hostName);
			} catch (IOException ignored) {
				logger.warning("Failed to get ranger.admin.kerberos.principal. Reason: "
						+ ignored.toString());
			}
		}

		solr_collection_name = getConfig(SOLR_COLLECTION_NAME,
				DEFAULT_COLLECTION_NAME);
		logger.info("Solr Collection name provided is : "
				+ solr_collection_name);
		solr_config_name = getConfig(SOLR_CONFIG_NAME, DEFAULT_CONFIG_NAME);
		logger.info("Solr Config name provided is : " + solr_config_name);
		no_of_replicas = getIntConfig(SOLR_NO_REPLICA, 1);
		logger.info("No. of replicas provided is : " + no_of_replicas);

		no_of_shards = getIntConfig(SOLR_NO_SHARDS, 1);
		logger.info("No. of shards provided is : " + no_of_shards);
		max_node_per_shards = getIntConfig(SOLR_MAX_SHARD_PER_NODE, 1);
		logger.info("Max no of nodes per shards provided is : "
				+ max_node_per_shards);

		time_interval = getLongConfig(SOLR_TIME_INTERVAL,
				DEFAULT_SOLR_TIME_INTERVAL_MS);
		logger.info("Solr time interval provided is : " + time_interval);
		if (System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null) {
			System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG,
					"/dev/null");
		}

		String basedir = new File(".").getCanonicalPath();
		String solrFileDir = new File(basedir).getParent();

		path_for_cloud_mode = Paths.get(solrFileDir, "contrib",
				"solr_for_audit_setup", "conf");
		configSetFolder = path_for_cloud_mode.toFile();
	}

	public void run() {
		logger.info("Started run method");

		String zkHosts = "";
		List<String> zookeeperHosts = null;
		if (getConfig(SOLR_ZK_HOSTS) != null
				&& !StringUtil.isEmpty(getConfig(SOLR_ZK_HOSTS))) {
			zkHosts = getConfig(SOLR_ZK_HOSTS).trim();
			zookeeperHosts = new ArrayList<String>(Arrays.asList(zkHosts
					.split(",")));
		}
		if (zookeeperHosts != null
				&& !zookeeperHosts.isEmpty()
				&& zookeeperHosts.stream().noneMatch(
						h -> h.equalsIgnoreCase("none"))) {
			logger.info("Solr zkHosts=" + zkHosts + ", collectionName="
					+ solr_collection_name);
			while (!is_completed) {
				try {
					if (connect(zookeeperHosts)) {
						if (solr_cloud_mode) {
							if (uploadConfiguration() && createCollection()
									&& setupACL(zkClient)) {
								is_completed = true;
								break;
							} else {
								logErrorMessageAndWait(
										"Error while performing operations on solr. ",
										null);
							}
						}

					} else {
						logErrorMessageAndWait(
								"Cannot connect to solr kindly check the solr related configs. ",
								null);
					}
				} catch (Exception ex) {
					logErrorMessageAndWait("Error while configuring solr. ", ex);
				}

			}

		} else {
			logger.severe("Solr ZKHosts for Audit are empty. Please set property "
					+ SOLR_ZK_HOSTS);
		}

	}

	private boolean connect(List<String> zookeeperHosts) {
		try {
			logger.info("Solr is in Cloud mode");
			if (isKERBEROS) {
				setHttpClientBuilderForKrb();
			}
			solrCloudClient = new CloudSolrClient.Builder(zookeeperHosts,
					Optional.empty()).build();
			solrCloudClient.setDefaultCollection(solr_collection_name);
			solrClient = solrCloudClient;
			solr_cloud_mode = true;

			return true;
		} catch (Exception ex) {
			logger.severe("Can't connect to Solr server. ZooKeepers="
					+ zookeeperHosts + ", collection=" + solr_collection_name
					+ ex);
			return false;
		}
	}

	private void setHttpClientBuilderForKrb() {
		Krb5HttpClientBuilder krbBuild = new Krb5HttpClientBuilder();
		SolrHttpClientBuilder kb = krbBuild.getBuilder();
		HttpClientUtil.setHttpClientBuilder(kb);
	}

	private boolean uploadConfiguration() {
		try {
			Path path = Paths.get(System.getProperty("java.io.tmpdir"), UUID
					.randomUUID().toString(), "RangerAudit");

			File tmpDir = path.toFile();

			solrCloudClient.connect();
			zkClient = solrCloudClient.getZkStateReader().getZkClient();

			if (zkClient != null) {
				ZkConfigManager zkConfigManager = new ZkConfigManager(zkClient);

				boolean configExists = zkConfigManager
						.configExists(solr_config_name);
				if (!configExists) {
					logger.info("Config does not exist with name "
							+ solr_config_name);
					doIfConfigNotExist(solr_config_name, zkConfigManager);
					uploadMissingConfigFiles(zkClient, zkConfigManager,
							solr_config_name);
					return true;
				} else {
					logger.info("Config exist with name " + solr_config_name);
					doIfConfigExists(solr_config_name, zkClient, path, tmpDir);
					return true;
				}
			} else {
				logger.severe("Solr is in cloud mode and could not find the zookeeper client for performing upload operations. ");
				return false;
			}
		} catch (Exception ex) {
			logger.severe("Error while uploading configuration : " + ex);
			return false;
		}

	}

	private void doIfConfigNotExist(String configName,
			ZkConfigManager zkConfigManager) throws IOException {
		File[] listOfFiles = getConfigSetFolder().listFiles();
		if (listOfFiles != null) {
			zkConfigManager.uploadConfigDir(getConfigSetFolder().toPath(),
					configName);
		}
	}

	private void doIfConfigExists(String solrConfigName, SolrZkClient zkClient,
			Path path, File tmpDir) throws IOException {

		logger.info("Config set exists for " + solr_collection_name
				+ " collection. Refreshing it if needed...");
		if (!tmpDir.mkdirs()) {
			logger.severe("Cannot create directories for"
					+ tmpDir.getAbsolutePath());
		}
		ZkConfigManager zkConfigManager = new ZkConfigManager(zkClient);
		zkConfigManager.downloadConfigDir(solrConfigName, path);
		File[] listOfFiles = getConfigSetFolder().listFiles();
		if (listOfFiles != null) {
			for (File file : listOfFiles) {
				if (file.getName().equals(getConfigFileName())
						&& updateConfigIfNeeded(solrConfigName, zkClient, file,
								path)) {
					break;
				}
			}
		}
	}

	private void uploadMissingConfigFiles(SolrZkClient zkClient,
			ZkConfigManager zkConfigManager, String configName)
			throws IOException {
		logger.info("Check any of the configs files are missing for config"
				+ configName);

		for (String configFile : configFiles) {
			if ("enumsConfig.xml".equals(configFile) && !hasEnumConfig) {
				logger.info("Config file " + configFile + " is not needed for "
						+ configName);
				continue;
			}
			Path zkPath = Paths.get(configName, configFile);
			if (zkConfigManager.configExists(zkPath.toString())) {
				logger.info("Config file " + configFile
						+ " has already uploaded properly.");
			} else {
				logger.info("Config file " + configFile
						+ " is missing. Reupload...");
				FileSystems.getDefault().getSeparator();
				uploadFileToZk(zkClient,
						Paths.get(getConfigSetFolder().toString(), configFile),
						Paths.get("/configs/", zkPath.toString()));

			}
		}
	}

	private boolean updateConfigIfNeeded(String solrConfigName,
			SolrZkClient zkClient, File file, Path path) throws IOException {
		boolean result = false;
		if (!FileUtils.contentEquals(file,
				Paths.get(path.toString(), file.getName()).toFile())) {
			logger.info("Solr config file differs " + file.getName()
					+ " upload config set to zookeeper");
			Path filePath = Paths.get(getConfigSetFolder().toString(),
					getConfigFileName());
			Path configsPath = Paths.get("/configs", solrConfigName,
					getConfigFileName());
			uploadFileToZk(zkClient, filePath, configsPath);
			result = true;
		} else {
			logger.info("Content is same of SolrConfig file");
		}
		return result;
	}

	private void logErrorMessageAndWait(String msg, Exception exception) {
		if (exception != null) {
			logger.severe(msg + " [retrying after " + time_interval
					+ " ms]. Error : " + exception);
		} else {
			logger.severe(msg + " [retrying after " + time_interval + " ms]");
		}

		try {
			Thread.sleep(time_interval);
		} catch (InterruptedException ex) {
			logger.info("sleep interrupted: " + ex.getMessage());
		}
	}

	private boolean createCollection() {
		try {
			List<String> allCollectionList = getCollections();
			if (allCollectionList != null) {
				if (!allCollectionList.contains(solr_collection_name)) {

					CollectionAdminRequest.Create createCollection = CollectionAdminRequest
							.createCollection(solr_collection_name,
									solr_config_name, no_of_shards,
									no_of_replicas);
					createCollection.setMaxShardsPerNode(max_node_per_shards);
					CollectionAdminResponse createResponse = createCollection
							.process(solrClient);
					if (createResponse.getStatus() != 0) {
						logger.severe("Error creating collection. collectionName="
								+ solr_collection_name
								+ " , solr config name = "
								+ solr_config_name
								+ " , replicas = "
								+ no_of_replicas
								+ ", shards="
								+ no_of_shards
								+ " , max node per shards = "
								+ max_node_per_shards
								+ ", response="
								+ createResponse);
						return false;
					} else {
						logger.info("Created collection "
								+ solr_collection_name + " with config name "
								+ solr_config_name + " replicas =  "
								+ no_of_replicas + " Shards = " + no_of_shards
								+ " max node per shards  = "
								+ max_node_per_shards);
						return true;
					}
				} else {
					logger.info("Collection already exists with name "
							+ solr_collection_name);
					return true;
				}
			} else {
				logger.severe("Error while connecting to solr ");
				return false;
			}
		} catch (Exception ex) {
			logger.severe("Error while creating collection in solr : " + ex);
			return false;
		}
	}

	private String getConfig(String key, String defaultValue) {
		String ret = getConfig(key);
		if (ret == null) {
			ret = defaultValue;
		}
		return ret;
	}

	private boolean setupACL(SolrZkClient zkClient) {
		solrZookeeper = zkClient.getSolrZooKeeper();
		String serviceName = "";
		List<String> aclUserList = null;
		Path zNodeConfigPath = Paths.get("/configs", solr_config_name);
		Path zNodeCollectionPath = Paths.get("/collections",
				solr_collection_name);
		if (isKERBEROS) {
			if (principal != null && !StringUtil.isEmpty(principal)) {
				serviceName = principal.substring(0, principal.indexOf("/"));
			} else {
				serviceName = DEFAULT_SERVICE_NAME;
			}
			aclUserList = new ArrayList<String>();
			if (getConfig(SOLR_ACL_USER_LIST_SASL) != null
					&& !StringUtil.isEmpty(SOLR_ACL_USER_LIST_SASL)) {
				aclUserList = Arrays.asList(getConfig(SOLR_ACL_USER_LIST_SASL)
						.trim().split(","));
			}
		}
		if (performACLOnZnode(zNodeConfigPath, solrZookeeper, serviceName,
				aclUserList)
				&& performACLOnZnode(zNodeCollectionPath, solrZookeeper,
						serviceName, aclUserList)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * In case of Kerberize env scheme "world" users should have only READ
	 * access and scheme "sasl" users e.g. infra-solr, solr, rangeradmin should
	 * have all access on Znode. In case of simple env scheme "world" users
	 * should have all access on Znode.
	 */
	private boolean performACLOnZnode(Path zookeeperNodePath,
			SolrZooKeeper solrZookeeper, String serviceName,
			List<String> aclUserList) {
		List<ACL> aclListForZnodePath = new ArrayList<ACL>();
		try {

			if (isKERBEROS) {
				Stat stat = solrZookeeper.exists(zookeeperNodePath.toString(),
						false);

				List<ACL> existingACLOnZnodePath = solrZookeeper.getACL(
						zookeeperNodePath.toString(), stat);
				if (existingACLOnZnodePath != null
						&& existingACLOnZnodePath.size() > 0) {
					List<String> existingIdForZnodePath = new ArrayList<String>();
					int permissionForWorldScheme = 0;
					for (ACL acl : existingACLOnZnodePath) {
						existingIdForZnodePath.add(acl.getId().getId());
						if (acl.getId().getId().equalsIgnoreCase("anyone")) {
							permissionForWorldScheme = acl.getPerms();
						}

					}

					if (aclUserList != null && !aclUserList.isEmpty()) {
						for (String aclUser : aclUserList) {
							if (!existingIdForZnodePath.contains(aclUser)) {
								aclListForZnodePath.add(new ACL(
										ZooDefs.Perms.ALL, new Id("sasl",
												aclUser)));
							}
						}
					}
					if (!existingIdForZnodePath.contains(serviceName)) {
						aclListForZnodePath.add(new ACL(ZooDefs.Perms.ALL,
								new Id("sasl", serviceName)));
					}
					if (!existingIdForZnodePath.contains("anyone")
							|| (existingACLOnZnodePath.contains("anyone") && permissionForWorldScheme != ZooDefs.Perms.READ)) {
						aclListForZnodePath.add(new ACL(ZooDefs.Perms.READ,
								new Id("world", "anyone")));
					}
				} else {
					aclListForZnodePath.add(new ACL(ZooDefs.Perms.READ, new Id(
							"world", "anyone")));
					aclListForZnodePath.add(new ACL(ZooDefs.Perms.ALL, new Id(
							"sasl", serviceName)));
					for (String aclUser : aclUserList) {
						aclListForZnodePath.add(new ACL(ZooDefs.Perms.ALL,
								new Id("sasl", aclUser)));
					}
				}

			} else {
				aclListForZnodePath.add(new ACL(ZooDefs.Perms.ALL, new Id(
						"world", "anyone")));
			}

			if (aclListForZnodePath != null && !aclListForZnodePath.isEmpty()) {
				solrZookeeper.setACL(zookeeperNodePath.toString(),
						aclListForZnodePath, -1);
			}

		} catch (Exception ex) {
			logger.severe("Error while performing ACL on zNode : "
					+ zookeeperNodePath.toString() + " Error: " + ex);
			return false;
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private List<String> getCollections() throws IOException, ParseException {
		try {
			CollectionAdminRequest.List colListReq = new CollectionAdminRequest.List();
			CollectionAdminResponse response = colListReq.process(solrClient);
			if (response.getStatus() != 0) {
				logger.severe("Error getting collection list from solr.  response="
						+ response);
				return null;
			}
			return (List<String>) response.getResponse().get("collections");
		} catch (SolrException e) {
			logger.severe("getCollections() operation failed : " + e);
			return null;
		} catch (SolrServerException e) {
			logger.severe("getCollections() operation failed : " + e);
			return null;
		}

	}

	private int getIntConfig(String key, int defaultValue) {
		int ret = defaultValue;
		String retStr = getConfig(key);
		try {
			if (retStr != null) {
				ret = Integer.parseInt(retStr);
			}
		} catch (Exception err) {
			logger.severe(retStr + " can't be parsed to int. Reason: "
					+ err.toString());
		}
		return ret;
	}

	private Long getLongConfig(String key, Long defaultValue) {
		Long ret = defaultValue;
		String retStr = getConfig(key);
		try {
			if (retStr != null) {
				ret = Long.parseLong(retStr);
			}
		} catch (Exception err) {
			logger.severe(retStr + " can't be parsed to long. Reason: "
					+ err.toString());
		}
		return ret;
	}

	private String getConfig(String key) {

		String value = serverConfigProperties.getProperty(key);
		if (value == null || value.trim().isEmpty()) {
			// Value not found in properties file, let's try to get from
			// System's property
			value = System.getProperty(key);
		}
		return value;
	}

	private void uploadFileToZk(SolrZkClient zkClient, Path filePath,
			Path configsPath) throws FileNotFoundException {
		InputStream is = new FileInputStream(filePath.toString());
		try {
			if (zkClient.exists(configsPath.toString(), true)) {
				zkClient.setData(configsPath.toString(),
						IOUtils.toByteArray(is), true);
			} else {
				zkClient.create(configsPath.toString(),
						IOUtils.toByteArray(is), CreateMode.PERSISTENT, true);
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			IOUtils.closeQuietly(is);
		}
	}

	private File getConfigSetFolder() {
		return configSetFolder;
	}

	private String getConfigFileName() {
		return SOLR_CONFIG_FILE;
	}
}