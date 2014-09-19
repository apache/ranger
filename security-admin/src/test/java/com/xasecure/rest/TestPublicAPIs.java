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

 package com.xasecure.rest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.annotation.Rollback;

import com.xasecure.common.AppConstants;
import com.xasecure.common.GUIDUtil;
import com.xasecure.common.JSONUtil;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXAsset;
import com.xasecure.entity.XXGroup;
import com.xasecure.entity.XXResource;
import com.xasecure.rest.PublicAPIs;
import com.xasecure.util.BaseTest;
import com.xasecure.view.VXGroup;
import com.xasecure.view.VXLong;
import com.xasecure.view.VXPermObj;
import com.xasecure.view.VXPolicy;
import com.xasecure.view.VXRepository;
import com.xasecure.view.VXResponse;

/**
 * @author tushar
 * 
 */

/**
 * JUnit testSuite for {@link com.xasecure.rest.PublicAPIs}
 * 
 */

public class TestPublicAPIs extends BaseTest {
	static Logger logger = Logger.getLogger(TestPublicAPIs.class);

	@Autowired
	PublicAPIs publicAPIs;

	@Autowired
	XADaoManager daoManager;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	XUserREST xUserREST;

	VXRepository vXRepoHDFS;
	VXRepository vXRepoHBase;
	VXRepository vXRepoHive;
	VXRepository vXRepoKnox;
	VXRepository vXRepoStorm;

	VXPolicy vXPolicyHDFS;
	VXPolicy vXPolicyHBase;
	VXPolicy vXPolicyHive;
	VXPolicy vXPolicyKnox;
	VXPolicy vXPolicyStorm;

	@Override
	public void init() {
		super.startSession();
		super.startRequest();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		super.authenticate();
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#createRepository(com.xasecure.view.VXRepository)}
	 * .
	 */
	public void testCreateRepository() {
		vXRepoHDFS = new VXRepository();
		vXRepoHBase = new VXRepository();
		vXRepoHive = new VXRepository();
		vXRepoKnox = new VXRepository();
		vXRepoStorm = new VXRepository();

		// Create HDFS Repository
		String configHdfs = "{"
				+ "  \"username\": \"policymgr\","
				+ "  \"password\": \"policymgr\","
				+ "  \"fs.default.name\": \"hdfs://sandbox.hortonworks.com:8020\","
				+ "  \"hadoop.security.authorization\": \"true\","
				+ "  \"hadoop.security.authentication\": \"simple\","
				+ "  \"hadoop.security.auth_to_local\": \"\","
				+ "  \"dfs.datanode.kerberos.principal\": \"\","
				+ "  \"dfs.namenode.kerberos.principal\": \"\","
				+ "  \"dfs.secondary.namenode.kerberos.principal\": \"\","
				+ "  \"commonNameForCertificate\": \"\"}";

		vXRepoHDFS.setName("HDFS Repo_" + GUIDUtil.genGUI());
		vXRepoHDFS.setDescription("HDFS Repository, using JUnit");
		vXRepoHDFS.setIsActive(true);
		vXRepoHDFS.setRepositoryType("hdfs");
		vXRepoHDFS.setConfig(configHdfs);

		String preCreatedHdfsRepo = jsonUtil.writeObjectAsString(vXRepoHDFS);
		logger.info(preCreatedHdfsRepo);
		vXRepoHDFS = publicAPIs.createRepository(vXRepoHDFS);
		String postCreatedHdfsRepo = jsonUtil.writeObjectAsString(vXRepoHDFS);
		logger.info(postCreatedHdfsRepo);

		assertNotNull("Error while creating Repository for HDFS", vXRepoHDFS);
		logger.info("Create : Repo for HDFS created Successfully");

		// Create Hive Repository
		String configHive = "{"
				+ "  \"username\": \"policymgr_hive\" ,"
				+ "  \"password\": \"policymgr_hive\","
				+ "  \"jdbc.driverClassName\": \"org.apache.hive.jdbc.HiveDriver\","
				+ "  \"jdbc.url\": \"jdbc:hive2://127.0.0.1:10000/default\","
				+ "  \"commonNameForCertificate\": \"\"}";

		vXRepoHive.setName("hivedev_" + GUIDUtil.genGUI());
		vXRepoHive.setDescription("Hive Dev");
		vXRepoHive.setIsActive(true);
		vXRepoHive.setRepositoryType("Hive");
		vXRepoHive.setConfig(configHive);

		String preCreatedHiveRepo = jsonUtil.writeObjectAsString(vXRepoHive);
		logger.info(preCreatedHiveRepo);
		vXRepoHive = publicAPIs.createRepository(vXRepoHive);
		String postCreatedHiveRepo = jsonUtil.writeObjectAsString(vXRepoHive);
		logger.info(postCreatedHiveRepo);

		assertNotNull("Error while creating Repository for Hive", vXRepoHive);
		logger.info("Create : Repo for Hive created Successfully");

		// Create HBase Repository
		String configHbase = "{"
				+ "  \"username\": \"policymgr_hbase\","
				+ "  \"password\": \"policymgr_hbase\","
				+ "  \"fs.default.name\": \"hdfs://sandbox.hortonworks.com:8020\","
				+ "  \"hadoop.security.authorization\": \"true\","
				+ "  \"hadoop.security.authentication\": \"simple\","
				+ "  \"hadoop.security.auth_to_local\": \"\","
				+ "  \"dfs.datanode.kerberos.principal\": \"\","
				+ "  \"dfs.namenode.kerberos.principal\": \"\","
				+ "  \"dfs.secondary.namenode.kerberos.principal\": \"\","
				+ "  \"hbase.master.kerberos.principal\": \"\","
				+ "  \"hbase.rpc.engine\": \"org.apache.hadoop.hbase.ipc.SecureRpcEngine\","
				+ "  \"hbase.rpc.protection\": \"PRIVACY\","
				+ "  \"hbase.security.authentication\": \"simple\","
				+ "  \"hbase.zookeeper.property.clientPort\": \"2181\","
				+ "  \"hbase.zookeeper.quorum\": \"sandbox.hortonworks.com\","
				+ "  \"zookeeper.znode.parent\": \"/hbase-unsecure\","
				+ "  \"commonNameForCertificate\": \"\"}";

		vXRepoHBase.setName("hbasedev_" + GUIDUtil.genGUI());
		vXRepoHBase.setDescription("HBase Dev");
		vXRepoHBase.setIsActive(true);
		vXRepoHBase.setRepositoryType("HBase");
		vXRepoHBase.setConfig(configHbase);

		String preCreatedHBaseRepo = jsonUtil.writeObjectAsString(vXRepoHBase);
		logger.info(preCreatedHBaseRepo);
		vXRepoHBase = publicAPIs.createRepository(vXRepoHBase);
		String postCreatedHBaseRepo = jsonUtil.writeObjectAsString(vXRepoHBase);
		logger.info(postCreatedHBaseRepo);

		assertNotNull("Error while creating Repo for HBase", vXRepoHBase);
		logger.info("Create : Repo for HBase created Successfully");

		String configKnox = "{" + "  \"username\": \"policymgr_hive\" ,"
				+ "  \"password\": \"policymgr_hive\","
				+ "  \"knox.url\": \"jdbc:hive2://127.0.0.1:10000/default\","
				+ "  \"commonNameForCertificate\": \"\"}";

		vXRepoKnox.setConfig(configKnox);
		vXRepoKnox.setName("knoxdev_" + GUIDUtil.genGUI());
		vXRepoKnox.setDescription("Knox Repo.. from JUnit");
		vXRepoKnox.setIsActive(true);
		vXRepoKnox.setRepositoryType("Knox");

		String preCreatedKnoxRepo = jsonUtil.writeObjectAsString(vXRepoKnox);
		logger.info(preCreatedKnoxRepo);
		vXRepoKnox = publicAPIs.createRepository(vXRepoKnox);
		String postCreatedKnoxRepo = jsonUtil.writeObjectAsString(vXRepoKnox);
		logger.info(postCreatedKnoxRepo);

		assertNotNull("Error while creating Repo for Knox", vXRepoKnox);
		logger.info("Create : Repo for Knox created Successfully");

		String configStorm = "{" + "  \"username\": \"policymgr_hive\" ,"
				+ "  \"password\": \"policymgr_hive\","
				+ "  \"commonNameForCertificate\": \"\"}";

		vXRepoStorm.setConfig(configStorm);
		vXRepoStorm.setName("stormdev_" + GUIDUtil.genGUI());
		vXRepoStorm.setDescription("Storm Repo.. from JUnit");
		vXRepoStorm.setIsActive(true);
		vXRepoStorm.setRepositoryType("Storm");

		String preCreatedStormRepo = jsonUtil.writeObjectAsString(vXRepoStorm);
		logger.info(preCreatedStormRepo);
		vXRepoStorm = publicAPIs.createRepository(vXRepoStorm);
		String postCreatedStormRepo = jsonUtil.writeObjectAsString(vXRepoStorm);
		logger.info(postCreatedStormRepo);

		assertNotNull("Error while creating Repo for Knox", vXRepoStorm);
		logger.info("Create : Repo for Storm created Successfully");

	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#updateRepository(com.xasecure.view.VXRepository)}
	 * .
	 */
	public void testUpdateRepository() {

		// Update HDFS Repo
		vXRepoHDFS.setName("HDFS Repo Updated_" + GUIDUtil.genGUI());
		vXRepoHDFS = publicAPIs
				.updateRepository(vXRepoHDFS, vXRepoHDFS.getId());

		assertNotNull("Error While updating Repo", vXRepoHDFS);
		logger.info("Update : Repo for HDFS updated Successfully");

		// Update HBase Repo
		vXRepoHBase.setName("HBase Repo Updated_" + GUIDUtil.genGUI());
		vXRepoHBase = publicAPIs.updateRepository(vXRepoHBase,
				vXRepoHBase.getId());

		assertNotNull("Error While updating Repo", vXRepoHBase);
		logger.info("Update : Repo for HBase updated Successfully");

		// Update HIVE Repo
		vXRepoHive.setName("Hive Repo Updated_" + GUIDUtil.genGUI());
		vXRepoHive = publicAPIs
				.updateRepository(vXRepoHive, vXRepoHive.getId());

		assertNotNull("Error While updating Repo", vXRepoHive);
		logger.info("Update : Repo for Hive updated Successfully");
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#getRepository(java.lang.Long)}.
	 */
	public void testGetRepository() {
		VXRepository vXRepositoryHDFS = publicAPIs.getRepository(vXRepoHDFS
				.getId());

		String vXRepoHDFSJson = jsonUtil.writeObjectAsString(vXRepositoryHDFS);
		logger.info(vXRepoHDFSJson);
		assertNotNull(
				"No Repository found for this Id : " + vXRepoHDFS.getId(),
				vXRepositoryHDFS);
		logger.info("Get : Repo found for this id : " + vXRepoHDFS.getId());

		VXRepository vXRepositoryHBase = publicAPIs.getRepository(vXRepoHBase
				.getId());
		assertNotNull(
				"No Repository found for this Id : " + vXRepoHBase.getId(),
				vXRepositoryHBase);
		logger.info("Get : Repo found for this id : " + vXRepoHBase.getId());

		VXRepository vXRepositoryHive = publicAPIs.getRepository(vXRepoHive
				.getId());
		assertNotNull(
				"No Repository found for this Id : " + vXRepoHive.getId(),
				vXRepositoryHive);
		logger.info("Get : Repo found for this id : " + vXRepoHive.getId());
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#deleteRepository(java.lang.Long, javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testDeleteRepository() {

		XXAsset xxAsset = daoManager.getXXAsset().findByAssetName(
				vXRepoHDFS.getName());
		MockHttpServletRequest request = super.startRequest();
		request.addParameter("force", "true");
		publicAPIs.deleteRepository(xxAsset.getId(), request);

		VXRepository deletedRepo = publicAPIs.getRepository(xxAsset.getId());

		if (deletedRepo != null && deletedRepo.getIsActive() == false) {
			logger.info("Repository has been deleted"
					+ " successfully, and DB change has been rolled back");
		} else {
			fail("Delete Repository test failed");
		}

		super.endRequest();
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#testConfig(com.xasecure.view.VXRepository)}
	 * .
	 */
	public void testTestConfig() {
		VXResponse vXResponseHDFS = publicAPIs.testConfig(vXRepoHDFS);
		assertNotNull(
				"Error while testing testConfig : for HDFS, testConfig function",
				vXResponseHDFS);

		if (vXResponseHDFS.getStatusCode() == VXResponse.STATUS_SUCCESS) {
			logger.info("testConfig : for HDFS, testConfig function has been tested and working as expected");
		} else if (vXResponseHDFS.getStatusCode() == VXResponse.STATUS_ERROR) {
			logger.info("testConfig : for HDFS, testConfig function send error response");
		}

		// NOTE : testConfig will not work on local server but to test
		// PublicREST API we need write it over here
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#searchRepositories(javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testSearchRepositories() {

	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#countRepositories(javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testCountRepositories() {
		VXLong vXLong = publicAPIs.countRepositories(super.startRequest());

		assertNotNull("Count : Error while counting Repos", vXLong);
		logger.info("Count : Total no of Repos are : " + vXLong.getValue());

		super.endRequest();
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#createPolicy(com.xasecure.view.VXPolicy)}
	 * .
	 */
	public void testCreatePolicy() {
		vXPolicyHDFS = new VXPolicy();
		vXPolicyHBase = new VXPolicy();
		vXPolicyHive = new VXPolicy();
		vXPolicyKnox = new VXPolicy();
		vXPolicyStorm = new VXPolicy();

		// Create Policy for HDFS
		createHDFSPolicy();
		// Create Policy for HBase
		createHBasePolicy();
		// Create Policy for Hive
		createHivePolicy();
		// Create Policy for Knox
		createKnoxPolicy();
		// Create Policy for Storm
		createStormPolicy();

	}

	private void createStormPolicy() {
		vXPolicyStorm.setPolicyName("HomePolicy_" + GUIDUtil.genGUI());
		vXPolicyStorm.setDescription("home policy for Storm");
		vXPolicyStorm.setRepositoryName(vXRepoStorm.getName());
		vXPolicyStorm.setRepositoryType("Storm");
		vXPolicyStorm.setIsAuditEnabled(true);
		vXPolicyStorm.setIsEnabled(true);
		vXPolicyStorm.setTopologies("topo1, topo2, topo3");

		VXPermObj vXPermObj = new VXPermObj();
		List<String> userList = new ArrayList<String>();
		userList.add("policymgr");
		vXPermObj.setUserList(userList);

		List<String> permList = new ArrayList<String>();
		permList.add("Get Nimbus Conf");
		permList.add("Get Cluster Info");
		permList.add("Rebalance");
		vXPermObj.setPermList(permList);

		VXPermObj vXPermObj2 = new VXPermObj();
		List<String> userList2 = new ArrayList<String>();
		List<String> permList2 = new ArrayList<String>();

		userList2.add("policymgr_hbase");
		userList2.add("policymgr_hive");

		permList2.add("File Download");
		permList2.add("File Upload");
		vXPermObj2.setUserList(userList2);
		vXPermObj2.setPermList(permList2);

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		permObjList.add(vXPermObj);
		permObjList.add(vXPermObj2);

		vXPolicyStorm.setPermMapList(permObjList);

		String preCreatedPolicyStorm = jsonUtil
				.writeObjectAsString(vXPolicyStorm);
		logger.info(preCreatedPolicyStorm);

		vXPolicyStorm = publicAPIs.createPolicy(vXPolicyStorm);

		String postCreatedPolicyStorm = jsonUtil
				.writeObjectAsString(vXPolicyStorm);
		logger.info(postCreatedPolicyStorm);

		assertNotNull("Create Policy : Error while creating Policy for Storm",
				vXPolicyStorm);
		logger.info("Create Policy : Policy created successfully for Storm");
	}

	private void createKnoxPolicy() {
		vXPolicyKnox.setPolicyName("HomePolicy_" + GUIDUtil.genGUI());
		vXPolicyKnox.setDescription("home policy for Knox");
		vXPolicyKnox.setRepositoryName(vXRepoKnox.getName());
		vXPolicyKnox.setRepositoryType("Knox");
		vXPolicyKnox.setIsAuditEnabled(true);
		vXPolicyKnox.setIsEnabled(true);
		vXPolicyKnox.setTopologies("topo1, topo2, topo3");
		vXPolicyKnox.setServices("service1, service2, service3");

		VXPermObj vXPermObj = new VXPermObj();
		List<String> userList = new ArrayList<String>();
		userList.add("policymgr");
		vXPermObj.setUserList(userList);

		List<String> permList = new ArrayList<String>();
		permList.add("Allow");
		permList.add("Admin");
		vXPermObj.setPermList(permList);

		VXPermObj vXPermObj2 = new VXPermObj();
		List<String> userList2 = new ArrayList<String>();
		List<String> permList2 = new ArrayList<String>();

		userList2.add("policymgr_hbase");
		userList2.add("policymgr_hive");

		permList2.add("Allow");
		vXPermObj2.setUserList(userList2);
		vXPermObj2.setPermList(permList2);

		VXPermObj vXPermObj3 = new VXPermObj();
		List<String> grpList = new ArrayList<String>();
		List<String> permList3 = new ArrayList<String>();

		grpList.add("Grp1");
		grpList.add("Grp2");
		permList3.add("Allow");
		permList3.add("Admin");
		vXPermObj3.setGroupList(grpList);
		vXPermObj3.setPermList(permList3);
		vXPermObj3.setUserList(userList);

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		permObjList.add(vXPermObj);
		permObjList.add(vXPermObj2);
		permObjList.add(vXPermObj3);

		vXPolicyKnox.setPermMapList(permObjList);

		String preCreatedPolicyKnox = jsonUtil
				.writeObjectAsString(vXPolicyKnox);
		logger.info(preCreatedPolicyKnox);

		vXPolicyKnox = publicAPIs.createPolicy(vXPolicyKnox);

		String postCreatedPolicyKnox = jsonUtil
				.writeObjectAsString(vXPolicyKnox);
		logger.info(postCreatedPolicyKnox);

		assertNotNull("Create Policy : Error while creating Policy for Knox",
				vXPolicyKnox);
		logger.info("Create Policy : Policy created successfully for Knox");
	}

	private void createHivePolicy() {
		vXPolicyHive.setPolicyName("HomePolicy_" + GUIDUtil.genGUI());
		vXPolicyHive.setDatabases("vXPolicyHive_" + GUIDUtil.genGUI());
		vXPolicyHive.setTables("finance,hr," + GUIDUtil.genGUI());
		vXPolicyHive.setColumns("amt, emp_id, " + GUIDUtil.genGUI());
		vXPolicyHive.setDescription("home policy for Hive");
		vXPolicyHive.setRepositoryName(vXRepoHive.getName());
		vXPolicyHive.setRepositoryType("Hive");
		vXPolicyHive.setIsEnabled(true);
		vXPolicyHive.setIsRecursive(true);
		vXPolicyHive.setIsAuditEnabled(true);
		vXPolicyHive.setColumnType("Exclusion");

		VXPermObj vXPermObj = new VXPermObj();
		List<String> userList = new ArrayList<String>();
		userList.add("policymgr");
		vXPermObj.setUserList(userList);

		List<String> permList = new ArrayList<String>();
		permList.add("read");
		permList.add("write");
		permList.add("admin");
		vXPermObj.setPermList(permList);

		VXPermObj vXPermObj2 = new VXPermObj();
		List<String> userList2 = new ArrayList<String>();
		List<String> permList2 = new ArrayList<String>();

		userList2.add("policymgr_hbase");
		userList2.add("policymgr_hive");

		permList2.add("admin");
		permList2.add("write");
		vXPermObj2.setUserList(userList2);
		vXPermObj2.setPermList(permList2);

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		permObjList.add(vXPermObj);
		permObjList.add(vXPermObj2);

		vXPolicyHive.setPermMapList(permObjList);

		String preCreatedPolicyHive = jsonUtil
				.writeObjectAsString(vXPolicyHive);
		logger.info(preCreatedPolicyHive);

		vXPolicyHive = publicAPIs.createPolicy(vXPolicyHive);

		String postCreatedPolicyHive = jsonUtil
				.writeObjectAsString(vXPolicyHive);
		logger.info(postCreatedPolicyHive);

		assertNotNull("Create Policy : Error while creating Policy for Hive",
				vXPolicyHive);
		logger.info("Create Policy : Policy created successfully for Hive");
	}

	private void createHBasePolicy() {
		vXPolicyHBase.setPolicyName("HomePolicy_" + GUIDUtil.genGUI());
		vXPolicyHBase.setTables("finance,hr," + GUIDUtil.genGUI());
		vXPolicyHBase.setColumnFamilies("invoices,emps," + GUIDUtil.genGUI());
		vXPolicyHBase.setColumns("amt, emp_id, " + GUIDUtil.genGUI());
		vXPolicyHBase.setDescription("home policy for HBase");
		vXPolicyHBase.setRepositoryName(vXRepoHBase.getName());
		vXPolicyHBase.setRepositoryType("HBase");
		vXPolicyHBase.setIsEnabled(true);
		vXPolicyHBase.setIsRecursive(true);
		vXPolicyHBase.setIsAuditEnabled(true);

		VXPermObj vXPermObj = new VXPermObj();
		List<String> userList = new ArrayList<String>();
		userList.add("policymgr");
		vXPermObj.setUserList(userList);

		List<String> permList = new ArrayList<String>();
		permList.add("read");
		permList.add("write");
		permList.add("admin");
		vXPermObj.setPermList(permList);

		VXPermObj vXPermObj2 = new VXPermObj();
		List<String> userList2 = new ArrayList<String>();
		List<String> permList2 = new ArrayList<String>();

		userList2.add("policymgr_hbase");
		userList2.add("policymgr_hive");

		permList2.add("admin");
		permList2.add("write");
		vXPermObj2.setUserList(userList2);
		vXPermObj2.setPermList(permList2);

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		permObjList.add(vXPermObj);
		permObjList.add(vXPermObj2);

		vXPolicyHBase.setPermMapList(permObjList);

		String preCreatedPolicyHBase = jsonUtil
				.writeObjectAsString(vXPolicyHBase);
		logger.info(preCreatedPolicyHBase);

		vXPolicyHBase = publicAPIs.createPolicy(vXPolicyHBase);

		String postCreatedPolicyHBase = jsonUtil
				.writeObjectAsString(vXPolicyHBase);
		logger.info(postCreatedPolicyHBase);

		assertNotNull("Create Policy : Error while creating Policy for HBase",
				vXPolicyHBase);
		logger.info("Create Policy : Policy created successfully for HBase");
	}

	private void createHDFSPolicy() {
		vXPolicyHDFS.setPolicyName("HomePolicy_" + GUIDUtil.genGUI());
		vXPolicyHDFS.setResourceName("/user/, / ,/hr/," + GUIDUtil.genGUI());
		vXPolicyHDFS.setDescription("home policy for HDFS");
		vXPolicyHDFS.setRepositoryName(vXRepoHDFS.getName());
		vXPolicyHDFS.setRepositoryType("hdfs");
		vXPolicyHDFS.setIsEnabled(true);
		vXPolicyHDFS.setIsRecursive(true);
		vXPolicyHDFS.setIsAuditEnabled(true);

		VXPermObj vXPermObj = new VXPermObj();
		List<String> userList = new ArrayList<String>();
		userList.add("policymgr");
		userList.add("policymgr_hive");
		vXPermObj.setUserList(userList);

		List<String> permList = new ArrayList<String>();
		permList.add("read");
		permList.add("write");
		permList.add("admin");
		vXPermObj.setPermList(permList);

		VXPermObj vXPermObj2 = new VXPermObj();
		List<String> userList2 = new ArrayList<String>();
		List<String> permList2 = new ArrayList<String>();

		userList2.add("policymgr_hbase");

		permList2.add("admin");
		permList2.add("write");
		vXPermObj2.setUserList(userList2);
		vXPermObj2.setPermList(permList2);

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		permObjList.add(vXPermObj);
		permObjList.add(vXPermObj2);

		vXPolicyHDFS.setPermMapList(permObjList);

		String preCreatedPolicyHDFS = jsonUtil
				.writeObjectAsString(vXPolicyHDFS);
		logger.info(preCreatedPolicyHDFS);

		vXPolicyHDFS = publicAPIs.createPolicy(vXPolicyHDFS);

		String postCreatedPolicyHDFS = jsonUtil
				.writeObjectAsString(vXPolicyHDFS);
		logger.info(postCreatedPolicyHDFS);

		assertNotNull("Create Policy : Error while creating Policy for HDFS",
				vXPolicyHDFS);
		logger.info("Create Policy : Policy created successfully for HDFS");
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#updatePolicy(com.xasecure.view.VXPolicy)}
	 * .
	 */
	public void testUpdatePolicy() {

		// Update HDFS Policy
		vXPolicyHDFS.setPolicyName("HDFS Policy Updated_" + GUIDUtil.genGUI());
		vXPolicyHDFS.setIsRecursive(false);
		vXPolicyHDFS.setPermMapList(vXPolicyHive.getPermMapList());
		vXPolicyHDFS = publicAPIs.updatePolicy(vXPolicyHDFS,
				vXPolicyHDFS.getId());

		assertNotNull("Error While updating Policy", vXPolicyHDFS);
		logger.info("Update : Policy for HDFS updated Successfully");

		// Update HBase Policy
		vXPolicyHBase
				.setPolicyName("HBase Policy Updated_" + GUIDUtil.genGUI());
		vXPolicyHBase.setIsEnabled(false);
		vXPolicyHBase = publicAPIs.updatePolicy(vXPolicyHBase,
				vXPolicyHBase.getId());

		assertNotNull("Error While updating Policy", vXPolicyHBase);
		logger.info("Update : Policy for HBase updated Successfully");

		// Update HIVE Policy
		vXPolicyHive.setPolicyName("Hive Policy Updated_" + GUIDUtil.genGUI());
		vXPolicyHive.setIsAuditEnabled(false);
		vXPolicyHive.setPermMapList(null);
		vXPolicyHive = publicAPIs.updatePolicy(vXPolicyHive,
				vXPolicyHive.getId());

		assertNotNull("Error While updating Policy", vXPolicyHive);
		logger.info("Update : Policy for Hive updated Successfully");

		// Update Knox Policy
		vXPolicyKnox.setPolicyName("Knox Policy Updated_" + GUIDUtil.genGUI());
		vXPolicyKnox.setIsAuditEnabled(false);
		vXPolicyKnox.setPermMapList(null);
		vXPolicyKnox = publicAPIs.updatePolicy(vXPolicyKnox,
				vXPolicyKnox.getId());

		assertNotNull("Error While updating Policy", vXPolicyStorm);
		logger.info("Update : Policy for Hive updated Successfully");

		// Update Storm Policy
		vXPolicyStorm
				.setPolicyName("Storm Policy Updated_" + GUIDUtil.genGUI());
		vXPolicyStorm.setIsAuditEnabled(false);
		vXPolicyStorm.setPermMapList(null);
		vXPolicyStorm = publicAPIs.updatePolicy(vXPolicyStorm,
				vXPolicyStorm.getId());

		assertNotNull("Error While updating Policy", vXPolicyStorm);
		logger.info("Update : Policy for Hive updated Successfully");
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#getPolicy(java.lang.Long)}.
	 */
	public void testGetPolicy() {

		VXPolicy vXPolHDFS = publicAPIs.getPolicy(vXPolicyHDFS.getId());
		String policyHDFSString = jsonUtil.writeObjectAsString(vXPolHDFS);
		logger.info(policyHDFSString);

		assertNotNull("No Policy found for this Id : " + vXPolicyHDFS.getId(),
				vXPolHDFS);
		logger.info("Get : Policy found for this id : " + vXPolicyHDFS.getId());

		VXPolicy vXPolHBase = publicAPIs.getPolicy(vXPolicyHBase.getId());
		assertNotNull("No Policy found for this Id : " + vXPolicyHBase.getId(),
				vXPolHBase);
		logger.info("Get : Policy found for this id : " + vXPolicyHBase.getId());

		VXPolicy vXPolHive = publicAPIs.getPolicy(vXPolicyHive.getId());
		assertNotNull("No Policy found for this Id : " + vXPolicyHive.getId(),
				vXPolHive);
		logger.info("Get : Policy found for this id : " + vXPolicyHive.getId());
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#deletePolicy(java.lang.Long, javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testDeletePolicy() {

		XXResource xxResource = daoManager.getXXResource()
				.findByAssetType(AppConstants.ASSET_HBASE).get(0);

		if (xxResource == null) {
			fail("No Resource found with name : HDFS Repo Updated");
		}

		MockHttpServletRequest request = super.startRequest();
		request.addParameter("force", "true");
		publicAPIs.deletePolicy(xxResource.getId(), request);

		super.endRequest();
	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#searchPolicies(javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testSearchPolicies() {

	}

	/**
	 * Test method for
	 * {@link com.xasecure.rest.PublicAPIs#countPolicies(javax.servlet.http.HttpServletRequest)}
	 * .
	 */
	public void testCountPolicies() {
		VXLong vXLong = publicAPIs.countPolicies(super.startRequest());

		assertNotNull("Count : Error while counting Policies", vXLong);
		logger.info("Count : Total no of Policies are : " + vXLong.getValue());

		super.endRequest();
	}

	@Test
	@Rollback(false)
	public void test() throws Exception {

		createGroups();

		testCreateRepository();
		testUpdateRepository();
		testGetRepository();
		testTestConfig();
		testSearchRepositories();
		testCountRepositories();
		testCreatePolicy();
		testUpdatePolicy();
		testGetPolicy();
		testSearchPolicies();
		testCountPolicies();
		testDeleteRepository();
		testDeletePolicy();
	}

	private void createGroups() {

		XXGroup xGrp1 = daoManager.getXXGroup().findByGroupName("Grp1");
		XXGroup xGrp2 = daoManager.getXXGroup().findByGroupName("Grp2");

		if (xGrp1 == null) {
			VXGroup vXGrp1 = new VXGroup();
			vXGrp1.setName("Grp1");
			vXGrp1.setDescription("Group 1");
			xUserREST.createXGroup(vXGrp1);

			assertNotNull("Error while creating Group", vXGrp1);
			logger.info("New Groups Created Grp Name : " + vXGrp1.getName());
		}
		if (xGrp2 == null) {
			VXGroup vXGrp2 = new VXGroup();
			vXGrp2.setName("Grp2");
			vXGrp2.setDescription("Group 2");
			xUserREST.createXGroup(vXGrp2);

			assertNotNull("Error while creating Group", vXGrp2);
			logger.info("New Groups Created. Grp Name : " + vXGrp2.getName());
		}
	}

}
