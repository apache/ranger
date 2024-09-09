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

package org.apache.ranger.biz;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXDBBaseDao;
import org.apache.ranger.db.XXTrxLogV2Dao;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.rest.ServiceREST;
import org.apache.ranger.security.context.RangerAdminOpContext;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXStringList;
import org.apache.ranger.view.VXUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RangerBizUtil {
	private static final Logger logger = LoggerFactory.getLogger(RangerBizUtil.class);

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	UserMgr userMgr;

	@Autowired
	XUserService xUserService;

	@Autowired
	GUIDUtil guidUtil;
	
	Set<Class<?>> groupEditableClasses;
	private Class<?>[] groupEditableClassesList = {};

	private int maxFirstNameLength;
	int maxDisplayNameLength = 150;
	boolean enableResourceAccessControl;
        private SecureRandom random;
	private static final String PATH_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrst0123456789-_.";
	private static char[] PATH_CHAR_SET = PATH_CHARS.toCharArray();
	private static int PATH_CHAR_SET_LEN = PATH_CHAR_SET.length;
	public static final String AUDIT_STORE_RDBMS = "DB";
	public static final String AUDIT_STORE_SOLR           = "solr";
	public static final String  AUDIT_STORE_ELASTIC_SEARCH = "elasticSearch";
	public static final String  AUDIT_STORE_CLOUD_WATCH = "cloudwatch";
	public static final boolean BATCH_CLEAR_ENABLED = PropertiesUtil.getBooleanProperty("ranger.jpa.jdbc.batch-clear.enable", true);
	public static final int POLICY_BATCH_SIZE  = PropertiesUtil.getIntProperty("ranger.jpa.jdbc.batch-clear.size", 10);
	public static final int BATCH_PERSIST_SIZE = PropertiesUtil.getIntProperty("ranger.jpa.jdbc.batch-persist.size", 500);

	String auditDBType = AUDIT_STORE_RDBMS;
	private final boolean allowUnauthenticatedAccessInSecureEnvironment;
	private final boolean allowUnauthenticatedDownloadAccessInSecureEnvironment;

	static String fileSeparator = PropertiesUtil.getProperty("ranger.file.separator", "/");

	public RangerBizUtil() {
		RangerAdminConfig config = RangerAdminConfig.getInstance();

		allowUnauthenticatedAccessInSecureEnvironment = config.getBoolean("ranger.admin.allow.unauthenticated.access", false);
		allowUnauthenticatedDownloadAccessInSecureEnvironment = config.getBoolean("ranger.admin.allow.unauthenticated.download.access",
				allowUnauthenticatedAccessInSecureEnvironment);

		maxFirstNameLength = Integer.parseInt(PropertiesUtil.getProperty("ranger.user.firstname.maxlength", "16"));
		maxDisplayNameLength = PropertiesUtil.getIntProperty("ranger.bookmark.name.maxlen", maxDisplayNameLength);

		groupEditableClasses = new HashSet<>(
				Arrays.asList(groupEditableClassesList));
		enableResourceAccessControl = PropertiesUtil.getBooleanProperty("ranger.resource.accessControl.enabled", true);

		auditDBType = PropertiesUtil.getProperty("ranger.audit.source.type",
				auditDBType).toLowerCase();
		logger.info("java.library.path is " + System.getProperty("java.library.path"));
		logger.info("Audit datasource is " + auditDBType);
                random = new SecureRandom();
	}

	// Access control methods
	public void checkSystemAdminAccess() {
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		if (currentUserSession != null && currentUserSession.isUserAdmin()) {
			return;
		}
		throw restErrorUtil
				.create403RESTException("Only System Administrators can add accounts");
	}

	/**
	 * @param userProfile
	 * @return
	 */
	public String generatePublicName(VXPortalUser userProfile,
			XXPortalUser gjUser) {
		return generatePublicName(userProfile.getFirstName(),
				userProfile.getLastName());
	}

	public String generatePublicName(String firstName, String lastName) {
		String publicName = null;
		String fName = firstName;
		if (firstName.length() > maxFirstNameLength) {
			fName = firstName.substring(0, maxFirstNameLength - (1 + 3))
					+ "...";
		}
		if (lastName != null && lastName.length() > 0) {
			publicName = fName + " " + lastName.substring(0, 1) + ".";
		}
		return publicName;
	}

	public VXStringList mapStringListToVStringList(List<String> stringList) {
		if (stringList == null) {
			return null;
		}

		List<VXString> vStringList = new ArrayList<>();
		for (String str : stringList) {
			VXString vXString = new VXString();
			vXString.setValue(str);
			vStringList.add(vXString);
		}

		return new VXStringList(vStringList);
	}

	/**
	 * return response object if users is having permission on given resource
	 *
	 * @param vXResource
	 * @param permission
	 * @return
	 */
	public VXResponse hasPermission(VXResource vXResource, int permission) {

		VXResponse vXResponse = new VXResponse();
		if (!enableResourceAccessControl) {
			logger.debug("Resource Access Control is disabled !!!");
			return vXResponse;
		}

		if (vXResource == null) {
			vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
			vXResponse.setMsgDesc("Please provide valid policy.");
			return vXResponse;
		}

		String resourceNames = vXResource.getName();
		if (stringUtil.isEmpty(resourceNames)) {
			vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
			vXResponse.setMsgDesc("Please provide valid policy.");
			return vXResponse;
		}

		if (isAdmin()) {
			return vXResponse;
		}

		Long xUserId = getXUserId();
		Long assetId = vXResource.getAssetId();
		List<XXResource> xResourceList = daoManager.getXXResource()
				.findByAssetIdAndResourceStatus(assetId, RangerCommonEnums.STATUS_ENABLED);

		XXAsset xAsset = daoManager.getXXAsset().getById(assetId);
		int assetType = xAsset.getAssetType();

		vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
		vXResponse.setMsgDesc("Permission Denied !");

		if (assetType == AppConstants.ASSET_HIVE) {
			String[] requestResNameList = resourceNames.split(",");
			if (stringUtil.isEmpty(vXResource.getUdfs())) {
				int reqTableType = vXResource.getTableType();
				int reqColumnType = vXResource.getColumnType();
				for (String resourceName : requestResNameList) {
					boolean matchFound = matchHivePolicy(resourceName,
							xResourceList, xUserId, permission, reqTableType,
							reqColumnType, false);
					if (!matchFound) {
						vXResponse
								.setMsgDesc("You're not permitted to perform "
										+ "the action for resource path : "
										+ resourceName);
						vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
						return vXResponse;
					}
				}
			} else {
				for (String resourceName : requestResNameList) {
					boolean matchFound = matchHivePolicy(resourceName,
							xResourceList, xUserId, permission);
					if (!matchFound) {
						vXResponse
								.setMsgDesc("You're not permitted to perform "
										+ "the action for resource path : "
										+ resourceName);
						vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
						return vXResponse;
					}
				}
			}
			vXResponse.setStatusCode(VXResponse.STATUS_SUCCESS);
			return vXResponse;
		} else if (assetType == AppConstants.ASSET_HBASE) {
		String[] requestResNameList = resourceNames.split(",");
		for (String resourceName : requestResNameList) {
				boolean matchFound = matchHbasePolicy(resourceName,
						xResourceList, vXResponse, xUserId, permission);
				if (!matchFound) {
					vXResponse.setMsgDesc("You're not permitted to perform "
							+ "the action for resource path : " + resourceName);
					vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
					return vXResponse;
				}
			}
			vXResponse.setStatusCode(VXResponse.STATUS_SUCCESS);
			return vXResponse;
		} else if (assetType == AppConstants.ASSET_HDFS) {
			String[] requestResNameList = resourceNames.split(",");
			for (String resourceName : requestResNameList) {
				boolean matchFound = matchHdfsPolicy(resourceName,
						xResourceList, xUserId, permission);
			if (!matchFound) {
					vXResponse.setMsgDesc("You're not permitted to perform "
							+ "the action for resource path : " + resourceName);
					vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
					return vXResponse;
				}
			}
			vXResponse.setStatusCode(VXResponse.STATUS_SUCCESS);
			return vXResponse;
		} else if (assetType == AppConstants.ASSET_KNOX) {
			String[] requestResNameList = resourceNames.split(",");
			for (String resourceName : requestResNameList) {
				boolean matchFound = matchKnoxPolicy(resourceName,
						xResourceList, xUserId, permission);
				if (!matchFound) {
					vXResponse.setMsgDesc("You're not permitted to perform "
							+ "the action for resource path : " + resourceName);
					vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
					return vXResponse;
				}
			}
			vXResponse.setStatusCode(VXResponse.STATUS_SUCCESS);
			return vXResponse;
		} else if (assetType == AppConstants.ASSET_STORM) {
			String[] requestResNameList = resourceNames.split(",");
			for (String resourceName : requestResNameList) {
				boolean matchFound = matchStormPolicy(resourceName,
						xResourceList, xUserId, permission);
				if (!matchFound) {
					vXResponse.setMsgDesc("You're not permitted to perform "
							+ "the action for resource path : " + resourceName);
				vXResponse.setStatusCode(VXResponse.STATUS_ERROR);
				return vXResponse;
			}
		}
		vXResponse.setStatusCode(VXResponse.STATUS_SUCCESS);
		return vXResponse;
	}
		return vXResponse;
	}

	/**
	 * return true id current logged in session is owned by admin
	 *
	 * @return
	 */
	public boolean isAdmin() {
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		if (currentUserSession == null) {
			logger.debug("Unable to find session.");
			return false;
		}

		if (currentUserSession.isUserAdmin()) {
			return true;
		}
		return false;
	}

    public boolean isAuditAdmin() {
        UserSessionBase currentUserSession = ContextUtil
                            .getCurrentUserSession();
            if (currentUserSession == null) {
                logger.debug("Unable to find session.");
            return false;
            }
            if (currentUserSession.isAuditUserAdmin()) {
                return true;
            }
            return false;
    }

	/**
	 * return username of currently logged in user
	 *
	 * @return
	 */
	public String getCurrentUserLoginId() {
		String ret = null;

		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();

		if (currentUserSession != null) {
			ret = currentUserSession.getLoginId();
		}

		return ret;
	}

	/**
	 * returns current user's userID from active user sessions
	 *
	 * @return
	 */
	public Long getXUserId() {

		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		if (currentUserSession == null) {
			logger.debug("Unable to find session.");
			return null;
		}

		XXPortalUser user = daoManager.getXXPortalUser().getById(
				currentUserSession.getUserId());
		if (user == null) {
			logger.debug("XXPortalUser not found with logged in user id : "
					+ currentUserSession.getUserId());
			return null;
		}

		XXUser xUser = daoManager.getXXUser().findByUserName(user.getLoginId());
		if (xUser == null) {
			logger.debug("XXPortalUser not found for user id :" + user.getId()
					+ " with name " + user.getFirstName());
			return null;
		}

		return xUser.getId();
	}

	/**
	 * returns true if user is having required permission on given Hdfs resource
	 *
	 * @param resourceName
	 * @param xResourceList
	 * @param xUserId
	 * @param permission
	 * @return
	 */
	private boolean matchHdfsPolicy(String resourceName,
			List<XXResource> xResourceList, Long xUserId, int permission) {
		boolean matchFound = false;
		resourceName = replaceMetaChars(resourceName);

		for (XXResource xResource : xResourceList) {
			if (xResource.getResourceStatus() != RangerCommonEnums.STATUS_ENABLED) {
				continue;
			}
			Long resourceId = xResource.getId();
			matchFound = checkUsrPermForPolicy(xUserId, permission, resourceId);
			if (matchFound) {
				matchFound = false;
				String resource = xResource.getName();
				String[] dbResourceNameList = resource.split(",");
				for (String dbResourceName : dbResourceNameList) {
					if (comparePathsForExactMatch(resourceName, dbResourceName)) {
						matchFound = true;
					} else {
						if (xResource.getIsRecursive() == RangerCommonEnums.BOOL_TRUE) {
							matchFound = isRecursiveWildCardMatch(resourceName,
									dbResourceName);
						} else {
							matchFound = nonRecursiveWildCardMatch(
									resourceName, dbResourceName);
						}
					}
					if (matchFound) {
						break;
					}
				}
				if (matchFound) {
					break;
				}
			}
		}
		return matchFound;
	}

	public void failUnauthenticatedIfNotAllowed() throws Exception {
		if (UserGroupInformation.isSecurityEnabled()) {
			UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
			if (currentUserSession == null && !allowUnauthenticatedAccessInSecureEnvironment) {
					throw new Exception("Unauthenticated access not allowed");
			}
		}
	}

	public void failUnauthenticatedDownloadIfNotAllowed() throws Exception {
		if (UserGroupInformation.isSecurityEnabled()) {
			UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
			if (currentUserSession == null && !allowUnauthenticatedDownloadAccessInSecureEnvironment) {
					throw new Exception("Unauthenticated access not allowed");
			}
		}
	}

	/**
	 * returns true if user is having required permission on given Hbase
	 * resource
	 *
	 * @param resourceName
	 * @param xResourceList
	 * @param vXResponse
	 * @param xUserId
	 * @param permission
	 * @return
	 */
	public boolean matchHbasePolicy(String resourceName,
			List<XXResource> xResourceList, VXResponse vXResponse,
			Long xUserId, int permission) {
		if (stringUtil.isEmpty(resourceName) || xResourceList == null
				|| xUserId == null) {
			return false;
		}

		String[] splittedResources = stringUtil.split(resourceName,
				fileSeparator);
		if (splittedResources.length < 1 || splittedResources.length > 3) {
			logger.debug("Invalid resourceName name : " + resourceName);
			return false;
		}

		String tblName = splittedResources.length > 0 ? splittedResources[0]
				: StringUtil.WILDCARD_ASTERISK;
		String colFamName = splittedResources.length > 1 ? splittedResources[1]
				: StringUtil.WILDCARD_ASTERISK;
		String colName = splittedResources.length > 2 ? splittedResources[2]
				: StringUtil.WILDCARD_ASTERISK;

		boolean policyMatched = false;
		// check all resources whether Hbase policy is enabled in any resource
		// of provided resource list
		for (XXResource xResource : xResourceList) {
			if (xResource.getResourceStatus() != AppConstants.STATUS_ENABLED) {
				continue;
			}
			Long resourceId = xResource.getId();
			boolean hasPermission = checkUsrPermForPolicy(xUserId, permission,
					resourceId);
			// if permission is enabled then load Tables,column family and
			// columns list from resource
			if (!hasPermission) {
				continue;
			}

			// 1. does the policy match the table?
			String[] xTables = stringUtil.isEmpty(xResource.getTables()) ? null
					: stringUtil.split(xResource.getTables(), ",");

			boolean matchFound = (xTables == null || xTables.length == 0) || matchPath(tblName, xTables);

			if (matchFound) {
				// 2. does the policy match the column?
				String[] xColumnFamilies = stringUtil.isEmpty(xResource
						.getColumnFamilies()) ? null : stringUtil.split(
						xResource.getColumnFamilies(), ",");

				matchFound = (xColumnFamilies == null || xColumnFamilies.length == 0)
						|| matchPath(colFamName, xColumnFamilies);

				if (matchFound) {
					// 3. does the policy match the columnFamily?
					String[] xColumns = stringUtil.isEmpty(xResource
							.getColumns()) ? null : stringUtil.split(
							xResource.getColumns(), ",");

					matchFound = (xColumns == null || xColumns.length == 0)
							|| matchPath(colName, xColumns);
				}
			}

			if (matchFound) {
				policyMatched = true;
				break;
			}
		}
		return policyMatched;
	}

	public boolean matchHivePolicy(String resourceName,
			List<XXResource> xResourceList, Long xUserId, int permission) {
		return matchHivePolicy(resourceName, xResourceList, xUserId,
				permission, 0, 0, true);
	}

	/**
	 * returns true if user is having required permission on given Hive resource
	 *
	 * @param resourceName
	 * @param xResourceList
	 * @param xUserId
	 * @param permission
	 * @param reqTableType
	 * @param reqColumnType
	 * @param isUdfPolicy
	 * @return
	 */
	public boolean matchHivePolicy(String resourceName,
			List<XXResource> xResourceList, Long xUserId, int permission,
			int reqTableType, int reqColumnType, boolean isUdfPolicy) {

		if (stringUtil.isEmpty(resourceName) || xResourceList == null
				|| xUserId == null) {
			return false;
		}

		String[] splittedResources = stringUtil.split(resourceName,
				fileSeparator);// get list of resources
		if (splittedResources.length < 1 || splittedResources.length > 3) {
			logger.debug("Invalid resource name : " + resourceName);
			return false;
		}

		String dbName = splittedResources.length > 0 ? splittedResources[0]
				: StringUtil.WILDCARD_ASTERISK;
		String tblName = splittedResources.length > 1 ? splittedResources[1]
				: StringUtil.WILDCARD_ASTERISK;
		String colName = splittedResources.length > 2 ? splittedResources[2]
				: StringUtil.WILDCARD_ASTERISK;

		boolean policyMatched = false;
		for (XXResource xResource : xResourceList) {
			if (xResource.getResourceStatus() != RangerCommonEnums.STATUS_ENABLED) {
				continue;
			}

			Long resourceId = xResource.getId();
			boolean hasPermission = checkUsrPermForPolicy(xUserId, permission,
					resourceId);

			if (!hasPermission) {
				continue;
			}

			// 1. does the policy match the database?
			String[] xDatabases = stringUtil.isEmpty(xResource.getDatabases()) ? null
					: stringUtil.split(xResource.getDatabases(), ",");

			boolean matchFound = (xDatabases == null || xDatabases.length == 0)
					|| matchPath(dbName, xDatabases);

			if (!matchFound) {
				continue;
			}

			// Type(either UDFs policy or non-UDFs policy) of current policy
			// should be of same as type of policy being iterated
			if (!stringUtil.isEmpty(xResource.getUdfs()) && !isUdfPolicy) {
				continue;
			}

			if (isUdfPolicy) {
				// 2. does the policy match the UDF?
				String[] xUdfs = stringUtil.isEmpty(xResource.getUdfs()) ? null
						: stringUtil.split(xResource.getUdfs(), ",");

				if (!matchPath(tblName, xUdfs)) {
					continue;
				} else {
					policyMatched = true;
					break;
				}
			} else {
				// 2. does the policy match the table?
				String[] xTables = stringUtil.isEmpty(xResource.getTables()) ? null
						: stringUtil.split(xResource.getTables(), ",");

				matchFound = (xTables == null || xTables.length == 0)
						|| matchPath(tblName, xTables);

				if (xResource.getTableType() == AppConstants.POLICY_EXCLUSION) {
					matchFound = !matchFound;
				}

				if (!matchFound) {
					continue;
				}

				// 3. does current policy match the column?
				String[] xColumns = stringUtil.isEmpty(xResource.getColumns()) ? null
						: stringUtil.split(xResource.getColumns(), ",");

				matchFound = (xColumns == null || xColumns.length == 0)
						|| matchPath(colName, xColumns);

				if (xResource.getColumnType() == AppConstants.POLICY_EXCLUSION) {
					matchFound = !matchFound;
				}

				if (!matchFound) {
					continue;
				} else {
					policyMatched = true;
					break;
				}
			}
		}
		return policyMatched;
	}

	/**
	 * returns true if user is having required permission on given Hbase
	 * resource
	 *
	 * @param resourceName
	 * @param xResourceList
	 * @param xUserId
	 * @param permission
	 * @return
	 */
	private boolean matchKnoxPolicy(String resourceName,
			List<XXResource> xResourceList,
			Long xUserId, int permission) {

		String[] splittedResources = stringUtil.split(resourceName,
				fileSeparator);
		int numberOfResources = splittedResources.length;
		if (numberOfResources < 1 || numberOfResources > 3) {
			logger.debug("Invalid policy name : " + resourceName);
			return false;
		}

		boolean policyMatched = false;
		// check all resources whether Knox policy is enabled in any resource
		// of provided resource list
		for (XXResource xResource : xResourceList) {
			if (xResource.getResourceStatus() != RangerCommonEnums.STATUS_ENABLED) {
				continue;
			}
			Long resourceId = xResource.getId();
			boolean hasPermission = checkUsrPermForPolicy(xUserId, permission,
					resourceId);
			// if permission is enabled then load Topologies,services list from
			// resource
			if (hasPermission) {
				String[] xTopologies = (xResource.getTopologies() == null || "".equalsIgnoreCase(xResource
						.getTopologies())) ? null
						: stringUtil.split(xResource.getTopologies(), ",");
				String[] xServices = (xResource.getServices() == null || "".equalsIgnoreCase(xResource
						.getServices())) ? null
						: stringUtil.split(xResource.getServices(), ",");

				boolean matchFound = false;

				for (int index = 0; index < numberOfResources; index++) {
					matchFound = false;
					// check whether given table resource matches with any
					// existing topology resource
					if (index == 0) {
						if (xTopologies != null) {
							for (String xTopology : xTopologies) {
								if (matchPath(splittedResources[index],
										xTopology)) {
									matchFound = true;
									continue;
								}
							}
						}
						if (!matchFound) {
							break;
						}
					} // check whether given service resource matches with
						// any existing service resource
					else if (index == 1) {
						if (xServices != null) {
							for (String xService : xServices) {
								if (matchPath(splittedResources[index],
										xService)) {
									matchFound = true;
									continue;
								}
							}
						}
						if (!matchFound) {
							break;
						}
					}
				}
				if (matchFound) {
					policyMatched = true;
					break;
				}
			}
		}
		return policyMatched;
	}

	/**
	 * returns true if user is having required permission on given STORM
	 * resource
	 *
	 * @param resourceName
	 * @param xResourceList
	 * @param xUserId
	 * @param permission
	 * @return
	 */
	private boolean matchStormPolicy(String resourceName,
			List<XXResource> xResourceList,
			Long xUserId, int permission) {

		String[] splittedResources = stringUtil.split(resourceName,
				fileSeparator);
		int numberOfResources = splittedResources.length;
		if (numberOfResources < 1 || numberOfResources > 3) {
			logger.debug("Invalid policy name : " + resourceName);
			return false;
		}

		boolean policyMatched = false;
		// check all resources whether Knox policy is enabled in any resource
		// of provided resource list
		for (XXResource xResource : xResourceList) {
			if (xResource.getResourceStatus() != RangerCommonEnums.STATUS_ENABLED) {
				continue;
			}
			Long resourceId = xResource.getId();
			boolean hasPermission = checkUsrPermForPolicy(xUserId, permission,
					resourceId);
			// if permission is enabled then load Topologies,services list from
			// resource
			if (hasPermission) {
				String[] xTopologies = (xResource.getTopologies() == null || "".equalsIgnoreCase(xResource
						.getTopologies())) ? null
						: stringUtil.split(xResource.getTopologies(), ",");

				boolean matchFound = false;

				for (int index = 0; index < numberOfResources; index++) {
					matchFound = false;
					// check whether given table resource matches with any
					// existing topology resource
					if (index == 0 && xTopologies != null) {
						for (String xTopology : xTopologies) {
							if (matchPath(splittedResources[index],
									xTopology)) {
								matchFound = true;
								continue;
							}
						}
					} // check whether given service resource matches with
						// any existing service resource
				}
				if (matchFound) {
					policyMatched = true;
					break;
				}
			}
		}
		return policyMatched;
	}

	/**
	 * returns path without meta characters
	 *
	 * @param path
	 * @return
	 */
	public String replaceMetaChars(String path) {
		if (path == null || path.isEmpty()) {
			return path;
		}

		if (path.contains("*")) {
			String replacement = getRandomString(5, 60);
			path = path.replaceAll("\\*", replacement);
		}
		if (path.contains("?")) {
			String replacement = getRandomString(1, 1);
			path = path.replaceAll("\\?", replacement);
		}
		return path;
	}

	/**
	 * returns random String of given length range
	 *
	 * @param minLen
	 * @param maxLen
	 * @return
	 */
	private String getRandomString(int minLen, int maxLen) {
		StringBuilder sb = new StringBuilder();
		int len = getRandomInt(minLen, maxLen);
		for (int i = 0; i < len; i++) {
			int charIdx = random.nextInt(PATH_CHAR_SET_LEN);
			sb.append(PATH_CHAR_SET[charIdx]);
		}
		return sb.toString();
	}

	/**
	 * return random integer number for given range
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	private int getRandomInt(int min, int max) {
		if (min == max) {
			return min;
		} else {
			int interval = max - min;
			int randomNum = random.nextInt();
			if(randomNum<0){
				randomNum=Math.abs(randomNum);
			}
			return ((randomNum % interval) + min);
		}
	}

	/**
	 * returns true if given userID is having specified permission on specified
	 * resource
	 *
	 * @param xUserId
	 * @param permission
	 * @param resourceId
	 * @return
	 */
	private boolean checkUsrPermForPolicy(Long xUserId, int permission,
			Long resourceId) {
		// this snippet load user groups and permission map list from DB
		List<XXGroup> userGroups = daoManager.getXXGroup().findByUserId(xUserId);
		List<XXPermMap> permMapList = daoManager.getXXPermMap().findByResourceId(resourceId);
		Long publicGroupId = getPublicGroupId();
		boolean matchFound = false;
		for (XXPermMap permMap : permMapList) {
			if (permMap.getPermType() == permission) {
				if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_GROUP) {
					// check whether permission is enabled for public group or a
					// group to which user belongs
					matchFound = (publicGroupId != null && publicGroupId.equals(permMap.getGroupId())) ||
											 isGroupInList(permMap.getGroupId(), userGroups);
				} else if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_USER) {
					// check whether permission is enabled to user
					matchFound = permMap.getUserId().equals(xUserId);
				}
			}
			if (matchFound) {
				break;
			}
		}
		return matchFound;
	}

	public Long getPublicGroupId() {
		XXGroup xXGroupPublic = daoManager.getXXGroup().findByGroupName(
				RangerConstants.GROUP_PUBLIC);

		return xXGroupPublic != null ? xXGroupPublic.getId() : null;
	}

	/**
	 * returns true is given group id is in given group list
	 *
	 * @param groupId
	 * @param xGroupList
	 * @return
	 */
	public boolean isGroupInList(Long groupId, List<XXGroup> xGroupList) {
		for (XXGroup xGroup : xGroupList) {
			if (xGroup.getId().equals(groupId)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * returns true if given path matches in same level or sub directories with
	 * given wild card pattern
	 *
	 * @param pathToCheck
	 * @param wildcardPath
	 * @return
	 */
	public boolean isRecursiveWildCardMatch(String pathToCheck,
			String wildcardPath) {
		if (pathToCheck != null) {
			if (wildcardPath != null && wildcardPath.equals(fileSeparator)) {
				return true;
			}
			StringBuilder sb = new StringBuilder();
			for (String p : pathToCheck.split(fileSeparator)) {
				sb.append(p);
				boolean matchFound = FilenameUtils.wildcardMatch(sb.toString(),
						wildcardPath);
				if (matchFound) {
					return true;
				}
				sb.append(fileSeparator);
			}
			sb = null;
		}
		return false;
	}

	/**
	 * return List<Integer>
	 *
	 * List of all possible parent return type for some specific resourceType
	 *
	 * @param resourceType
	 *            , assetType
	 *
	 */
	public List<Integer> getResorceTypeParentHirearchy(int resourceType,
			int assetType) {
		List<Integer> resourceTypeList = new ArrayList<>();

		if (assetType == AppConstants.ASSET_HDFS) {
			resourceTypeList.add(AppConstants.RESOURCE_PATH);
		} else if (assetType == AppConstants.ASSET_HIVE) {
			resourceTypeList.add(AppConstants.RESOURCE_DB);
			if (resourceType == AppConstants.RESOURCE_TABLE) {
				resourceTypeList.add(AppConstants.RESOURCE_TABLE);
			} else if (resourceType == AppConstants.RESOURCE_UDF) {
				resourceTypeList.add(AppConstants.RESOURCE_UDF);
			} else if (resourceType == AppConstants.RESOURCE_COLUMN) {
				resourceTypeList.add(AppConstants.RESOURCE_TABLE);
				resourceTypeList.add(AppConstants.RESOURCE_COLUMN);
			}
		} else if (assetType == AppConstants.ASSET_HBASE) {
			resourceTypeList.add(AppConstants.RESOURCE_TABLE);
			if (resourceType == AppConstants.RESOURCE_COL_FAM) {
				resourceTypeList.add(AppConstants.RESOURCE_COL_FAM);
			} else if (resourceType == AppConstants.RESOURCE_COLUMN) {
				resourceTypeList.add(AppConstants.RESOURCE_COL_FAM);
				resourceTypeList.add(AppConstants.RESOURCE_COLUMN);
			}
		}

		return resourceTypeList;
	}

	/**
	 * return true if both path matches exactly, wild card matching is not
	 * checked
	 *
	 * @param path1
	 * @param path2
	 * @return
	 */
	public boolean comparePathsForExactMatch(String path1, String path2) {
		String pathSeparator = fileSeparator;
		if (!path1.endsWith(pathSeparator)) {
			path1 = path1.concat(pathSeparator);
		}
		if (!path2.endsWith(pathSeparator)) {
			path2 = path2.concat(pathSeparator);
		}
		return path1.equalsIgnoreCase(path2);
	}

	/**
	 * return true if both path matches at same level path, this function does
	 * not match sub directories
	 *
	 * @param pathToCheck
	 * @param wildcardPath
	 * @return
	 */
	public boolean nonRecursiveWildCardMatch(String pathToCheck,
			String wildcardPath) {
		if (pathToCheck != null && wildcardPath != null) {

			List<String> pathToCheckArray = new ArrayList<>();
			List<String> wildcardPathArray = new ArrayList<>();

			Collections.addAll(pathToCheckArray, pathToCheck.split(fileSeparator));
			Collections.addAll(wildcardPathArray, wildcardPath.split(fileSeparator));

			if (pathToCheckArray.size() == wildcardPathArray.size()) {
				boolean match = false;
				for (int index = 0; index < pathToCheckArray.size(); index++) {
					match = matchPath(pathToCheckArray.get(index),
							wildcardPathArray.get(index));
					if (!match)
						return match;
				}
				return match;
			}
		}
		return false;
	}

	/**
	 * returns true if first and second path are same
	 *
	 * @param pathToCheckFragment
	 * @param wildCardPathFragment
	 * @return
	 */
	private boolean matchPath(String pathToCheckFragment,
			String wildCardPathFragment) {
		if (pathToCheckFragment == null || wildCardPathFragment == null) {
			return false;
		}

		if (pathToCheckFragment.contains("*")
				|| pathToCheckFragment.contains("?")) {
			pathToCheckFragment = replaceMetaChars(pathToCheckFragment);

			if (wildCardPathFragment.contains("*")
					|| wildCardPathFragment.contains("?")) {
				return FilenameUtils.wildcardMatch(pathToCheckFragment,
						wildCardPathFragment, IOCase.SENSITIVE);
			} else {
				return false;
			}
		} else {
			if (wildCardPathFragment.contains("*")
					|| wildCardPathFragment.contains("?")) {
				return FilenameUtils.wildcardMatch(pathToCheckFragment,
						wildCardPathFragment, IOCase.SENSITIVE);
			} else {
				return pathToCheckFragment.trim().equals(
						wildCardPathFragment.trim());
			}
		}
	}

	private boolean matchPath(String pathToCheck, String[] wildCardPaths) {
		if (pathToCheck != null && wildCardPaths != null) {
			for (String wildCardPath : wildCardPaths) {
				if (matchPath(pathToCheck, wildCardPath)) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * This method returns true if first parameter value is equal to others
	 * argument value passed
	 *
	 * @param checkValue
	 * @param otherValues
	 * @return
	 */
	public static boolean areAllEqual(int checkValue, int... otherValues) {
		for (int value : otherValues) {
			if (value != checkValue) {
				return false;
			}
		}
		return true;
	}

	public void createTrxLog(List<XXTrxLogV2> trxLogList) {
		if (trxLogList == null || trxLogList.size() == 0) {
			return;
		}

		if (guidUtil == null) {
			// log a warning
			return;
		}

		XXTrxLogV2Dao   dao       = daoManager.getXXTrxLogV2();
		String          trxId     = Long.toString(guidUtil.genLong());
		UserSessionBase usb       = ContextUtil.getCurrentUserSession();
		String          sessionId = usb != null && usb.getSessionId() != null ? usb.getSessionId().toString() : null;
		Long            userId    = usb != null ? usb.getUserId() : null;

		for (XXTrxLogV2 xTrxLog : trxLogList) {
			xTrxLog.setTransactionId(trxId);
			xTrxLog.setSessionId(sessionId);
			xTrxLog.setSessionType("Spring Authenticated Session");
			xTrxLog.setRequestId(trxId);

			if (xTrxLog.getAddedByUserId() == null) {
				xTrxLog.setAddedByUserId(userId);
			}

			dao.create(xTrxLog);
		}
	}

	public static int getDBFlavor() {
		String[] propertyNames = { "xa.db.flavor",
									"ranger.jpa.jdbc.dialect",
									"ranger.jpa.jdbc.url",
									"ranger.jpa.jdbc.driver"
								};

		for(String propertyName : propertyNames) {
			String propertyValue = PropertiesUtil.getProperty(propertyName);

			if(StringUtils.isBlank(propertyValue)) {
				continue;
			}

			if (StringUtils.containsIgnoreCase(propertyValue, "mysql")) {
				return AppConstants.DB_FLAVOR_MYSQL;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "oracle")) {
				return AppConstants.DB_FLAVOR_ORACLE;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "postgresql")) {
				return AppConstants.DB_FLAVOR_POSTGRES;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqlserver")) {
				return AppConstants.DB_FLAVOR_SQLSERVER;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "mssql")) {
				return AppConstants.DB_FLAVOR_SQLSERVER;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqlanywhere")) {
				return AppConstants.DB_FLAVOR_SQLANYWHERE;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqla")) {
				return AppConstants.DB_FLAVOR_SQLANYWHERE;
			}else {
				if(logger.isDebugEnabled()) {
					logger.debug("DB Flavor could not be determined from property - " + propertyName + "=" + propertyValue);
				}
			}
		}

		logger.error("DB Flavor could not be determined");

		return AppConstants.DB_FLAVOR_UNKNOWN;
	}

	public static String getDBFlavorType(int dbFlavor) {
		final String dbFlavourType;

		switch (dbFlavor) {
			case AppConstants.DB_FLAVOR_MYSQL:
				dbFlavourType = "MYSQL";
				break;
			case AppConstants.DB_FLAVOR_ORACLE:
				dbFlavourType = "ORACLE";
				break;
			case AppConstants.DB_FLAVOR_POSTGRES:
				dbFlavourType = "POSTGRES";
				break;
			case AppConstants.DB_FLAVOR_SQLANYWHERE:
				dbFlavourType = "SQLANYWHERE";
				break;
			case AppConstants.DB_FLAVOR_SQLSERVER:
				dbFlavourType = "SQLSERVER";
				break;
			default:
				dbFlavourType = "Unknown";
				break;
		}

		return dbFlavourType;
	}

	public static String getDBVersionQuery(int dbFlavor){
		final String dbQuery;

		switch (dbFlavor) {
			case AppConstants.DB_FLAVOR_ORACLE:
				dbQuery = "SELECT banner from v$version where rownum<2";
				break;
			case AppConstants.DB_FLAVOR_MYSQL:
			case AppConstants.DB_FLAVOR_POSTGRES:
				dbQuery = "SELECT version()";
				break;
			case AppConstants.DB_FLAVOR_SQLANYWHERE:
			case AppConstants.DB_FLAVOR_SQLSERVER:
				dbQuery = "SELECT @@version";
				break;
			default:
				dbQuery = null;
				break;
		}

		return dbQuery;
	}


	public String getDBVersion(){
		return daoManager.getXXUser().getDBVersion();
    }

	public String getAuditDBType() {
		return auditDBType;
	}

	public void setAuditDBType(String auditDBType) {
		this.auditDBType = auditDBType;
	}

	/**
	 * return true id current logged in session is owned by keyadmin
	 *
	 * @return
	 */
	public boolean isKeyAdmin() {
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		if (currentUserSession == null) {
			logger.debug("Unable to find session.");
			return false;
		}

		return currentUserSession.isKeyAdmin();
	}
    public boolean isAuditKeyAdmin() {
        UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
        if (currentUserSession == null) {
                logger.debug("Unable to find session.");
                return false;
        }
        return (currentUserSession.isAuditKeyAdmin());
    }

	/**
	 * @param xxDbBase
	 * @param baseModel
	 * @return Boolean
	 *
	 * @NOTE: Kindly check all the references of this function before making any changes
	 */
	public Boolean hasAccess(XXDBBase xxDbBase, RangerBaseModelObject baseModel) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			logger.info("User session not found, granting access.");
			return true;
		}

		boolean isKeyAdmin = session.isKeyAdmin();
		boolean isSysAdmin = session.isUserAdmin();
		boolean isAuditor =  session.isAuditUserAdmin();
		boolean isAuditorKeyAdmin = session.isAuditKeyAdmin();
		boolean isUser = session.getUserRoleList().contains(RangerConstants.ROLE_USER);

		if (xxDbBase != null && xxDbBase instanceof XXServiceDef) {
			return hasAccessToXXServiceDef((XXServiceDef) xxDbBase, isKeyAdmin, isSysAdmin, isAuditor, isAuditorKeyAdmin, isUser);
		}

		if (xxDbBase != null && xxDbBase instanceof XXService) {
			return hasAccessToXXService((XXService) xxDbBase, isKeyAdmin, isSysAdmin, isAuditor, isAuditorKeyAdmin, isUser);
		}
		return false;
	}

	private Boolean hasAccessToXXServiceDef(XXServiceDef xxDbBase, boolean isKeyAdmin, boolean isSysAdmin, boolean isAuditor, boolean isAuditorKeyAdmin, boolean isUser) {
		XXServiceDef xServiceDef = xxDbBase;
		final String implClass = xServiceDef.getImplclassname();
		if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(implClass)) {
			// KMS case
			return isKeyAdmin || isAuditorKeyAdmin;
		} else {
			// Other cases - implClass can be null!
			return isSysAdmin || isUser || isAuditor;
		}
	}

	private Boolean hasAccessToXXService(XXService xxDbBase, boolean isKeyAdmin, boolean isSysAdmin, boolean isAuditor, boolean isAuditorKeyAdmin, boolean isUser) {
		// TODO: As of now we are allowing SYS_ADMIN to create/update/read/delete all the
		// services including KMS
		if (isSysAdmin || isAuditor) {
			return true;
		}

		XXService xService = xxDbBase;
		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
		String implClass = xServiceDef.getImplclassname();
		if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(implClass)) {
			// KMS case
			return isKeyAdmin || isAuditorKeyAdmin;
		} else {
			// Other cases - implClass can be null!
			return isUser;
		}
	}

	public void hasAdminPermissions(String objType) {

		UserSessionBase session = ContextUtil.getCurrentUserSession();

		if (session == null) {
			throw restErrorUtil.createRESTException("UserSession cannot be null, only Admin can create/update/delete "
					+ objType, MessageEnums.OPER_NO_PERMISSION);
		}

		if (!session.isKeyAdmin() && !session.isUserAdmin()) {
			throw restErrorUtil.createRESTException(
					"This user is not allowed this operation. Only users with Admin permission have access to this operation " + objType,
					MessageEnums.OPER_NO_PERMISSION);
		}
	}

	public void hasKMSPermissions(String objType, String implClassName) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			throw restErrorUtil.createRESTException("UserSession cannot be null, only KeyAdmin can create/update/delete "
					+ objType, MessageEnums.OPER_NO_PERMISSION);
		}

		if (session.isKeyAdmin() && !EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(implClassName)) {
			throw restErrorUtil.createRESTException("KeyAdmin can create/update/delete only KMS " + objType,
					MessageEnums.OPER_NO_PERMISSION);
		}

		// TODO: As of now we are allowing SYS_ADMIN to create/update/read/delete all the
		// services including KMS

		if ("Service-Def".equalsIgnoreCase(objType) && session.isUserAdmin() && EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(implClassName)) {
			throw restErrorUtil.createRESTException("System Admin cannot create/update/delete KMS " + objType,
					MessageEnums.OPER_NO_PERMISSION);
		}
	}

	public boolean checkUserAccessible(VXUser vXUser) {
                boolean isAccessible = true;
                Collection<String> roleList = userMgr.getRolesByLoginId(vXUser
                                .getName());
                if (isKeyAdmin()) {
                        if (vXUser.getUserRoleList().contains(RangerConstants.ROLE_SYS_ADMIN)
                                        || vXUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN_AUDITOR)
                                        || roleList.contains(RangerConstants.ROLE_SYS_ADMIN)
                                        || roleList.contains(RangerConstants.ROLE_ADMIN_AUDITOR)) {
                                isAccessible = false;
                        }
                }
                if (isAdmin()) {
                        if (vXUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN)
                                        || vXUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)
                                        || roleList.contains(RangerConstants.ROLE_KEY_ADMIN)
                                        || roleList.contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)) {
                                isAccessible = false;
                        }
                }
                if (!isAccessible) {
                        throw restErrorUtil.createRESTException(
                                        "Logged in user is not allowed to create/update user",
					MessageEnums.OPER_NO_PERMISSION);
		}
                return isAccessible;
	}
	
	public boolean isSSOEnabled() {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null) {
			return session.isSSOEnabled() == null ? PropertiesUtil.getBooleanProperty("ranger.sso.enabled", false) : session.isSSOEnabled();
		} else {
			throw restErrorUtil.createRESTException(
					"User session is not created",
					MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
		}
	}
	
	public boolean isUserAllowed(RangerService rangerService, String cfgNameAllowedUsers) {
		Map<String, String> map = rangerService.getConfigs();
		String user = null;
		UserSessionBase userSession = ContextUtil.getCurrentUserSession();
		if(userSession != null){
			user = userSession.getLoginId();
		}
		if (map != null && map.containsKey(cfgNameAllowedUsers)) {
			String userNames = map.get(cfgNameAllowedUsers);
			String[] userList = userNames.split(",");
			if(userList != null){
				for (String u : userList) {
					if ("*".equals(u) || (user != null && u.equalsIgnoreCase(user))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public boolean isUserAllowedForGrantRevoke(RangerService rangerService, String userName) {
		return isUserInConfigParameter(rangerService, ServiceREST.Allowed_User_List_For_Grant_Revoke, userName);
	}

	public boolean isUserRangerAdmin(String username) {
		boolean isAdmin = false;
		try {
			VXUser vxUser = xUserService.getXUserByUserName(username);
			if (vxUser != null && (vxUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN) || vxUser.getUserRoleList().contains(RangerConstants.ROLE_SYS_ADMIN))) {
				isAdmin = true;
			}
		} catch (Exception ex) {
		}
		return isAdmin;
	}

	public boolean isUserServiceAdmin(RangerService rangerService, String userName) {
		boolean ret = isUserInConfigParameter(rangerService, ServiceDBStore.SERVICE_ADMIN_USERS, userName);

		if (!ret && userMgr != null && userMgr.xUserMgr != null) {
			ret = isAnyGroupInConfigParameter(rangerService, ServiceDBStore.SERVICE_ADMIN_GROUPS, userMgr.xUserMgr.getGroupsForUser(userName));
		}

		return ret;
	}

	public boolean isUserInConfigParameter(RangerService rangerService, String configParamName, String userName) {
		Map<String, String> map = rangerService.getConfigs();

		if (map != null && map.containsKey(configParamName)) {
			String userNames = map.get(configParamName);
			String[] userList = userNames.split(",");
			if (userList != null) {
				for (String u : userList) {
					if ("*".equals(u) || (userName != null && u.equalsIgnoreCase(userName))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public boolean isAnyGroupInConfigParameter(RangerService rangerService, String configParamName, Set<String> groupNames) {
		boolean             ret      = false;
		Map<String, String> map      = rangerService.getConfigs();
		String              cfgValue = map != null ? map.get(configParamName) : null;

		if (StringUtils.isNotBlank(cfgValue) && CollectionUtils.isNotEmpty(groupNames)) {
			String[] svcCfgGroupNames = cfgValue.split(",");

			for (String svcCfgGroupName : svcCfgGroupNames) {
				if (RangerConstants.GROUP_PUBLIC.equals(svcCfgGroupName) || groupNames.contains(svcCfgGroupName)) {
					ret = true;

					break;
				}
			}
		}

		return ret;
	}

        public void blockAuditorRoleUser() {
                UserSessionBase session = ContextUtil.getCurrentUserSession();
                if (session != null) {
                        if (session.isAuditKeyAdmin() || session.isAuditUserAdmin()) {
                                VXResponse vXResponse = new VXResponse();
                                vXResponse.setStatusCode(HttpServletResponse.SC_FORBIDDEN);
                                vXResponse.setMsgDesc("Operation"
                                                + " denied. LoggedInUser="
                                                +  session.getXXPortalUser().getId()
                                                + " ,isn't permitted to perform the action.");
                                throw restErrorUtil.generateRESTException(vXResponse);
                        }
                } else {
                        VXResponse vXResponse = new VXResponse();
                        vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED); // user is null
                        vXResponse.setMsgDesc("Bad Credentials");
                        throw restErrorUtil.generateRESTException(vXResponse);
                }
        }

	public boolean hasModuleAccess(String moduleName) {
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		if(currentUserSession == null) {
			return false;
		}
		if(!currentUserSession.isUserAdmin() && !currentUserSession.isAuditUserAdmin()) {
			if(!currentUserSession.getRangerUserPermission().getUserPermissions().contains(moduleName)) {
				return false;
			}
		}
		return true;
	}

	public void removeEmptyStrings(List<String> list) {
		if(!CollectionUtils.isEmpty(list)) {
			Iterator<String> i = list.iterator();
			while (i.hasNext()){
				String item = i.next();
				if (item == null || StringUtils.isEmpty(StringUtils.trim(item))){
					i.remove();
			    }
			}
			trimAll(list);
		}
	}

	public void trimAll(List<String> list) {
		if(!CollectionUtils.isEmpty(list)) {
			for (int i = 0; i < list.size(); i++) {
				String item=list.get(i);
				if(item.startsWith(" ") || item.endsWith(" ")) {
					list.set(i, StringUtils.trim(item));
				}
			}
		}
	}

	public static boolean isBulkMode() {
		return ContextUtil.isBulkModeContext();
	}

	public static boolean setBulkMode(boolean val) {
		RangerContextHolder.getOrCreateOpContext().setBulkModeContext(val);

		return isBulkMode();
	}

	public boolean getCreatePrincipalsIfAbsent() {
		RangerAdminOpContext opContext = RangerContextHolder.getOpContext();
		Boolean              ret       = opContext != null ? opContext.getCreatePrincipalsIfAbsent() : null;

		return ret != null && ret;
	}

	//should be used only in bulk operation like importPolicies, policies delete.
	public void bulkModeOnlyFlushAndClear() {
		if (BATCH_CLEAR_ENABLED) {
			XXDBBaseDao xXDBBaseDao = daoManager.getXXDBBase();
			if (xXDBBaseDao != null) {
				xXDBBaseDao.flush();
				xXDBBaseDao.clear();
			}
		}
	}

	public boolean checkAdminAccess() {
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		if (currentUserSession != null) {
			return currentUserSession.isUserAdmin();
		} else {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED); // user is null
			vXResponse.setMsgDesc("Bad Credentials");
			throw restErrorUtil.generateRESTException(vXResponse);
		}
	}

	public boolean isGdsServiceDef(XXDBBase xxdbBase) {
		return (xxdbBase instanceof XXServiceDef) && EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME.equals(((XXServiceDef) xxdbBase).getName());
	}

	public boolean isGdsService(XXDBBase xxdbBase) {
		return (xxdbBase instanceof XXService) && EmbeddedServiceDefsUtil.instance().getGdsServiceDefId() == ((XXService) xxdbBase).getType();
	}
}
