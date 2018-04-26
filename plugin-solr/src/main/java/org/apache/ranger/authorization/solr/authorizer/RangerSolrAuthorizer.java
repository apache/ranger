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

package org.apache.ranger.authorization.solr.authorizer;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.audit.RangerMultiResourceAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;

public class RangerSolrAuthorizer implements AuthorizationPlugin {
	private static final Log logger = LogFactory
			.getLog(RangerSolrAuthorizer.class);
	private static final Log PERF_SOLRAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("solrauth.request");

	public static final String PROP_USE_PROXY_IP = "xasecure.solr.use_proxy_ip";
	public static final String PROP_PROXY_IP_HEADER = "xasecure.solr.proxy_ip_header";
	public static final String PROP_SOLR_APP_NAME = "xasecure.solr.app.name";

	public static final String KEY_COLLECTION = "collection";

	public static final String ACCESS_TYPE_CREATE = "create";
	public static final String ACCESS_TYPE_UPDATE = "update";
	public static final String ACCESS_TYPE_QUERY = "query";
	public static final String ACCESS_TYPE_OTHERS = "others";
	public static final String ACCESS_TYPE_ADMIN = "solr_admin";

	private static volatile RangerBasePlugin solrPlugin = null;

	boolean useProxyIP = false;
	String proxyIPHeader = "HTTP_X_FORWARDED_FOR";
	String solrAppName = "Client";

	public RangerSolrAuthorizer() {
		logger.info("RangerSolrAuthorizer()");

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.solr.security.SolrAuthorizationPlugin#init(java.util.Map)
	 */
	@Override
	public void init(Map<String, Object> initInfo) {
		logger.info("init()");

		try {
			useProxyIP = RangerConfiguration.getInstance().getBoolean(
					PROP_USE_PROXY_IP, useProxyIP);
			proxyIPHeader = RangerConfiguration.getInstance().get(
					PROP_PROXY_IP_HEADER, proxyIPHeader);
			// First get from the -D property
			solrAppName = System.getProperty("solr.kerberos.jaas.appname",
					solrAppName);
			// Override if required from Ranger properties
			solrAppName = RangerConfiguration.getInstance().get(
					PROP_SOLR_APP_NAME, solrAppName);

			logger.info("init(): useProxyIP=" + useProxyIP);
			logger.info("init(): proxyIPHeader=" + proxyIPHeader);
			logger.info("init(): solrAppName=" + solrAppName);
			logger.info("init(): KerberosName.rules="
					+ MiscUtil.getKerberosNamesRules());
			authToJAASFile();

		} catch (Throwable t) {
			logger.fatal("Error init", t);
		}

		try {
			RangerBasePlugin me = solrPlugin;
			if (me == null) {
				synchronized(RangerSolrAuthorizer.class) {
					me = solrPlugin;
					logger.info("RangerSolrAuthorizer(): init called");
					if (me == null) {
						me = solrPlugin = new RangerBasePlugin("solr", "solr");
					}
				}
			}
			solrPlugin.init();
		} catch (Throwable t) {
			logger.fatal("Error creating and initializing RangerBasePlugin()");
		}
	}

	private void authToJAASFile() {
		try {
			MiscUtil.setUGIFromJAASConfig(solrAppName);
			logger.info("LoginUser=" + MiscUtil.getUGILoginUser());
		} catch (Throwable t) {
			logger.error("Error authenticating for appName=" + solrAppName, t);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		logger.info("close() called");
		try {
			solrPlugin.cleanup();
			/* Solr shutdown is not graceful so that JVM shutdown hooks
			 * are not always invoked and the audit store are not flushed. So
			 * we are forcing a cleanup here.
			 */
			if (solrPlugin.getAuditProviderFactory() != null) {
				solrPlugin.getAuditProviderFactory().shutdown();
			}
		} catch (Throwable t) {
			logger.error("Error cleaning up Ranger plugin. Ignoring error", t);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.apache.solr.security.SolrAuthorizationPlugin#authorize(org.apache
	 * .solr.security.SolrRequestContext)
	 */
	@Override
	public AuthorizationResponse authorize(AuthorizationContext context) {
		boolean isDenied = false;

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("==> RangerSolrAuthorizer.authorize()");
				logAuthorizationConext(context);
			}

			RangerMultiResourceAuditHandler auditHandler = new RangerMultiResourceAuditHandler();

			RangerPerfTracer perf = null;

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_SOLRAUTH_REQUEST_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_SOLRAUTH_REQUEST_LOG, "RangerSolrAuthorizer.authorize()");
			}

			String userName = getUserName(context);
			Set<String> userGroups = getGroupsForUser(userName);
			String ip = null;
			Date eventTime = new Date();

			// // Set the IP
			if (useProxyIP) {
				ip = context.getHttpHeader(proxyIPHeader);
			}
			if (ip == null) {
				ip = context.getHttpHeader("REMOTE_ADDR");
			}
			if (ip == null) {
				ip = context.getRemoteAddr();
			}

			// Create the list of requests for access check. Each field is
			// broken
			// into a request
			List<RangerAccessRequestImpl> rangerRequests = new ArrayList<RangerAccessRequestImpl>();
			List<CollectionRequest>   collectionRequests = context.getCollectionRequests();

			if (CollectionUtils.isEmpty(collectionRequests)) {
				// if Collection is empty we set the collection to *. This happens when LIST is done.
				RangerAccessRequestImpl requestForCollection = createRequest(
						userName, userGroups, ip, eventTime, context,
						null);
				if (requestForCollection != null) {
					rangerRequests.add(requestForCollection);
				}
			} else {
				// Create the list of requests for access check. Each field is
				// broken
				// into a request
				for (CollectionRequest collectionRequest : context
						.getCollectionRequests()) {

					RangerAccessRequestImpl requestForCollection = createRequest(
							userName, userGroups, ip, eventTime, context,
							collectionRequest);
					if (requestForCollection != null) {
						rangerRequests.add(requestForCollection);
					}
				}

			}

			if (logger.isDebugEnabled()) {
				logger.debug("rangerRequests.size()=" + rangerRequests.size());
			}

			try {
				// Let's check the access for each request/resource
				for (RangerAccessRequestImpl rangerRequest : rangerRequests) {
					RangerAccessResult result = solrPlugin.isAccessAllowed(
							rangerRequest, auditHandler);
					if (logger.isDebugEnabled()) {
						logger.debug("rangerRequest=" + result);
					}
					if (result == null || !result.getIsAllowed()) {
						isDenied = true;
						// rejecting on first failure
						break;
					}
				}
			} finally {
				auditHandler.flushAudit();
				RangerPerfTracer.log(perf);
			}
		} catch (Throwable t) {
			isDenied = true;
			MiscUtil.logErrorMessageByInterval(logger, t.getMessage(), t);
		}
		AuthorizationResponse response = null;
		if (isDenied) {
			response = new AuthorizationResponse(403);
		} else {
			response = new AuthorizationResponse(200);
		}
		if (logger.isDebugEnabled()) {
			logger.debug( "<== RangerSolrAuthorizer.authorize() result: " + isDenied + "Response : " + response.getMessage());
		}
		return response;
	}

	/**
	 * @param context
	 */
	private void logAuthorizationConext(AuthorizationContext context) {
		try {
			// Note: This method should be called with isDebugEnabled()
			String collections = "";
			int i = -1;
			for (CollectionRequest collectionRequest : context
					.getCollectionRequests()) {
				i++;
				if (i > 0) {
					collections += ",";
				}
				collections += collectionRequest.collectionName;
			}

			String headers = "";
			i = -1;
			@SuppressWarnings("unchecked")
			Enumeration<String> eList = context.getHeaderNames();
			while (eList.hasMoreElements()) {
				i++;
				if (i > 0) {
					headers += ",";
				}
				String header = eList.nextElement();
				String value = context.getHttpHeader(header);
				headers += header + "=" + value;
			}

			String ipAddress = context.getHttpHeader("HTTP_X_FORWARDED_FOR");

			if (ipAddress == null) {
				ipAddress = context.getHttpHeader("REMOTE_HOST");
			}
			if (ipAddress == null) {
				ipAddress = context.getHttpHeader("REMOTE_ADDR");
			}
			if (ipAddress == null) {
				ipAddress = context.getRemoteAddr();
			}

			String userName = getUserName(context);
			Set<String> groups = getGroupsForUser(userName);
			String resource    = context.getResource();
			String solrParams  = "";
			try {
				solrParams = context.getParams().toQueryString();
			} catch (Throwable t) {
				//Exception ignored
			}
			RequestType requestType  = context.getRequestType();
			String 		accessType   = mapToRangerAccessType(context);
			Principal	principal	 = context.getUserPrincipal();

			String contextString = new String("AuthorizationContext: ");
			contextString  = contextString + "context.getResource()= " + ((resource != null ) ? resource : "");
			contextString  = contextString + ", solarParams= " + (( solrParams != null ) ? solrParams : "");
			contextString  = contextString + ", requestType= " + (( requestType != null ) ? requestType : "");
			contextString  = contextString + ", ranger.requestType= " + ((accessType != null ) ? accessType : "");
			contextString  = contextString + ", userPrincipal= " + ((principal != null ) ? principal : "");
			contextString  = contextString + ", userName= "  + userName;
			contextString  = contextString + ", groups= " + groups;
			contextString  = contextString + ", ipAddress= " + ipAddress;
			contextString  = contextString + ", collections= " + collections;
			contextString  = contextString + ", headers= " + headers;

			logger.debug(contextString);
		} catch (Throwable t) {
			logger.error("Error getting request context!!!", t);
		}

	}

	/**
	 * @param userName
	 * @param userGroups
	 * @param ip
	 * @param eventTime
	 * @param context
	 * @param collectionRequest
	 * @return
	 */
	private RangerAccessRequestImpl createRequest(String userName,
			Set<String> userGroups, String ip, Date eventTime,
			AuthorizationContext context, CollectionRequest collectionRequest) {

		String accessType = mapToRangerAccessType(context);
		String action = accessType;
		RangerAccessRequestImpl rangerRequest = createBaseRequest(userName,
				userGroups, ip, eventTime);
		RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
		if (collectionRequest == null) {
			rangerResource.setValue(KEY_COLLECTION, "*");
		} else {
			rangerResource.setValue(KEY_COLLECTION, collectionRequest.collectionName);
		}
		rangerRequest.setResource(rangerResource);
		rangerRequest.setAccessType(accessType);
		rangerRequest.setAction(action);

		return rangerRequest;
	}

	private RangerAccessRequestImpl createBaseRequest(String userName,
			Set<String> userGroups, String ip, Date eventTime) {
		RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
		if (userName != null && !userName.isEmpty()) {
			rangerRequest.setUser(userName);
		}
		if (userGroups != null && userGroups.size() > 0) {
			rangerRequest.setUserGroups(userGroups);
		}
		if (ip != null && !ip.isEmpty()) {
			rangerRequest.setClientIPAddress(ip);
		}
		rangerRequest.setAccessTime(eventTime);
		return rangerRequest;
	}

	private String getUserName(AuthorizationContext context) {
		Principal principal = context.getUserPrincipal();
		if (principal != null) {
			return MiscUtil.getShortNameFromPrincipalName(principal.getName());
		}
		return null;
	}

	/**
	 * @param name
	 * @return
	 */
	private Set<String> getGroupsForUser(String name) {
		return MiscUtil.getGroupsForRequestUser(name);
	}

	String mapToRangerAccessType(AuthorizationContext context) {
		String accessType = ACCESS_TYPE_OTHERS;

		RequestType requestType = context.getRequestType();
		if (RequestType.ADMIN.equals(requestType)) {
			accessType = ACCESS_TYPE_ADMIN;
		} else if (RequestType.READ.equals(requestType)) {
			accessType = ACCESS_TYPE_QUERY;
		} else if (RequestType.WRITE.equals(requestType)) {
			accessType = ACCESS_TYPE_UPDATE;
		} else if (RequestType.UNKNOWN.equals(requestType)) {
			logger.info("UNKNOWN request type. Mapping it to " + accessType
					+ ". Resource=" + context.getResource());
			accessType = ACCESS_TYPE_OTHERS;
		} else {
			logger.info("Request type is not supported. requestType="
					+ requestType + ". Mapping it to " + accessType
					+ ". Resource=" + context.getResource());
		}
		return accessType;
	}

}
