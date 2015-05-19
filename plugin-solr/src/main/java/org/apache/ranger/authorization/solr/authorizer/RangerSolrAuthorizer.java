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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.audit.RangerMultiResourceAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;

public class RangerSolrAuthorizer implements AuthorizationPlugin {
	private static final Log logger = LogFactory
			.getLog(RangerSolrAuthorizer.class);

	public static final String PROP_USE_PROXY_IP = "xasecure.solr.use_proxy_ip";
	public static final String PROP_PROXY_IP_HEADER = "xasecure.solr.proxy_ip_header";

	public static final String KEY_COLLECTION = "collection";

	public static final String ACCESS_TYPE_CREATE = "create";
	public static final String ACCESS_TYPE_UPDATE = "update";
	public static final String ACCESS_TYPE_QUERY = "query";
	public static final String ACCESS_TYPE_OTHER = "other";
	public static final String ACCESS_TYPE_ADMIN = "solr_admin";

	private static volatile RangerBasePlugin solrPlugin = null;

	boolean useProxyIP = false;
	String proxyIPHeader = "HTTP_X_FORWARDED_FOR";

	public RangerSolrAuthorizer() {
		logger.info("RangerSolrAuthorizer()");
		if (solrPlugin == null) {
			logger.info("RangerSolrAuthorizer(): init called");
			solrPlugin = new RangerBasePlugin("solr", "solr");
		}
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
			solrPlugin.init();

			useProxyIP = RangerConfiguration.getInstance().getBoolean(
					PROP_USE_PROXY_IP, useProxyIP);
			proxyIPHeader = RangerConfiguration.getInstance().get(
					PROP_PROXY_IP_HEADER, proxyIPHeader);

		} catch (Throwable t) {
			logger.fatal("Error init", t);
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
		// TODO: Change this to Debug only
		if (logger.isInfoEnabled()) {
			logAuthorizationConext(context);
		}

		RangerMultiResourceAuditHandler auditHandler = new RangerMultiResourceAuditHandler();

		String userName = null;
		Set<String> userGroups = null;
		String ip = null;
		Date eventTime = StringUtil.getUTCDate();

		// Set the User and Groups
		Principal principal = context.getUserPrincipal();
		if (principal != null) {
			userName = StringUtils.substringBefore(principal.getName(), "@");
			userGroups = getGroupsForUser(userName);
		}

		// // Set the IP
		if (useProxyIP) {
			ip = context.getHttpHeader(proxyIPHeader);
		}
		if (ip == null) {
			ip = context.getHttpHeader("REMOTE_ADDR");
		}

		String requestData = context.getResource() + ":" + context.getParams();

		// Create the list of requests for access check. Each field is broken
		// into a request
		List<RangerAccessRequestImpl> rangerRequests = new ArrayList<RangerAccessRequestImpl>();
		for (CollectionRequest collectionRequest : context
				.getCollectionRequests()) {

			List<RangerAccessRequestImpl> requestsForCollection = createRequests(
					userName, userGroups, ip, eventTime, context,
					collectionRequest, requestData);
			rangerRequests.addAll(requestsForCollection);
		}

		boolean isDenied = false;
		try {
			// Let's check the access for each request/resource
			for (RangerAccessRequestImpl rangerRequest : rangerRequests) {
				RangerAccessResult result = solrPlugin.isAccessAllowed(
						rangerRequest, auditHandler);
				if (result == null || !result.getIsAllowed()) {
					isDenied = true;
					// rejecting on first failure
					break;
				}
			}
		} finally {
			auditHandler.flushAudit();
		}

		AuthorizationResponse response = null;
		if (isDenied) {
			response = new AuthorizationResponse(403);
		} else {
			response = new AuthorizationResponse(200);
		}
		return response;
	}

	/**
	 * @param context
	 */
	private void logAuthorizationConext(AuthorizationContext context) {
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
			ipAddress = context.getHttpHeader("REMOTE_ADDR");
		}

		Principal principal = context.getUserPrincipal();
		String userName = null;
		if (principal != null) {
			userName = principal.getName();
			userName = StringUtils.substringBefore(userName, "@");
		}

		logger.info("AuthorizationContext: context.getResource()="
				+ context.getResource() + ", solarParams="
				+ context.getParams() + ", requestType="
				+ context.getRequestType() + ", userPrincipal="
				+ context.getUserPrincipal() + ", userName=" + userName
				+ ", ipAddress=" + ipAddress + ", collections=" + collections
				+ ", headers=" + headers);

	}

	/**
	 * @param userName
	 * @param userGroups
	 * @param ip
	 * @param eventTime
	 * @param context
	 * @param collectionRequest
	 * @param requestData
	 * @return
	 */
	private List<RangerAccessRequestImpl> createRequests(String userName,
			Set<String> userGroups, String ip, Date eventTime,
			AuthorizationContext context, CollectionRequest collectionRequest,
			String requestData) {

		List<RangerAccessRequestImpl> requests = new ArrayList<RangerAccessRequestImpl>();
		String accessType = mapToRangerAccessType(context);
		String action = accessType;

		if (collectionRequest.collectionName != null) {
			RangerAccessRequestImpl rangerRequest = createBaseRequest(userName,
					userGroups, ip, eventTime);
			RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
			rangerResource.setValue(KEY_COLLECTION,
					collectionRequest.collectionName);
			rangerRequest.setResource(rangerResource);
			rangerRequest.setAccessType(accessType);
			rangerRequest.setAction(action);

			requests.add(rangerRequest);
		} else {
			logger.fatal("Can't create RangerRequest oject. userName="
					+ userName + ", accessType=" + accessType + ", ip=" + ip
					+ ", collectionRequest=" + collectionRequest);
		}

		return requests;
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

	/**
	 * @param name
	 * @return
	 */
	private Set<String> getGroupsForUser(String name) {
		// TODO: Need to implement this method

		return null;
	}

	String mapToRangerAccessType(AuthorizationContext context) {
		String accessType = ACCESS_TYPE_OTHER;

		RequestType requestType = context.getRequestType();
		if (requestType.equals(RequestType.ADMIN)) {
			accessType = ACCESS_TYPE_ADMIN;
		} else if (requestType.equals(RequestType.READ)) {
			accessType = ACCESS_TYPE_QUERY;
		} else if (requestType.equals(RequestType.WRITE)) {
			accessType = ACCESS_TYPE_UPDATE;
		} else if (requestType.equals(RequestType.UNKNOWN)) {
			logger.info("UNKNOWN request type. Mapping it to " + accessType);
			accessType = ACCESS_TYPE_OTHER;
		} else {
			logger.info("Request type is not supported. requestType="
					+ requestType + ". Mapping it to " + accessType);
		}
		return accessType;
	}

}
