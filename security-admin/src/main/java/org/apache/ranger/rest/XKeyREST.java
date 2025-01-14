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
package org.apache.ranger.rest;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.core.Response;
import org.apache.ranger.biz.KmsKeyMgr;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.view.VXKmsKey;
import org.apache.ranger.view.VXKmsKeyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


@Path("keys")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("KeyMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class XKeyREST {
	private static final Logger logger = LoggerFactory.getLogger(XKeyREST.class);

	private static String UNAUTHENTICATED_MSG = "Unauthenticated : Please check the permission in the policy for the user";
	
	@Autowired
	KmsKeyMgr keyMgr;
		
	@Autowired
	SearchUtil searchUtil;
	
	@Autowired
	RESTErrorUtil restErrorUtil;
	
	/**
	 * Implements the traditional search functionalities for Keys
	 *
	 * @param request
	 * @return
	 */
	@GET
	@Path("/keys")
	@Produces({ "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.SEARCH_KEYS + "\")")
	public VXKmsKeyList searchKeys(@Context HttpServletRequest request, @QueryParam("provider") String provider) {
		VXKmsKeyList vxKmsKeyList = new VXKmsKeyList();
		try{
			vxKmsKeyList = keyMgr.searchKeys(request, provider);
		}catch(Exception e){
			handleError(e);						
		}
		return vxKmsKeyList;
	}
	
	/**
	 * Implements the Rollover key functionality
	 * @param vXKey
	 * @return
	 */
	@PUT
	@Path("/key")
	@Consumes({ "application/json" })
	@Produces({ "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.ROLLOVER_KEYS + "\")")
	public VXKmsKey rolloverKey(@QueryParam("provider") String provider, VXKmsKey vXKey) {
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			String name = vXKey.getName();
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			if(vXKey.getCipher() == null || vXKey.getCipher().trim().isEmpty()){
				vXKey.setCipher(null);
			}
			vxKmsKey = keyMgr.rolloverKey(provider, vXKey);
		}catch(Exception e){
			handleError(e);
		}
		return vxKmsKey;
	}	
	
	/**
	 * Implements the delete key functionality
	 * @param name
	 * @param request
	 */
	@DELETE
	@Path("/key/{alias}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_KEY + "\")")
	public void deleteKey(@PathParam("alias") String name, @QueryParam("provider") String provider, @Context HttpServletRequest request) {
		try{
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			keyMgr.deleteKey(provider, name);
		}catch(Exception e){
			handleError(e);
		}
	}
	
	/**
	 * Implements the create key functionality
	 * @param vXKey
	 * @return
	 */
	@POST
	@Path("/key")
	@Consumes({ "application/json" })
	@Produces({ "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_KEY + "\")")
	public VXKmsKey createKey(@QueryParam("provider") String provider, VXKmsKey vXKey) {
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			String name = vXKey.getName();
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			if(vXKey.getCipher() == null || vXKey.getCipher().trim().isEmpty()){
				vXKey.setCipher(null);
			}
			vxKmsKey = keyMgr.createKey(provider, vXKey);
		}catch(Exception e){
			handleError(e);
		}
		return vxKmsKey;
	}
	
	/**
	 *
	 * @param name
	 * @param provider
	 * @return
	 */
	@GET
	@Path("/key/{alias}")
	@Produces({ "application/json" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_KEY + "\")")
	public VXKmsKey getKey(@PathParam("alias") String name,@QueryParam("provider") String provider){
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			vxKmsKey = keyMgr.getKey(provider, name);
		}catch(Exception e){
			handleError(e);
		}
		return vxKmsKey;
	}
	
	private void handleError(Exception e) {
		String message = e.getMessage();
		if (e instanceof WebApplicationException){
			WebApplicationException uie=(WebApplicationException)e;
			Response response = uie.getResponse();
			if (response.hasEntity()){
				try {
					message = uie.getResponse().readEntity(String.class);
					logger.error(message);

					JsonNode rootNode = JsonUtilsV2.getMapper().readTree(message);
					JsonNode excpNode = rootNode != null ? rootNode.get("RemoteException") : null;
					JsonNode msgNode  = excpNode != null ? excpNode.get("message") : null;

					message = msgNode != null ? msgNode.asText() : null;
				} catch (JsonProcessingException e1) {
					logger.error("Unable to parse the error message, So sending error message as it is - Error : " + e1.getMessage());
				}
			}
		}
		if (message != null && !message.isEmpty()) {
			if (message.contains("Connection refused")) {
				message = "Connection refused: Please check the KMS provider URL and whether the Ranger KMS is running";
			} else if (message.contains("response status of 403") || message.contains("HTTP Status 403")) {
				message = UNAUTHENTICATED_MSG;
			} else if (message.contains("response status of 401") || message.contains("HTTP Status 401 - Authentication required")) {
				message = UNAUTHENTICATED_MSG;
			}
		} else {
			message = UNAUTHENTICATED_MSG;
		}

		throw restErrorUtil.createRESTException(message, MessageEnums.ERROR_SYSTEM);
	}	
}
