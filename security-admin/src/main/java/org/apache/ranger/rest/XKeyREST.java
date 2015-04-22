package org.apache.ranger.rest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.KmsKeyMgr;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.SearchUtil;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.view.VXKmsKey;
import org.apache.ranger.view.VXKmsKeyList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


@Path("keys")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("KeyMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class XKeyREST {
	static Logger logger = Logger.getLogger(XKeyREST.class);
	
	private static String UNAUTHENTICATED_MSG = "Unauthenticated : Please check the premission in the policy for the user";
	
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
	@Produces({ "application/xml", "application/json" })
	public VXKmsKeyList searchKeys(@Context HttpServletRequest request, @QueryParam("provider") String provider) {
		VXKmsKeyList vxKmsKeyList = new VXKmsKeyList();
		try{
			vxKmsKeyList = keyMgr.searchKeys(provider);
			vxKmsKeyList = keyMgr.getFilteredKeyList(request, vxKmsKeyList);
		}catch(Exception e){
			e.printStackTrace();
			handleError(e.getMessage());						
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
	@Produces({ "application/xml", "application/json" })
	public VXKmsKey rolloverKey(@QueryParam("provider") String provider, VXKmsKey vXKey) {
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			String name = vXKey.getName();
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			vxKmsKey = keyMgr.rolloverKey(provider, vXKey);
		}catch(Exception e){
			handleError(e.getMessage());
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
	@Produces({ "application/xml", "application/json" })
	public void deleteKey(@PathParam("alias") String name, @QueryParam("provider") String provider, @Context HttpServletRequest request) {
		try{
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			keyMgr.deleteKey(provider, name);
		}catch(Exception e){
			handleError(e.getMessage());
		}
	}
	
	/**
	 * Implements the create key functionality
	 * @param vXKey
	 * @return
	 */
	@POST
	@Path("/key")
	@Produces({ "application/xml", "application/json" })
	public VXKmsKey createKey(@QueryParam("provider") String provider, VXKmsKey vXKey) {
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			String name = vXKey.getName();
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			vxKmsKey = keyMgr.createKey(provider, vXKey);
		}catch(Exception e){
			handleError(e.getMessage());
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
	@Produces({ "application/xml", "application/json" })
	public VXKmsKey getKey(@PathParam("alias") String name,@QueryParam("provider") String provider){
		VXKmsKey vxKmsKey = new VXKmsKey();
		try{
			if (name == null || name.isEmpty()) {
				throw restErrorUtil.createRESTException("Please provide a valid "
						+ "alias.", MessageEnums.INVALID_INPUT_DATA);
			}
			vxKmsKey = keyMgr.getKey(provider, name);
		}catch(Exception e){
			handleError(e.getMessage());
		}
		return vxKmsKey;
	}
	
	private void handleError(String message) {		
		if(!(message==null) && !(message.isEmpty()) && message.contains("Connection refused")){
			message = "Connection refused : Please check the KMS provider URL and whether the Ranger KMS is running";			
		}else if(!(message==null) && !(message.isEmpty()) && message.contains("response status of 403")){
			message = UNAUTHENTICATED_MSG;
		}else if(!(message==null) && !(message.isEmpty()) && message.contains("response status of 401")){
			message = UNAUTHENTICATED_MSG;
		}	
		throw restErrorUtil.createRESTException(message, MessageEnums.ERROR_SYSTEM);
	}	
}
