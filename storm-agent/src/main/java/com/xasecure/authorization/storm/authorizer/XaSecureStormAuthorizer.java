package com.xasecure.authorization.storm.authorizer;

import java.security.Principal;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

import com.xasecure.audit.model.EnumRepositoryType;
import com.xasecure.audit.model.StormAuditEvent;
import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;
import com.xasecure.authorization.storm.XaStormAccessVerifier;
import com.xasecure.authorization.storm.XaStormAccessVerifierFactory;
import com.xasecure.authorization.utils.StringUtil;

public class XaSecureStormAuthorizer implements IAuthorizer {

	private static final Logger LOG = LoggerFactory.getLogger(XaSecureStormAuthorizer.class);
	
	private static final String XaSecureModuleName =  XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_XASECURE_MODULE_ACL_NAME_PROP , XaSecureHadoopConstants.DEFAULT_XASECURE_MODULE_ACL_NAME) ;
	
	private static final String repositoryName     = XaSecureConfiguration.getInstance().get(XaSecureHadoopConstants.AUDITLOG_REPOSITORY_NAME_PROP);
	
	private XaStormAccessVerifier xaStormVerifier = XaStormAccessVerifierFactory.getInstance() ;
	
	/**
     * permit() method is invoked for each incoming Thrift request.
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
	
	@Override
	public boolean permit(ReqContext aRequestContext, String aOperationName, Map aTopologyConfigMap) {
		
		boolean accessAllowed = false ;
		
		String topologyName = null ;
		
		try {
			topologyName = (aTopologyConfigMap == null ? "" : (String)aTopologyConfigMap.get(Config.TOPOLOGY_NAME)) ;
	
			if (LOG.isDebugEnabled()) {
				LOG.debug("[req "+ aRequestContext.requestID()+ "] Access "
		                + " from: [" + aRequestContext.remoteAddress() + "]"
		                + " user: [" + aRequestContext.principal() + "],"  
		                + " op:   [" + aOperationName + "],"
		                + "topology: [" + topologyName + "]") ;
				
				if (aTopologyConfigMap != null) {
					for(Object keyObj : aTopologyConfigMap.keySet()) {
						Object valObj = aTopologyConfigMap.get(keyObj) ;
						LOG.debug("TOPOLOGY CONFIG MAP [" + keyObj + "] => [" + valObj + "]");
					}
				}
				else {
					LOG.debug("TOPOLOGY CONFIG MAP is passed as null.") ;
				}
			}
	
			String userName = null ;
			String[] groups = null ;
	
			Principal user = aRequestContext.principal() ;
			
			if (user != null) {
				userName = user.getName() ;
				if (userName != null) {
					UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName) ;
					userName = ugi.getShortUserName() ;
					groups = ugi.getGroupNames() ;
					if (LOG.isDebugEnabled()) {
						LOG.debug("User found from principal [" + user.getName() + "] => user:[" + userName + "], groups:[" + StringUtil.toString(groups) + "]") ;
					}

				}
			}
				
				
			if (userName != null) {
				accessAllowed = xaStormVerifier.isAccessAllowed(userName, groups, aOperationName, topologyName) ;
				if (LOG.isDebugEnabled()) {
					LOG.debug("User found from principal [" + userName + "], groups [" + StringUtil.toString(groups) + "]: verifying using [" + xaStormVerifier.getClass().getName() + "], allowedFlag => [" + accessAllowed + "]");
				}
			}
			else {
				LOG.info("NULL User found from principal [" + user + "]: Skipping authorization;  allowedFlag => [" + accessAllowed + "]");
			}
				
			boolean isAuditEnabled = xaStormVerifier.isAudited(topologyName) ;
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("User found from principal [" + userName + "] and verifying using [" + xaStormVerifier + "], Audit Enabled:" + isAuditEnabled);
			}
			
			if (isAuditEnabled) {
				
				StormAuditEvent auditEvent = new StormAuditEvent() ;
	
				String sessionId = null ;
				String clientIp = null ;
				
				if (aRequestContext != null) {
					sessionId = String.valueOf(aRequestContext.requestID()) ;
					clientIp =  (aRequestContext.remoteAddress() == null ? null : aRequestContext.remoteAddress().getHostAddress() ) ;
				}
				
				try {
					auditEvent.setAclEnforcer(XaSecureModuleName);
					auditEvent.setSessionId(sessionId);
					auditEvent.setResourceType("@ TOPOLOGY"); 
					auditEvent.setAccessType(aOperationName) ;
					auditEvent.setAction(aOperationName);
					auditEvent.setUser(userName);
					auditEvent.setAccessResult((short)(accessAllowed ? 1 : 0));
					auditEvent.setClientIP(clientIp);
					auditEvent.setClientType("Strom REST");
					auditEvent.setEventTime(StringUtil.getUTCDate());
					auditEvent.setRepositoryType(EnumRepositoryType.STORM);
					auditEvent.setRepositoryName(repositoryName) ;
					auditEvent.setRequestData("");
	
					auditEvent.setResourcePath(topologyName);
				
					if(LOG.isDebugEnabled()) {
						LOG.debug("logAuditEvent [" + auditEvent + "] - START");
					}
	
					AuditProviderFactory.getAuditProvider().log(auditEvent);
	
					if(LOG.isDebugEnabled()) {
						LOG.debug("logAuditEvent [" + auditEvent + "] - END");
					}
				}
				catch(Throwable t) {
					LOG.error("ERROR logEvent [" + auditEvent + "]", t);
				}
					
			}
		}
		catch(Throwable t) {
			LOG.error("XaSecureStormAuthorizer found this exception", t);
		}
		finally {
			LOG.info("[req "+ aRequestContext.requestID()+ "] Access "
	                + " from: [" + aRequestContext.remoteAddress() + "]"
	                + " user: [" + aRequestContext.principal() + "],"  
	                + " op:   [" + aOperationName + "],"
	                + "topology: [" + topologyName + "] => returns [" + accessAllowed + "]") ;
		}
		
		return accessAllowed ;
	}
	
	/**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */

	@Override
	public void prepare(Map aStormConfigMap) {
	}
	
}
