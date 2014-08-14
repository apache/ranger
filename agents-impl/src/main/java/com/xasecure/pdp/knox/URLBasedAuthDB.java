/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xasecure.pdp.knox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.hadoop.config.XaSecureConfiguration;
import com.xasecure.pdp.config.Jersey2PolicyRefresher;
import com.xasecure.pdp.config.PolicyChangeListener;
import com.xasecure.pdp.constants.XaSecureConstants;
import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.RolePermission;

public class URLBasedAuthDB implements PolicyChangeListener {

	private static final Log LOG = LogFactory.getLog(URLBasedAuthDB.class) ;

	private static URLBasedAuthDB me = null;
	
	private Jersey2PolicyRefresher refresher = null ;
	
	private PolicyContainer policyContainer = null;
	
	private HashMap<String,Boolean> cachedAuditFlag = new HashMap<String,Boolean>() ;	// needs to be cleaned when ruleList changes
	
	public static URLBasedAuthDB getInstance() {
		if (me == null) {
			synchronized (URLBasedAuthDB.class) {
				URLBasedAuthDB temp = me;
				if (temp == null) {
					me = new URLBasedAuthDB();
					me.init() ;
				}
			}
		}
		return me;
	}

	public static URLBasedAuthDB getInstanceWithBackEndMocked() {
		return new URLBasedAuthDB("instanceWithBackednMocked");
	}
	
	private URLBasedAuthDB() {
		String url 			 = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_KNOX_POLICYMGR_URL_PROP);
		long  refreshInMilli = XaSecureConfiguration.getInstance().getLong(
				XaSecureConstants.XASECURE_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP ,
				XaSecureConstants.XASECURE_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT);
		String sslConfigFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_KNOX_POLICYMGR_SSL_CONFIG_FILE_PROP) ;
		
		String lastStoredFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_KNOX_LAST_SAVED_POLICY_FILE_PROP) ;
		
		refresher = new Jersey2PolicyRefresher(url, refreshInMilli,sslConfigFileName,lastStoredFileName) ;
	
		String saveAsFileName = XaSecureConfiguration.getInstance().get(XaSecureConstants.XASECURE_KNOX_POLICYMGR_URL_SAVE_FILE_PROP) ;
		if (saveAsFileName != null) {
			refresher.setSaveAsFileName(saveAsFileName) ;
		}
		
		if (lastStoredFileName != null) {
			refresher.setLastStoredFileName(lastStoredFileName);
		}	
	}

	private URLBasedAuthDB(String mockName) {
	}
	
	private void init() {
		refresher.setPolicyChangeListener(this);
	}
	
	
	@Override
	public void OnPolicyChange(PolicyContainer aPolicyContainer) {
		setPolicyContainer(aPolicyContainer);
	}


	public boolean isAccessGranted(String topology, String service, String access, String userName, Set<String> groups, 
					String requestIp) {

		boolean accessGranted = false;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Evaluating access for topology: " + topology +
					", service: " + service +
					", access: " + access +
					", requestingIp: " +requestIp +
					", requestingUser: " + userName +
					", requestingUserGroups: " + groups);
		}
		PolicyContainer policyContainer = getPolicyContainer() ;
		
		if (policyContainer == null) {
			LOG.warn("Denying access: policyContainer is null") ;
			return false ;
		}
		
		for(Policy policy :  policyContainer.getAcl()) {
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Evaluating policy: " + policy.toString() ) ;
			}
			
			if (!policy.isEnabled()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Skipping policy: " + policy + ", policy disabled") ;
				}
				continue; // jump to next policy
			}
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Evaluating topology match for policyTopologyList: " + policy.getTopologyList() +
						", requestTopology: " + topology) ;
			}
			
			boolean topologyMatched = false;
			
			List<String> topologyList = policy.getTopologyList();
			if (topologyList == null || topologyList.isEmpty()) {
				LOG.debug("Denying access: policy topologyList is empty") ;
				continue; // jump to next policy
			}
			
			if (topologyList.contains("*") || topologyList.contains(topology)) {
				topologyMatched = true;
				LOG.debug("Policy topologyList matches requested topology");
			}
			
			if (!topologyMatched) {
				for (String policyTopology : topologyList) {
					if (FilenameUtils.wildcardMatch(topology, policyTopology)) {
						topologyMatched = true;
						LOG.debug("Policy topologyList matches requested topology");
						break; // break out of topologyList
					}
				}
			}
			if (!topologyMatched) {
				LOG.debug("Denying access: policy topologyList does not match requested topology") ;
				continue; // jump to next policy
			} else {
				LOG.debug("policy topologyList matches requested topology");
			}
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Evaluating service match for policyServiceList: " + policy.getServiceList() +
						", requestService: " + service) ;
			}
			
			boolean serviceMatched = false;
			
			List<String> serviceList = policy.getServiceList();
			if (serviceList == null || serviceList.isEmpty()) {
				LOG.debug("Denying access: policy serviceList is empty") ;
				continue; // jump to next policy
			}
			
			if (serviceList.contains("*") || serviceList.contains(service)) {
				serviceMatched = true;
				LOG.debug("Policy serviceList matches requested service");
			}
			
			if (!serviceMatched) {
				for (String policyService : serviceList) {
					if (FilenameUtils.wildcardMatch(service, policyService)) {
						serviceMatched = true;
						LOG.debug("Policy serviceList matches requested service");
						break; // break out of serviceList 
					}
				}
			}
			if (!serviceMatched) {
				LOG.debug("Denying access: policy serviceList does not match requested service") ;
				continue; // jump to next policy
			} else {
				LOG.debug("Policy serviceList matches requested service");
			}
			
			LOG.debug("Checking accessType, IP, User, Group based permission");
			if ( policy.getPermissions() == null 
					|| policy.getPermissions().isEmpty()) {
				LOG.debug("Policy not applicable, no user or group based permission");
			}
			
			for (RolePermission rp : policy.getPermissions()) {
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Evaluating RolePermission: " + rp);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Checking accessTypeMatch for rolePermissionAccesType: " 
							+ rp.getAccess() + ", requestAccessType: " + access);
				}
				
				if (rp.getAccess().contains(access)) {
					
					LOG.debug("RolePermission accessType matches request accessType");
					
					boolean ipMatched = false;
					List<String> ipList = rp.getIpAddress();
					if (LOG.isDebugEnabled()) {
						LOG.debug("Checking ipMatch for rolePermissionIpList: " + ipList +
								", requestIP: " + requestIp);
					}
					
					if (ipList == null || ipList.isEmpty()) {
						LOG.debug("RolePermission does not require IP Matching");
						ipMatched = true;
					} else if ( ipList.contains("*") ) {
						LOG.debug("RolePermission allows any IP: *");
						ipMatched = true;
					} else {
						for (String ip : ipList) {
							if (ipMatches(ip, requestIp)) {
								LOG.debug("RolePermission IP matches request IP");
								ipMatched = true;
								break;// break out of ipList
							}
						}
					}
					
					if (!ipMatched) {
						// ip not matched, jump to next RolePermission check
						LOG.debug("Request IP does not match RolePermission");
						continue; // jump to next rolePermission
					} else {
						LOG.debug("Request IP matches RolePermission");
					}
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("Checking userMatch for rolePermissionUsers: " 
								+ rp.getUsers() + ", requestUser: " + userName);
					}
					
					if ( rp.getUsers() != null && rp.getUsers().contains(userName) ) {
						LOG.debug("Request user matches RolePermission");
						return true ;
					}
					LOG.debug("RolePermission does not permit request by request user, would check by groups");
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("Checking groupMatch for rolePermissionGroups: " 
								+ rp.getGroups() + ", requestGroups: " + groups);
					}
					
					for(String ug : groups) {
						if ( rp.getGroups() != null && rp.getGroups().contains(ug)) {
							LOG.debug("Request userGroups matches RolePermission");
							return true ;
						}
					}
					LOG.debug("RolePermission does not permit request by request user groups");
					
					if (rp.getGroups().contains(XaSecureConstants.PUBLIC_ACCESS_ROLE)) {
						LOG.debug("RolePermission applies to public group");
						return true ;
					}
					
					LOG.debug("RolePermission does not permit by users, groups or public group");
				} else {
					LOG.debug("rolePermissionAccessType does not match requestAccessType");
				}
			}
		}
		LOG.debug("No matching policy permission found, denying access");
		return accessGranted;
	}
	
	public boolean isAuditEnabled(String topology, String service) {
		
		boolean auditEnabled = false;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Checcking whether audit is enabled for topology: " + topology +
					", service: " + service );
		}
		
		PolicyContainer policyContainer = getPolicyContainer() ;
		if (policyContainer == null) {
			LOG.warn("PolicyContainer is null") ;
			return false ;
		}
		
		for(Policy policy :  policyContainer.getAcl()) {
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Evaluating policy: " + policy) ;
			}
			
			if (!policy.isEnabled()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Skipping policy: " + policy + ", policy disabled") ;
				}
				continue; // jump to next policy
			}
			
			if (policy.getAuditInd() == 0) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Skipping policy: " + policy + ", policy audit disabled") ;
				}
				continue; // jump to next policy
			}
			
			boolean topologyMatched = false;
			
			List<String> topologyList = policy.getTopologyList();
			if (topologyList == null || topologyList.isEmpty()) {
				LOG.debug("Policy not applicable: policy topologyList is empty") ;
				continue; // jump to next policy
			}
			
			if (topologyList.contains("*") || topologyList.contains(topology)) {
				topologyMatched = true;
				LOG.debug("Policy topologyList matches requested topology");
			}
			
			if (!topologyMatched) {
				for (String policyTopology : topologyList) {
					if (FilenameUtils.wildcardMatch(topology, policyTopology)) {
						topologyMatched = true;
						LOG.debug("Policy topologyList matches requested topology");
						break; // break out of topologyList check
					}
				}
			}
			if (!topologyMatched) {
				LOG.debug("Policy not applicable: polocy topologyList does not match requested topology") ;
				continue; // jump to next policy
			} else {
				LOG.debug("Policy topologyList matches requested topology");
			}
			
			boolean serviceMatched = false;
			
			List<String> serviceList = policy.getServiceList();
			if (serviceList == null || serviceList.isEmpty()) {
				LOG.debug("Policy not applicable: serviceList is empty") ;
				continue; // jump to next policy
			}
			
			if (serviceList.contains("*") || serviceList.contains(service)) {
				serviceMatched = true;
				LOG.debug("Policy serviceList matches requested service");
			}
			
			if (!serviceMatched) {
				for (String policyService : serviceList) {
					if (FilenameUtils.wildcardMatch(service, policyService)) {
						serviceMatched = true;
						LOG.debug("Policy serviceList matches requested service");
						break; // break out of serviceList check
					}
				}
			}
			if (!serviceMatched) {
				LOG.debug("Policy not applicable: policy serviceList does not match requested service") ;
				continue; // jump to next policy
			} else {
				LOG.debug("Policy serviceList matches requested service");
			}
			auditEnabled = true;;
			break; // break out of policyList check
		}
		return auditEnabled;
	}
		
	public PolicyContainer getPolicyContainer() {
		return policyContainer;
	}

	
	synchronized void setPolicyContainer(PolicyContainer aPolicyContainer) {
		
		for(Policy p : aPolicyContainer.getAcl()) {
			for(RolePermission rp : p.getPermissions()) {
				// lowercase accesType value stings
				List<String> rpaccess = rp.getAccess() ;
				if (rpaccess != null && rpaccess.size() > 0) {
					List<String> temp = new ArrayList<String>() ;
					for(String s : rpaccess) {
						temp.add(s.toLowerCase()) ;
					}
					rp.setAccess(temp);
				}
			}
		}
		
		this.policyContainer = aPolicyContainer ;
		this.cachedAuditFlag.clear(); 
	}


	private boolean ipMatches(String policyIp, String requestIp) {
		if (policyIp == null) {
			return false;
		}
		policyIp = policyIp.trim();
		if (policyIp.isEmpty()) {
			return false;
		}
		boolean ipMatched = false;
		boolean wildEnd = false;
		if (policyIp.contains(".")) {
			while (policyIp.endsWith(".*")) {
				wildEnd = true;
				policyIp = policyIp.substring(0, policyIp.lastIndexOf(".*"));
			}
			if (wildEnd) {
				policyIp = policyIp + ".";
			}
		} else if (policyIp.contains(":")) {
			while (policyIp.endsWith(":*")) {
				wildEnd = true;
				policyIp = policyIp.substring(0, policyIp.lastIndexOf(":*"));
			}
			if (wildEnd) {
				policyIp = policyIp + ":";
			}
		}
		if (wildEnd && requestIp.startsWith(policyIp)) {
			ipMatched = true;
		} else if (policyIp.equals(requestIp)) {
			ipMatched = true;
		}
		return ipMatched;
	}
	
}
