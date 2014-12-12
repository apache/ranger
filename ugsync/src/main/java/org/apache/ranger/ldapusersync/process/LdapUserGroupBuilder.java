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

 package org.apache.ranger.ldapusersync.process;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;

import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usergroupsync.UserGroupSource;

public class LdapUserGroupBuilder implements UserGroupSource {
	
	private static final Logger LOG = Logger.getLogger(LdapUserGroupBuilder.class);
	
	private static final int PAGE_SIZE = 100;
	
	private UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();
	
	private String userSearchBase;
	private String extendedSearchFilter;
	private String userNameAttribute;
	
	private LdapContext ldapContext;
	private SearchControls searchControls;
	
	private boolean userNameCaseConversionFlag = false ;
	private boolean groupNameCaseConversionFlag = false ;
	private boolean userNameLowerCaseFlag = false ;
	private boolean groupNameLowerCaseFlag = false ;

	
	public static void main(String[] args) throws Throwable {
		LdapUserGroupBuilder  ugBuilder = new LdapUserGroupBuilder();
		ugBuilder.init();
	}
	
	public LdapUserGroupBuilder() {
		LOG.info("LdapUserGroupBuilder created") ;
		
		String userNameCaseConversion = config.getUserNameCaseConversion() ;
		
		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion)) {
		    userNameCaseConversionFlag = false ;
		}
		else {
		    userNameCaseConversionFlag = true ;
		    userNameLowerCaseFlag = UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion) ;
		}
		
		String groupNameCaseConversion = config.getGroupNameCaseConversion() ;
		
		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion)) {
		    groupNameCaseConversionFlag = false ;
		}
		else {
		    groupNameCaseConversionFlag = true ;
		    groupNameLowerCaseFlag = UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion) ;
		}
		
	}

	@Override
	public void init() {
		// do nothing
	}
	
	private void createLdapContext() throws Throwable {
		LOG.info("LdapUserGroupBuilder initialization started");
		String ldapUrl = config.getLdapUrl();
		String ldapBindDn = config.getLdapBindDn();
		String ldapBindPassword = config.getLdapBindPassword();
		String ldapAuthenticationMechanism = config.getLdapAuthenticationMechanism();
		
		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY, 
		    "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, ldapUrl);
		
		env.put(Context.SECURITY_PRINCIPAL, ldapBindDn);
		env.put(Context.SECURITY_CREDENTIALS, ldapBindPassword);
		env.put(Context.SECURITY_AUTHENTICATION, ldapAuthenticationMechanism);
		env.put(Context.REFERRAL, "follow") ;

		ldapContext = new InitialLdapContext(env, null);
		
		userSearchBase = config.getUserSearchBase();
		int  userSearchScope = config.getUserSearchScope();
		String userObjectClass = config.getUserObjectClass();
		String userSearchFilter = config.getUserSearchFilter();
		extendedSearchFilter = "(objectclass=" + userObjectClass + ")";
		if (userSearchFilter != null && !userSearchFilter.trim().isEmpty()) {
			String customFilter = userSearchFilter.trim();
			if (!customFilter.startsWith("(")) {
				customFilter = "(" + customFilter + ")";
			}
			extendedSearchFilter = "(&" + extendedSearchFilter + customFilter + ")";
		}
		
		userNameAttribute = config.getUserNameAttribute();
		
		Set<String> userSearchAttributes = new HashSet<String>();
		userSearchAttributes.add(userNameAttribute);
		
		Set<String> userGroupNameAttributeSet = config.getUserGroupNameAttributeSet();
		for (String useGroupNameAttribute : userGroupNameAttributeSet) {
			userSearchAttributes.add(useGroupNameAttribute);
		}
		
		searchControls = new SearchControls();
		searchControls.setSearchScope(userSearchScope);
		searchControls.setReturningAttributes(userSearchAttributes.toArray(
				new String[userSearchAttributes.size()]));
		
		if (LOG.isInfoEnabled()) {
			LOG.info("LdapUserGroupBuilder initialization completed with --  "
					+ "ldapUrl: " + ldapUrl 
					+ ",  ldapBindDn: " + ldapBindDn
					+ ",  ldapBindPassword: ***** " 
					+ ",  ldapAuthenticationMechanism: "
					+ ldapAuthenticationMechanism + ",  userSearchBase: "
					+ userSearchBase + ",  userSearchScope: " + userSearchScope
					+ ",  userObjectClass: " + userObjectClass
					+ ",  userSearchFilter: " + userSearchFilter
					+ ",  extendedSearchFilter: " + extendedSearchFilter
					+ ",  userNameAttribute: " + userNameAttribute
					+ ",  userSearchAttributes: " + userSearchAttributes	);
		}
		
	}
	
	private void closeLdapContext() throws Throwable {
		if (ldapContext != null) {
			ldapContext.close();
		}
	}
	
	@Override
	public boolean isChanged() {
		// we do not want to get the full ldap dit and check whether anything has changed
		return true;
	}

	@Override
	public void updateSink(UserGroupSink sink) throws Throwable {
		LOG.info("LDAPUserGroupBuilder updateSink started");
    NamingEnumeration<SearchResult> searchResultEnum = null;
		try {
			createLdapContext();
			
			// Activate paged results
		    byte[] cookie = null;
		    ldapContext.setRequestControls(new Control[]{
		        new PagedResultsControl(PAGE_SIZE, Control.NONCRITICAL) });
		    int total;
		    
			int counter = 0;
			do {
				searchResultEnum = ldapContext
					.search(userSearchBase, extendedSearchFilter,
							searchControls);
				while (searchResultEnum.hasMore()) { 
					// searchResults contains all the user entries
					final SearchResult userEntry = searchResultEnum.next();
					String userName = (String) userEntry.getAttributes()
						.get(userNameAttribute).get();
				
				
					if (userNameCaseConversionFlag) {
						if (userNameLowerCaseFlag) {
							userName = userName.toLowerCase() ;
						}
						else {
							userName = userName.toUpperCase() ;
						}
					}
				
					Set<String> groups = new HashSet<String>();
					Set<String> userGroupNameAttributeSet = config.getUserGroupNameAttributeSet();
					for (String useGroupNameAttribute : userGroupNameAttributeSet) {
						Attribute userGroupfAttribute = userEntry.getAttributes().get(useGroupNameAttribute);
						if(userGroupfAttribute != null) {
							NamingEnumeration<?> groupEnum = userGroupfAttribute.getAll();
							while (groupEnum.hasMore()) {
								String gName = getShortGroupName((String) groupEnum
									.next());
								if (groupNameCaseConversionFlag) {
									if (groupNameLowerCaseFlag) {
										gName = gName.toLowerCase();
									} else {
										gName = gName.toUpperCase();
									}
								}
								groups.add(gName);
							}
						}
					}

					List<String> groupList = new ArrayList<String>(groups);
					counter++;
					if (counter <= 2000) { 
						if (LOG.isInfoEnabled()) {
							LOG.info("Updating user count: " + counter
								+ ", userName: " + userName + ", groupList: "
								+ groupList);
						}
					} else {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Updating user count: " + counter
								+ ", userName: " + userName + ", groupList: "
								+ groupList);
						}
					}
					try {
						sink.addOrUpdateUser(userName, groupList);
					} catch (Throwable t) {
						LOG.error("sink.addOrUpdateUser failed with exception: " + t.getMessage()
							+ ", for user: " + userName
							+ ", groups: " + groupList);
					}
				}
				
				// Examine the paged results control response
		        Control[] controls = ldapContext.getResponseControls();
		        if (controls != null) {
		        	for (int i = 0; i < controls.length; i++) {
		        		if (controls[i] instanceof PagedResultsResponseControl) {
		        			PagedResultsResponseControl prrc =
		                             (PagedResultsResponseControl)controls[i];
		        			total = prrc.getResultSize();
		        			if (total != 0) {
		        				LOG.debug("END-OF-PAGE total : " + total);
		        			} else {
		        				LOG.debug("END-OF-PAGE total : unknown");
		        			}
		        			cookie = prrc.getCookie();
		        		}
		        	}
		        } else {
		        	LOG.debug("No controls were sent from the server");
		        }
		        // Re-activate paged results
		        ldapContext.setRequestControls(new Control[]{
		        		new PagedResultsControl(PAGE_SIZE, cookie, Control.CRITICAL) });
			} while (cookie != null);
			LOG.info("LDAPUserGroupBuilder.updateSink() completed with user count: "
					+ counter);
		} finally {
      if (searchResultEnum != null) {
        searchResultEnum.close();
      }
			closeLdapContext();
		}
	}
	
	private static String getShortGroupName(String longGroupName) throws InvalidNameException {
		if (longGroupName == null) {
			return null;
		}
		StringTokenizer stc = new StringTokenizer(longGroupName, ",");
		String firstToken = stc.nextToken();
		StringTokenizer ste = new StringTokenizer(firstToken, "=");
		String groupName =  ste.nextToken();
		if (ste.hasMoreTokens()) {
			groupName = ste.nextToken();
		}
		groupName = groupName.trim();
		LOG.info("longGroupName: " + longGroupName + ", groupName: " + groupName);
		return groupName;
	}
	
}
