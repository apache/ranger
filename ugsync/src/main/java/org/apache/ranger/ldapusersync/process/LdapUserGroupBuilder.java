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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;

import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.usergroupsync.AbstractUserGroupSource;
import org.apache.ranger.usergroupsync.UserGroupSink;

public class LdapUserGroupBuilder extends AbstractUserGroupSource {
	
	private static final Logger LOG = Logger.getLogger(LdapUserGroupBuilder.class);
	
	private static final int PAGE_SIZE = 500;

  private String ldapUrl;
  private String ldapBindDn;
  private String ldapBindPassword;
  private String ldapAuthenticationMechanism;
  private String ldapReferral;
  private String searchBase;

  private String[] userSearchBase;
	private String userNameAttribute;
  private int    userSearchScope;
  private String userObjectClass;
  private String userSearchFilter;
  private String extendedUserSearchFilter;
  private SearchControls userSearchControls;
  private Set<String> userGroupNameAttributeSet;

  private boolean pagedResultsEnabled = true;
  private int pagedResultsSize = 500;

  private boolean groupSearchFirstEnabled = false;
  private boolean userSearchEnabled = false;
  private boolean groupSearchEnabled = true;
  private String[] groupSearchBase;
  private int    groupSearchScope;
  private String groupObjectClass;
  private String groupSearchFilter;
  private String extendedGroupSearchFilter;
  private String extendedAllGroupsSearchFilter;
  private SearchControls groupSearchControls;
  private String groupMemberAttributeName;
  private String groupNameAttribute;

	private LdapContext ldapContext;
	StartTlsResponse tls;

	private boolean userNameCaseConversionFlag = false ;
	private boolean groupNameCaseConversionFlag = false ;
	private boolean userNameLowerCaseFlag = false ;
	private boolean groupNameLowerCaseFlag = false ;

  private boolean  groupUserMapSyncEnabled = false;

  private Map<String, UserInfo> userGroupMap;
  
	public static void main(String[] args) throws Throwable {
		LdapUserGroupBuilder  ugBuilder = new LdapUserGroupBuilder();
		ugBuilder.init();
	}
	
	public LdapUserGroupBuilder() {
		super();
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
	public void init() throws Throwable{		
		setConfig();
	}
	
	private void createLdapContext() throws Throwable {
		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY, 
				"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, ldapUrl);
		if (ldapUrl.startsWith("ldaps") && (config.getSSLTrustStorePath() != null && !config.getSSLTrustStorePath().trim().isEmpty())) {
			env.put("java.naming.ldap.factory.socket", "org.apache.ranger.ldapusersync.process.CustomSSLSocketFactory");
		}

		ldapContext = new InitialLdapContext(env, null);
		if (!ldapUrl.startsWith("ldaps")) {
			if (config.isStartTlsEnabled()) {
				tls = (StartTlsResponse) ldapContext.extendedOperation(new StartTlsRequest());
				if (config.getSSLTrustStorePath() != null && !config.getSSLTrustStorePath().trim().isEmpty()) {
					tls.negotiate(CustomSSLSocketFactory.getDefault());
				} else {
					tls.negotiate();
				}
				LOG.info("Starting TLS session...");
			}
		}

		ldapContext.addToEnvironment(Context.SECURITY_PRINCIPAL, ldapBindDn);
		ldapContext.addToEnvironment(Context.SECURITY_CREDENTIALS, ldapBindPassword);
		ldapContext.addToEnvironment(Context.SECURITY_AUTHENTICATION, ldapAuthenticationMechanism);
		ldapContext.addToEnvironment(Context.REFERRAL, ldapReferral) ;
	}
	
	private void setConfig() throws Throwable {
		LOG.info("LdapUserGroupBuilder initialization started");

		groupSearchFirstEnabled =   config.isGroupSearchFirstEnabled();
		userSearchEnabled =   config.isUserSearchEnabled();
		groupSearchEnabled =   config.isGroupSearchEnabled();
    ldapUrl = config.getLdapUrl();
    ldapBindDn = config.getLdapBindDn();
    ldapBindPassword = config.getLdapBindPassword();
    //ldapBindPassword = "admin-password";
    ldapAuthenticationMechanism = config.getLdapAuthenticationMechanism();
    ldapReferral = config.getContextReferral();
		searchBase = config.getSearchBase();
		
		userSearchBase = config.getUserSearchBase().split(";");
		userSearchScope = config.getUserSearchScope();
		userObjectClass = config.getUserObjectClass();
		userSearchFilter = config.getUserSearchFilter();
		extendedUserSearchFilter = "(objectclass=" + userObjectClass + ")";
		if (userSearchFilter != null && !userSearchFilter.trim().isEmpty()) {
			String customFilter = userSearchFilter.trim();
			if (!customFilter.startsWith("(")) {
				customFilter = "(" + customFilter + ")";
			}
			
			extendedUserSearchFilter = "(&" + extendedUserSearchFilter + customFilter + ")";
		}
		
		userNameAttribute = config.getUserNameAttribute();
		
		Set<String> userSearchAttributes = new HashSet<String>();
		userSearchAttributes.add(userNameAttribute);
		// For Group based search, user's group name attribute should not be added to the user search attributes
		if (!groupSearchFirstEnabled && !groupSearchEnabled) { 
			userGroupNameAttributeSet = config.getUserGroupNameAttributeSet();
			for (String useGroupNameAttribute : userGroupNameAttributeSet) {
				userSearchAttributes.add(useGroupNameAttribute);
			}
		}
		
		userSearchControls = new SearchControls();
		userSearchControls.setSearchScope(userSearchScope);
		userSearchControls.setReturningAttributes(userSearchAttributes.toArray(
				new String[userSearchAttributes.size()]));

    pagedResultsEnabled =   config.isPagedResultsEnabled();
    pagedResultsSize =   config.getPagedResultsSize();

    groupSearchBase = config.getGroupSearchBase().split(";");
    groupSearchScope = config.getGroupSearchScope();
    groupObjectClass = config.getGroupObjectClass();
    groupSearchFilter = config.getGroupSearchFilter();
    groupMemberAttributeName =  config.getUserGroupMemberAttributeName();
    groupNameAttribute = config.getGroupNameAttribute();

    extendedGroupSearchFilter = "(objectclass=" + groupObjectClass + ")";
    if (groupSearchFilter != null && !groupSearchFilter.trim().isEmpty()) {
      String customFilter = groupSearchFilter.trim();
      if (!customFilter.startsWith("(")) {
        customFilter = "(" + customFilter + ")";
      }
      extendedGroupSearchFilter = extendedGroupSearchFilter + customFilter;
    }
    extendedAllGroupsSearchFilter = "(&"  + extendedGroupSearchFilter + ")";
    if (!groupSearchFirstEnabled) {
      extendedGroupSearchFilter =  "(&"  + extendedGroupSearchFilter + "(|(" + groupMemberAttributeName + "={0})(" + groupMemberAttributeName + "={1})))";
    }
    groupUserMapSyncEnabled = config.isGroupUserMapSyncEnabled();

    groupSearchControls = new SearchControls();
    groupSearchControls.setSearchScope(groupSearchScope);
    
    Set<String> groupSearchAttributes = new HashSet<String>();
    groupSearchAttributes.add(groupNameAttribute);
    groupSearchAttributes.add(groupMemberAttributeName);
	
    groupSearchControls.setReturningAttributes(groupSearchAttributes.toArray(
			new String[groupSearchAttributes.size()]));

		if (LOG.isInfoEnabled()) {
			LOG.info("LdapUserGroupBuilder initialization completed with --  "
					+ "ldapUrl: " + ldapUrl 
					+ ",  ldapBindDn: " + ldapBindDn
					+ ",  ldapBindPassword: ***** " 
					+ ",  ldapAuthenticationMechanism: " + ldapAuthenticationMechanism
          + ",  searchBase: " + searchBase
          + ",  userSearchBase: " + Arrays.toString(userSearchBase)
          + ",  userSearchScope: " + userSearchScope
					+ ",  userObjectClass: " + userObjectClass
					+ ",  userSearchFilter: " + userSearchFilter
					+ ",  extendedUserSearchFilter: " + extendedUserSearchFilter
					+ ",  userNameAttribute: " + userNameAttribute
					+ ",  userSearchAttributes: " + userSearchAttributes
          + ",  userGroupNameAttributeSet: " + userGroupNameAttributeSet
          + ",  pagedResultsEnabled: " + pagedResultsEnabled
          + ",  pagedResultsSize: " + pagedResultsSize
          + ",  groupSearchEnabled: " + groupSearchEnabled
          + ",  groupSearchBase: " + Arrays.toString(groupSearchBase)
          + ",  groupSearchScope: " + groupSearchScope
          + ",  groupObjectClass: " + groupObjectClass
          + ",  groupSearchFilter: " + groupSearchFilter
          + ",  extendedGroupSearchFilter: " + extendedGroupSearchFilter
          + ",  extendedAllGroupsSearchFilter: " + extendedAllGroupsSearchFilter
          + ",  groupMemberAttributeName: " + groupMemberAttributeName
          + ",  groupNameAttribute: " + groupNameAttribute
          + ", groupSearchAttributes: " + groupSearchAttributes
          + ",  groupUserMapSyncEnabled: " + groupUserMapSyncEnabled
          + ", groupSearchFirstEnabled: " + groupSearchFirstEnabled
          + ", userSearchEnabled: " + userSearchEnabled
          + ",  ldapReferral: " + ldapReferral
      );
		}

	}
	
	private void closeLdapContext() throws Throwable {
		if (tls != null) {
			tls.close();
		}
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
		userGroupMap = new HashMap<String, UserInfo>();
		if (!groupSearchFirstEnabled) {
			LOG.info("Performing user search first");
			getUsers(sink);
			
			LOG.debug("Total No. of users saved = " + userGroupMap.size());
			//Iterator<UserInfo> userInfoIterator = userGroupMap.
			for (UserInfo userInfo : userGroupMap.values()) {
				String userName = userInfo.getUserName();
				if (groupSearchEnabled) {
					// Perform group search
					LOG.info("groupSearch is enabled, would search for groups and compute memberships");
					getGroups(sink, userInfo);
				}
				List<String> groupList = userInfo.getGroups();
				if (userNameCaseConversionFlag) {
					if (userNameLowerCaseFlag) {
						userName = userName.toLowerCase() ;
					}
					else {
						userName = userName.toUpperCase() ;
					}
				}

				if (userNameRegExInst != null) {
					userName = userNameRegExInst.transform(userName);
				}
				try {
					sink.addOrUpdateUser(userName, groupList);
				} catch (Throwable t) {
					LOG.error("sink.addOrUpdateUser failed with exception: " + t.getMessage()
					+ ", for user: " + userName
					+ ", groups: " + groupList);
				}
			}
			
		} else {
			LOG.info("Performing Group search first");
			getGroups(sink, null);
			if (userSearchEnabled) {
				LOG.info("User search is enabled and hence computing user membership.");
				getUsers(sink);
			} else {
				LOG.info("User search is disabled and hence using the group member attribute for username.");
				// Go through the userInfo map and update ranger admin.
				for (UserInfo userInfo : userGroupMap.values()) {
					String userName = getShortUserName(userInfo.getUserFullName());
					List<String> groupList = userInfo.getGroups();
					if (userNameCaseConversionFlag) {
						if (userNameLowerCaseFlag) {
							userName = userName.toLowerCase() ;
						}
						else {
							userName = userName.toUpperCase() ;
						}
					}

					if (userNameRegExInst != null) {
						userName = userNameRegExInst.transform(userName);
					}
					
					try {
						sink.addOrUpdateUser(userName, groupList);
					} catch (Throwable t) {
						LOG.error("sink.addOrUpdateUser failed with exception: " + t.getMessage()
						+ ", for user: " + userName
						+ ", groups: " + groupList);
					}
				}
			}
		}
	}
	
	private void getUsers(UserGroupSink sink) throws Throwable {
		UserInfo userInfo;
		NamingEnumeration<SearchResult> userSearchResultEnum = null;
		NamingEnumeration<SearchResult> groupSearchResultEnum = null;
		try {
			createLdapContext();
			int total;
			// Activate paged results
			if (pagedResultsEnabled)   {
				ldapContext.setRequestControls(new Control[]{
						new PagedResultsControl(pagedResultsSize, Control.NONCRITICAL) });
			}

			// When multiple OUs are configured, go through each OU as the user search base to search for users.
			for (int ou=0; ou<userSearchBase.length; ou++) {
				byte[] cookie = null;
				int counter = 0;
				try {
				do {
					userSearchResultEnum = ldapContext
							.search(userSearchBase[ou], extendedUserSearchFilter,
									userSearchControls);
					
					while (userSearchResultEnum.hasMore()) {
						// searchResults contains all the user entries
						final SearchResult userEntry = userSearchResultEnum.next();

						if (userEntry == null)  {
							if (LOG.isInfoEnabled())  {
								LOG.info("userEntry null, skipping sync for the entry");
							}
							continue;
						}

						Attributes attributes =   userEntry.getAttributes();
						if (attributes == null)  {
							if (LOG.isInfoEnabled())  {
								LOG.info("attributes  missing for entry " + userEntry.getNameInNamespace() +
										", skipping sync");
							}
							continue;
						}

						Attribute userNameAttr  = attributes.get(userNameAttribute);
						if (userNameAttr == null)  {
							if (LOG.isInfoEnabled())  {
								LOG.info(userNameAttribute + " missing for entry " + userEntry.getNameInNamespace() +
										", skipping sync");
							}
							continue;
						}

						String userName = (String) userNameAttr.get();

						if (userName == null || userName.trim().isEmpty())  {
							if (LOG.isInfoEnabled())  {
								LOG.info(userNameAttribute + " empty for entry " + userEntry.getNameInNamespace() +
										", skipping sync");
							}
							continue;
						}

						if (!groupSearchFirstEnabled) {
							userInfo = new UserInfo(userName, userEntry.getNameInNamespace());
							Set<String> groups = new HashSet<String>();

							// Get all the groups from the group name attribute of the user only when group search is not enabled.
							if (!groupSearchEnabled) {
								for (String useGroupNameAttribute : userGroupNameAttributeSet) {
									Attribute userGroupfAttribute = userEntry.getAttributes().get(useGroupNameAttribute);
									if (userGroupfAttribute != null) {
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
											if (groupNameRegExInst != null) {
												gName = groupNameRegExInst.transform(gName);
											}
											groups.add(gName);
										}
									}
								}
							}

							userInfo.addGroups(groups);
							
							//populate the userGroupMap with username, userInfo. 
							//userInfo contains details of user that will be later used for
							//group search to compute group membership as well as to call sink.addOrUpdateUser()
							if (userGroupMap.containsKey(userName)) {
								LOG.warn("user object with username " + userName + " already exists and is replaced with the latest user object." );
							}
							userGroupMap.put(userName, userInfo);

							//List<String> groupList = new ArrayList<String>(groups);
							List<String> groupList = userInfo.getGroups();
							counter++;
							if (counter <= 2000) { 
								if (LOG.isInfoEnabled()) {
									LOG.info("Updating user count: " + counter
											+ ", userName: " + userName + ", groupList: "
											+ groupList);
								}
								if ( counter == 2000 ) {
									LOG.info("===> 2000 user records have been synchronized so far. From now on, only a summary progress log will be written for every 100 users. To continue to see detailed log for every user, please enable Trace level logging. <===");
								}
							} else {
								if (LOG.isTraceEnabled()) {
									LOG.trace("Updating user count: " + counter
											+ ", userName: " + userName + ", groupList: "
											+ groupList);
								} else  {
									if ( counter % 100 == 0) {
										LOG.info("Synced " + counter + " users till now");
									}
								}
							}
						} else {
							// If the user from the search result is present in the usersList, 
							// then update user name in the userInfo map with the value from the search result
							// and update ranger admin.
							String userFullName = (userEntry.getNameInNamespace()).toLowerCase();
							LOG.debug("Chekcing if the user " + userFullName + " is part of the retrieved groups");
							
							userInfo = userGroupMap.get(userFullName);
							if (userInfo == null) {
								userInfo = userGroupMap.get(userName.toLowerCase());
							}
							if (userInfo != null) {
								counter++;
								LOG.info("Updating username for " + userFullName + " with " + userName);
								userInfo.updateUserName(userName);
								List<String> groupList = userInfo.getGroups();
								if (userNameCaseConversionFlag) {
									if (userNameLowerCaseFlag) {
										userName = userName.toLowerCase() ;
									}
									else {
										userName = userName.toUpperCase() ;
									}
								}

								if (userNameRegExInst != null) {
									userName = userNameRegExInst.transform(userName);
								}
								
								try {
									sink.addOrUpdateUser(userName, groupList);
								} catch (Throwable t) {
									LOG.error("sink.addOrUpdateUser failed with exception: " + t.getMessage()
									+ ", for user: " + userName
									+ ", groups: " + groupList);
								}
							} 
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
					if (pagedResultsEnabled)   {
						ldapContext.setRequestControls(new Control[]{
								new PagedResultsControl(PAGE_SIZE, cookie, Control.CRITICAL) });
					}
				} while (cookie != null);
				LOG.info("LDAPUserGroupBuilder.getUsers() completed with user count: "
						+ counter);
				} catch (Throwable t) {
					LOG.error("LDAPUserGroupBuilder.getUsers() failed with exception: " + t);
					LOG.info("LDAPUserGroupBuilder.getUsers() user count: "
							+ counter);
				}
			}

		} finally {
			if (userSearchResultEnum != null) {
				userSearchResultEnum.close();
			}
			if (groupSearchResultEnum != null) {
				groupSearchResultEnum.close();
			}
			closeLdapContext();
		}
	}
	
	private void getGroups(UserGroupSink sink, UserInfo userInfo) throws Throwable {
		NamingEnumeration<SearchResult> groupSearchResultEnum = null;
		try {
			createLdapContext();
			int total;
			// Activate paged results
			if (pagedResultsEnabled)   {
				ldapContext.setRequestControls(new Control[]{
						new PagedResultsControl(pagedResultsSize, Control.NONCRITICAL) });
			}
			for (int ou=0; ou<groupSearchBase.length; ou++) {
				byte[] cookie = null;
				int counter = 0;
				try {
					do {
						if (!groupSearchFirstEnabled) {
							if (userInfo == null) {
								// Should never reach this.
								LOG.error("No user information provided for group search!");
								return;
							}
							groupSearchResultEnum = ldapContext
									.search(groupSearchBase[ou], extendedGroupSearchFilter,
											new Object[]{userInfo.getUserFullName(), userInfo.getUserName()},
											groupSearchControls);
						} else {
							// If group based search is enabled, then first retrieve all the groups based on the group configuration. 
							groupSearchResultEnum = ldapContext
									.search(groupSearchBase[ou], extendedAllGroupsSearchFilter,
											groupSearchControls);
						}
						while (groupSearchResultEnum.hasMore()) {
							final SearchResult groupEntry = groupSearchResultEnum.next();
							if (groupEntry != null) {
								counter++;
								Attribute groupNameAttr = groupEntry.getAttributes().get(groupNameAttribute);
								if (groupNameAttr == null) {
									if (LOG.isInfoEnabled())  {
										LOG.info(groupNameAttribute + " empty for entry " + groupEntry.getNameInNamespace() +
												", skipping sync");
									}
									continue;
								}
								String gName = (String) groupNameAttr.get();
								if (groupNameCaseConversionFlag) {
									if (groupNameLowerCaseFlag) {
										gName = gName.toLowerCase();
									} else {
										gName = gName.toUpperCase();
									}
								}
								if (groupNameRegExInst != null) {
									gName = groupNameRegExInst.transform(gName);
								}
								if (!groupSearchFirstEnabled) {
									//computedGroups.add(gName);
									if (LOG.isInfoEnabled())  {
										LOG.info("computed groups for user: " + userInfo.getUserName() +", groups: " + gName);
									}
									userInfo.addGroup(gName);
								} else {
									// If group based search is enabled, then
									// update the group name to ranger admin
									// check for group members and populate userInfo object with user's full name and group mapping
									Attribute groupMemberAttr = groupEntry.getAttributes().get(groupMemberAttributeName);
									LOG.debug("Update Ranger admin with " + gName);
									sink.addOrUpdateGroup(gName);
									int userCount = 0;
									if (groupMemberAttr == null || groupMemberAttr.size() <= 0) {
										LOG.info("No members available for " + gName);
										continue;
									}
									NamingEnumeration<?> userEnum = groupMemberAttr.getAll();
									while (userEnum.hasMore()) {
										String originalUserFullName = (String) userEnum.next();
										if (originalUserFullName == null || originalUserFullName.trim().isEmpty()) {
											continue;
										}
										String userFullName = originalUserFullName.toLowerCase();
										userCount++;
										if (!userGroupMap.containsKey(userFullName)) {
											userInfo = new UserInfo(userFullName, originalUserFullName); // Preserving the original full name for later
											userGroupMap.put(userFullName, userInfo);
										} else {
											userInfo = userGroupMap.get(userFullName);
										}
										LOG.info("Adding " + gName + " to user " + userInfo.getUserFullName());
										userInfo.addGroup(gName);
									}
									LOG.info("No. of members in the group " + gName + " = " + userCount);
								}
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
						if (pagedResultsEnabled)   {
							ldapContext.setRequestControls(new Control[]{
									new PagedResultsControl(PAGE_SIZE, cookie, Control.CRITICAL) });
						}
					} while (cookie != null);
					LOG.info("LDAPUserGroupBuilder.getGroups() completed with group count: "
							+ counter);
				} catch (Throwable t) {
					LOG.error("LDAPUserGroupBuilder.getGroups() failed with exception: " + t);
					LOG.info("LDAPUserGroupBuilder.getGroups() group count: "
							+ counter);
				}
			}

		} finally {
			if (groupSearchResultEnum != null) {
				groupSearchResultEnum.close();
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
	
	private static String getShortUserName(String longUserName) throws InvalidNameException {
		if (longUserName == null) {
			return null;
		}
		StringTokenizer stc = new StringTokenizer(longUserName, ",");
		String firstToken = stc.nextToken();
		StringTokenizer ste = new StringTokenizer(firstToken, "=");
		String userName =  ste.nextToken();
		if (ste.hasMoreTokens()) {
			userName = ste.nextToken();
		}
		userName = userName.trim();
		LOG.info("longUserName: " + longUserName + ", userName: " + userName);
		return userName;
	}
	
}

class UserInfo {
	private String userName;
	private String userFullName;
	private Set<String> groupList;
	
	public UserInfo(String userName, String userFullName) {
		this.userName = userName;
		this.userFullName = userFullName;
		this.groupList = new HashSet<String>();
	}
	
	public void updateUserName(String userName) {
		this.userName = userName;
	}
	
	public String getUserName() {
		return userName;
	}
	public String getUserFullName() {
		return userFullName;
	}
	public void addGroups(Set<String> groups) {
		groupList.addAll(groups);
	}
	public void addGroup(String group) {
		groupList.add(group);
	}
	public List<String> getGroups() {
		return (new ArrayList<String>(groupList));
	}
}
