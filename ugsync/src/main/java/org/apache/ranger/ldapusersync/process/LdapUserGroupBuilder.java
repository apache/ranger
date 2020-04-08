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
import java.util.NoSuchElementException;

import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
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
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.LdapSyncSourceInfo;
import org.apache.ranger.unixusersync.model.UgsyncAuditInfo;
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
	private int userSearchScope;
	private String userObjectClass;
	private String userSearchFilter;
	private String extendedUserSearchFilter;
	private SearchControls userSearchControls;
	private Set<String> userGroupNameAttributeSet;

	private boolean pagedResultsEnabled = true;
	private int pagedResultsSize = PAGE_SIZE;

	private boolean groupSearchFirstEnabled;
	private boolean userSearchEnabled;
	private boolean groupSearchEnabled = true;
	private String[] groupSearchBase;
	private int groupSearchScope;
	private String groupObjectClass;
	private String groupSearchFilter;
	private String extendedGroupSearchFilter;
	private String extendedAllGroupsSearchFilter;
	private SearchControls groupSearchControls;
	private String groupMemberAttributeName;
	private String groupNameAttribute;
    private int groupHierarchyLevels;

	private LdapContext ldapContext;
	private StartTlsResponse tls;

	private boolean userNameCaseConversionFlag;
	private boolean groupNameCaseConversionFlag;
	private boolean userNameLowerCaseFlag;
	private boolean groupNameLowerCaseFlag;

	private Map<String, UserInfo> userGroupMap;
    //private Set<String> firstGroupDNs;
	private Set<String> allUsers;

	UgsyncAuditInfo ugsyncAuditInfo;
	LdapSyncSourceInfo ldapSyncSourceInfo;

	public static void main(String[] args) throws Throwable {
		LdapUserGroupBuilder  ugBuilder = new LdapUserGroupBuilder();
		ugBuilder.init();
	}

	public LdapUserGroupBuilder() {
		super();
		LOG.info("LdapUserGroupBuilder created");

		String userNameCaseConversion = config.getUserNameCaseConversion();

		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion)) {
			userNameCaseConversionFlag = false;
		}
		else {
			userNameCaseConversionFlag = true;
			userNameLowerCaseFlag = UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(userNameCaseConversion);
		}

		String groupNameCaseConversion = config.getGroupNameCaseConversion();

		if (UserGroupSyncConfig.UGSYNC_NONE_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion)) {
			groupNameCaseConversionFlag = false;
		}
		else {
			groupNameCaseConversionFlag = true;
			groupNameLowerCaseFlag = UserGroupSyncConfig.UGSYNC_LOWER_CASE_CONVERSION_VALUE.equalsIgnoreCase(groupNameCaseConversion);
		}
	}

	@Override
	public void init() throws Throwable{
		setConfig();
		ugsyncAuditInfo = new UgsyncAuditInfo();
		ldapSyncSourceInfo = new LdapSyncSourceInfo();
		ldapSyncSourceInfo.setLdapUrl(ldapUrl);
		ldapSyncSourceInfo.setIncrementalSycn("False");
		ldapSyncSourceInfo.setUserSearchEnabled(Boolean.toString(userSearchEnabled));
		ldapSyncSourceInfo.setGroupSearchEnabled(Boolean.toString(groupSearchEnabled));
		ldapSyncSourceInfo.setGroupSearchFirstEnabled(Boolean.toString(groupSearchFirstEnabled));
		ldapSyncSourceInfo.setGroupHierarchyLevel(Integer.toString(groupHierarchyLevels));
		ugsyncAuditInfo.setSyncSource("LDAP/AD");
		ugsyncAuditInfo.setLdapSyncSourceInfo(ldapSyncSourceInfo);
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
		ldapContext.addToEnvironment(Context.REFERRAL, ldapReferral);
	}

	private void setConfig() throws Throwable {
		LOG.info("LdapUserGroupBuilder initialization started");

		groupSearchFirstEnabled =   config.isGroupSearchFirstEnabled();
		userSearchEnabled =   config.isUserSearchEnabled();
		groupSearchEnabled =   config.isGroupSearchEnabled();
		ldapUrl = config.getLdapUrl();
		ldapBindDn = config.getLdapBindDn();
		ldapBindPassword = config.getLdapBindPassword();
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
        groupHierarchyLevels = config.getGroupHierarchyLevels();

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
		Set<String> allGroups = new HashSet<String>();
		allUsers = new HashSet<String>();

		if (!groupSearchFirstEnabled) {
			LOG.info("Performing user search first");
			getUsers(sink);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Total No. of users saved = " + userGroupMap.size());
			}
			if (!groupSearchEnabled && groupHierarchyLevels > 0) {
				getRootDN();
			}
            //Iterator<UserInfo> userInfoIterator = userGroupMap.
			for (UserInfo userInfo : userGroupMap.values()) {
				String userName = userInfo.getUserName();
				if (groupSearchEnabled) {
					// Perform group search
					LOG.info("groupSearch is enabled, would search for groups and compute memberships");
                    //firstGroupDNs = new HashSet<String>();
					getGroups(sink, userInfo);
				}
                if (groupHierarchyLevels > 0) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Going through group hierarchy for nested group evaluation");
					}
                    goUpGroupHierarchyLdap(userInfo.getGroupDNs(), groupHierarchyLevels - 1, userInfo);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Completed group hierarchy computation");
					}
                }
				List<String> groupList = userInfo.getGroups();
				allGroups.addAll(groupList);
				if (LOG.isDebugEnabled()) {
					LOG.debug("updateSink(): group list for " + userName + " = " + groupList);
				}
				if (userNameCaseConversionFlag) {
					if (userNameLowerCaseFlag) {
						userName = userName.toLowerCase();
					}
					else {
						userName = userName.toUpperCase();
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
			ldapSyncSourceInfo.setUserSearchFilter(extendedUserSearchFilter);
			ldapSyncSourceInfo.setGroupSearchFilter(extendedAllGroupsSearchFilter);
			ldapSyncSourceInfo.setTotalUsersSynced(allUsers.size());
			ldapSyncSourceInfo.setTotalGroupsSynced(allGroups.size());
			try {
				sink.postUserGroupAuditInfo(ugsyncAuditInfo);
			} catch (Throwable t) {
				LOG.error("sink.postUserGroupAuditInfo failed with exception: " + t.getMessage());
			}

		} else {
			LOG.info("Performing Group search first");
			getGroups(sink, null);
			 // Go through the userInfo map and update ranger admin.
            for (UserInfo userInfo : userGroupMap.values()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("userName from map = " + userInfo.getUserFullName());
				}
                String userName = getShortName(userInfo.getUserFullName());
                if (groupHierarchyLevels > 0) {
                    //System.out.println("Going through group hierarchy for nested group evaluation");
                    goUpGroupHierarchyLdap(userInfo.getGroupDNs(), groupHierarchyLevels - 1, userInfo);
                    //System.out.println("Completed group hierarchy computation");
                }
				List<String> groupList = userInfo.getGroups();
				allGroups.addAll(groupList);
                if (userSearchEnabled) {
                    LOG.info("User search is enabled and hence computing user membership.");
                    getUsers(sink);
                } else {
                    LOG.info("User search is disabled and hence using the group member attribute for username" + userName);
					allGroups.addAll(groupList);
					allUsers.add(userName); // Note:- in this case the usernames may contain groups as part of nested groups
                    if (userNameCaseConversionFlag) {
                        if (userNameLowerCaseFlag) {
                            userName = userName.toLowerCase();
                        } else {
                            userName = userName.toUpperCase();
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
			ldapSyncSourceInfo.setUserSearchFilter(extendedUserSearchFilter);
			ldapSyncSourceInfo.setGroupSearchFilter(extendedAllGroupsSearchFilter);
			ldapSyncSourceInfo.setTotalUsersSynced(allUsers.size());
			ldapSyncSourceInfo.setTotalGroupsSynced(allGroups.size());
			try {
				sink.postUserGroupAuditInfo(ugsyncAuditInfo);
			} catch (Throwable t) {
				LOG.error("sink.postUserGroupAuditInfo failed with exception: " + t.getMessage());
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
			for (String ou : userSearchBase) {
				byte[] cookie = null;
				int counter = 0;
				try {
					int paged = 0;
					do {
						userSearchResultEnum = ldapContext
								.search(ou, extendedUserSearchFilter, userSearchControls);

						while (userSearchResultEnum.hasMore()) {
							// searchResults contains all the user entries
							final SearchResult userEntry = userSearchResultEnum.next();

							if (userEntry == null)  {
								if (LOG.isInfoEnabled())  {
									LOG.info("userEntry null, skipping sync for the entry");
								}
								continue;
							}

							Attributes attributes = userEntry.getAttributes();
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
                                                String groupDN = (String) groupEnum.next();
												if (LOG.isDebugEnabled()) {
													LOG.debug("Adding " + groupDN + " to " + userName);
												}
                                                userInfo.addGroupDN(groupDN);
												String gName = getShortName(groupDN);
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
								allUsers.add(userName);

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
								if (LOG.isDebugEnabled()) {
									LOG.debug("Checking if the user " + userFullName + " is part of the retrieved groups");
								}

								userInfo = userGroupMap.get(userFullName);
								if (userInfo == null) {
									userInfo = userGroupMap.get(userName.toLowerCase());
								}
								if (userInfo != null) {
									counter++;
									LOG.info("Updating username for " + userFullName + " with " + userName);
									userInfo.updateUserName(userName);
									allUsers.add(userName);
                                    List<String> groupList = userInfo.getGroups();
                                    if (userNameCaseConversionFlag) {
                                        if (userNameLowerCaseFlag) {
                                            userName = userName.toLowerCase();
                                        }
                                        else {
                                            userName = userName.toUpperCase();
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
							for (Control control : controls) {
								if (control instanceof PagedResultsResponseControl) {
									PagedResultsResponseControl prrc =
											(PagedResultsResponseControl)control;
									total = prrc.getResultSize();
									if (total != 0) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : " + total);
										}
									} else {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : unknown");
										}
									}
									cookie = prrc.getCookie();
								}
							}
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("No controls were sent from the server");
							}
						}
						// Re-activate paged results
						if (pagedResultsEnabled)   {
							if (LOG.isDebugEnabled()) {
								LOG.debug(String.format("Fetched paged results round: %s", ++paged));
							}
							ldapContext.setRequestControls(new Control[]{
									new PagedResultsControl(pagedResultsSize, cookie, Control.CRITICAL) });
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
        //LOG.debug("getGroups(): for user " + userInfo.getUserName());
		NamingEnumeration<SearchResult> groupSearchResultEnum = null;
		try {
			createLdapContext();
			int total;
            // Activate paged results
			if (pagedResultsEnabled)   {
				ldapContext.setRequestControls(new Control[]{
						new PagedResultsControl(pagedResultsSize, Control.NONCRITICAL) });
			}
            for (String ou : groupSearchBase) {
				byte[] cookie = null;
				int counter = 0;
				try {
					int paged = 0;
					do {
						if (!groupSearchFirstEnabled) {
							if (userInfo == null) {
								// Should never reach this.
								LOG.error("No user information provided for group search!");
								return;
							}
							if (LOG.isDebugEnabled()) {
								LOG.debug("Searching for groups for user " + userInfo.getUserName() +
										" using filter " + String.format(extendedGroupSearchFilter, userInfo.getUserFullName(),
												userInfo.getUserName()));
							}
							groupSearchResultEnum = ldapContext
									.search(ou, extendedGroupSearchFilter,
											new Object[]{userInfo.getUserFullName(), userInfo.getUserName()},
											groupSearchControls);
						} else {
							// If group based search is enabled, then first retrieve all the groups based on the group configuration.
							groupSearchResultEnum = ldapContext
									.search(ou, extendedAllGroupsSearchFilter,
											groupSearchControls);
						}
						while (groupSearchResultEnum.hasMore()) {
							final SearchResult groupEntry = groupSearchResultEnum.next();
							if (groupEntry != null) {
								counter++;
								Attribute groupNameAttr = groupEntry.getAttributes().get(groupNameAttribute);
                                //System.out.println("getGroups(): Going through all groups");
								if (groupNameAttr == null) {
									if (LOG.isInfoEnabled())  {
										LOG.info(groupNameAttribute + " empty for entry " + groupEntry.getNameInNamespace() +
												", skipping sync");
									}
									continue;
								}
                                String groupDN = groupEntry.getNameInNamespace();
                                //System.out.println("getGroups(): groupDN = " + groupDN);
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
										LOG.info("computed groups for user: " + userInfo.getUserName() + ", groups: " + gName);
									}
                                    userInfo.addGroupDN(groupDN);
                                    userInfo.addGroup(gName);
								} else {
									// If group based search is enabled, then
									// update the group name to ranger admin
									// check for group members and populate userInfo object with user's full name and group mapping
									Attribute groupMemberAttr = groupEntry.getAttributes().get(groupMemberAttributeName);
									if (LOG.isDebugEnabled()) {
										LOG.debug("Update Ranger admin with " + gName);
									}
									int userCount = 0;
									if (groupMemberAttr == null || groupMemberAttr.size() <= 0) {
										LOG.info("No members available for " + gName);
										sink.addOrUpdateGroup(gName, new HashMap<String, String>(), null);
										continue;
									}
									sink.addOrUpdateGroup(gName, new HashMap<String, String>());
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
                                        userInfo.addGroupDN(groupDN);
									}
									LOG.info("No. of members in the group " + gName + " = " + userCount);
								}
							}
						}
						// Examine the paged results control response
						Control[] controls = ldapContext.getResponseControls();
						if (controls != null) {
							for (Control control : controls) {
								if (control instanceof PagedResultsResponseControl) {
									PagedResultsResponseControl prrc =
											(PagedResultsResponseControl)control;
									total = prrc.getResultSize();
									if (total != 0) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : " + total);
										}
									} else {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : unknown");
										}
									}
									cookie = prrc.getCookie();
								}
							}
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("No controls were sent from the server");
							}
						}
						// Re-activate paged results
						if (pagedResultsEnabled)   {
							if (LOG.isDebugEnabled()) {
								LOG.debug(String.format("Fetched paged results round: %s", ++paged));
							}
							ldapContext.setRequestControls(new Control[]{
									new PagedResultsControl(pagedResultsSize, cookie, Control.CRITICAL) });
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


	private static String getShortName(String longName) {
		if (StringUtils.isEmpty(longName)) {
			return null;
		}
		String shortName = "";
		try {
			LdapName subjectDN = new LdapName(longName);
			List<Rdn> rdns = subjectDN.getRdns();
			for (int i = rdns.size() - 1; i >= 0; i--) {
				if (StringUtils.isNotEmpty(shortName)) {
					break;
				}
				Rdn rdn = rdns.get(i);
				Attributes attributes = rdn.toAttributes();
				try {
					Attribute uid = attributes.get("uid");
					if (uid != null) {
						Object value = uid.get();
						if (value != null) {
							shortName = value.toString();
						}
					} else {
						Attribute cn = attributes.get("cn");
						if (cn != null) {
							Object value = cn.get();
							if (value != null) {
								shortName = value.toString();
							}
						}
					}
				} catch (NoSuchElementException ignore) {
					shortName = longName;
				} catch (NamingException ignore) {
					shortName = longName;
				}
			}
		} catch (InvalidNameException ex) {
			shortName = longName;
		}
		LOG.info("longName: " + longName + ", userName: " + shortName);
		return shortName;
	}

	private void goUpGroupHierarchyLdap(Set<String> groupDNs, int groupHierarchyLevels, UserInfo userInfo) throws Throwable {
		if (LOG.isDebugEnabled()) {
			LOG.debug("goUpGroupHierarchyLdap(): Incoming groups " + groupDNs);
		}
		if (groupHierarchyLevels <= 0 || groupDNs.isEmpty()) {
			return;
		}
		Set<String> nextLevelGroups = new HashSet<String>();

		NamingEnumeration<SearchResult> groupSearchResultEnum = null;
		try {
			createLdapContext();
			int total;
			// Activate paged results
			if (pagedResultsEnabled)   {
				ldapContext.setRequestControls(new Control[]{
						new PagedResultsControl(pagedResultsSize, Control.NONCRITICAL) });
			}
			String groupFilter = "(&(objectclass=" + groupObjectClass + ")";
            if (groupSearchFilter != null && !groupSearchFilter.trim().isEmpty()) {
                String customFilter = groupSearchFilter.trim();
                if (!customFilter.startsWith("(")) {
                    customFilter = "(" + customFilter + ")";
                }
                groupFilter += customFilter + "(|";
            }
			StringBuilder filter = new StringBuilder();

			for (String groupDN : groupDNs) {
				filter.append("(").append(groupMemberAttributeName).append("=")
						.append(groupDN).append(")");
			}
			filter.append("))");
            groupFilter += filter;

			if (LOG.isDebugEnabled()) {
				LOG.debug("extendedAllGroupsSearchFilter = " + groupFilter);
			}
			for (String ou : groupSearchBase) {
				byte[] cookie = null;
				int counter = 0;
				try {
					do {
						groupSearchResultEnum = ldapContext
									.search(ou, groupFilter,
											groupSearchControls);
                        //System.out.println("goUpGroupHierarchyLdap(): Going through the sub groups");
						while (groupSearchResultEnum.hasMore()) {
							final SearchResult groupEntry = groupSearchResultEnum.next();
							if (groupEntry == null) {
								if (LOG.isInfoEnabled())  {
									LOG.info("groupEntry null, skipping sync for the entry");
								}
								continue;
							}
							counter++;
							Attribute groupNameAttr = groupEntry.getAttributes().get(groupNameAttribute);
							if (groupNameAttr == null) {
								if (LOG.isInfoEnabled())  {
									LOG.info(groupNameAttribute + " empty for entry " + groupEntry.getNameInNamespace() +
											", skipping sync");
								}
								continue;
							}
                            String groupDN = groupEntry.getNameInNamespace();
                            //System.out.println("goUpGroupHierarchyLdap(): next Level Group DN = " + groupDN);
							nextLevelGroups.add(groupDN);
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
							userInfo.addGroup(gName);
						}
						// Examine the paged results control response
						Control[] controls = ldapContext.getResponseControls();
						if (controls != null) {
							for (Control control : controls) {
								if (control instanceof PagedResultsResponseControl) {
									PagedResultsResponseControl prrc =
											(PagedResultsResponseControl)control;
									total = prrc.getResultSize();
									if (total != 0) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : " + total);
										}
									} else {
										if (LOG.isDebugEnabled()) {
											LOG.debug("END-OF-PAGE total : unknown");
										}
									}
									cookie = prrc.getCookie();
								}
							}
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("No controls were sent from the server");
							}
						}
						// Re-activate paged results
						if (pagedResultsEnabled)   {
							ldapContext.setRequestControls(new Control[]{
									new PagedResultsControl(PAGE_SIZE, cookie, Control.CRITICAL) });
						}
					} while (cookie != null);
					LOG.info("LdapUserGroupBuilder.goUpGroupHierarchyLdap() completed with group count: "
							+ counter);
				} catch (RuntimeException re) {
					LOG.error("LdapUserGroupBuilder.goUpGroupHierarchyLdap() failed with runtime exception: ", re);
					throw re;
				} catch (Exception t) {
					LOG.error("LdapUserGroupBuilder.goUpGroupHierarchyLdap() failed with exception: ", t);
					LOG.info("LdapUserGroupBuilder.goUpGroupHierarchyLdap() group count: "
							+ counter);
				}
			}

		} catch (RuntimeException re) {
			LOG.error("LdapUserGroupBuilder.goUpGroupHierarchyLdap() failed with exception: ", re);
			throw re;
		} finally {
			if (groupSearchResultEnum != null) {
				groupSearchResultEnum.close();
			}
			closeLdapContext();
		}
		goUpGroupHierarchyLdap(nextLevelGroups, groupHierarchyLevels - 1, userInfo);
	}

	private void getRootDN() throws Throwable {
		NamingEnumeration groupSearchResultEnum = null;
		SearchControls sc1 = new SearchControls();
		sc1.setSearchScope(SearchControls.OBJECT_SCOPE);
		sc1.setReturningAttributes(new String[]{"namingContexts"});
		try {
			createLdapContext();
			groupSearchResultEnum = ldapContext
					.search("", "objectclass=*", sc1);
			//System.out.println("goUpGroupHierarchyLdap(): Going through the sub groups");
			while (groupSearchResultEnum.hasMore()) {
				SearchResult result1 = (SearchResult) groupSearchResultEnum.next();

				Attributes attrs = result1.getAttributes();
				Attribute attr = attrs.get("namingContexts");
				if (LOG.isDebugEnabled()) {
					LOG.debug("namingContexts = " + attr);
				}
				groupSearchBase = new String[] {attr.get(0).toString()};
				LOG.info("RootDN = " + Arrays.toString(groupSearchBase));
			}
		} catch (RuntimeException re) {
			throw re;
		} finally {
			if (groupSearchResultEnum != null) {
				groupSearchResultEnum.close();
			}
			closeLdapContext();
		}
	}
}

class UserInfo {
	private String userName;
	private String userFullName;
	private Set<String> groupList;
    private Set<String> groupDNList;

	public UserInfo(String userName, String userFullName) {
		this.userName = userName;
		this.userFullName = userFullName;
		this.groupList = new HashSet<String>();
        this.groupDNList = new HashSet<String>();
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

    public void addGroupDNs(Set<String> groupDNs) {
        groupDNList.addAll(groupDNs);
    }
    public void addGroupDN(String groupDN) {
        groupDNList.add(groupDN);
    }
    public Set<String> getGroupDNs() {
        return (groupDNList);
    }
}
