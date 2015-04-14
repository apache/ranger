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
import javax.naming.directory.Attributes;
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
	
	private static final int PAGE_SIZE = 500;
	
	private UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();

  private String ldapUrl;
  private String ldapBindDn;
  private String ldapBindPassword;
  private String ldapAuthenticationMechanism;

  private String searchBase;

  private String userSearchBase;
	private String userNameAttribute;
  private int    userSearchScope;
  private String userObjectClass;
  private String userSearchFilter;
  private String extendedUserSearchFilter;
  private SearchControls userSearchControls;
  private Set<String> userGroupNameAttributeSet;

  private boolean pagedResultsEnabled = true;
  private int pagedResultsSize = 500;

  private boolean groupSearchEnabled = true;
  private String groupSearchBase;
  private int    groupSearchScope;
  private String groupObjectClass;
  private String groupSearchFilter;
  private String extendedGroupSearchFilter;
  private String extendedAllGroupsSearchFilter;
  private SearchControls groupSearchControls;
  private String groupMemberAttributeName;
  private String groupNameAttribute;

	private LdapContext ldapContext;

	private boolean userNameCaseConversionFlag = false ;
	private boolean groupNameCaseConversionFlag = false ;
	private boolean userNameLowerCaseFlag = false ;
	private boolean groupNameLowerCaseFlag = false ;

  private boolean  groupUserMapSyncEnabled = false;

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

    ldapUrl = config.getLdapUrl();
    ldapBindDn = config.getLdapBindDn();
    ldapBindPassword = config.getLdapBindPassword();
    //ldapBindPassword = "admin-password";
    ldapAuthenticationMechanism = config.getLdapAuthenticationMechanism();

		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY, 
		    "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, ldapUrl);
		
		env.put(Context.SECURITY_PRINCIPAL, ldapBindDn);
		env.put(Context.SECURITY_CREDENTIALS, ldapBindPassword);
		env.put(Context.SECURITY_AUTHENTICATION, ldapAuthenticationMechanism);
		env.put(Context.REFERRAL, "follow") ;

		ldapContext = new InitialLdapContext(env, null);

    searchBase = config.getSearchBase();

		userSearchBase = config.getUserSearchBase();
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
		
		userGroupNameAttributeSet = config.getUserGroupNameAttributeSet();
		for (String useGroupNameAttribute : userGroupNameAttributeSet) {
			userSearchAttributes.add(useGroupNameAttribute);
		}
		
		userSearchControls = new SearchControls();
		userSearchControls.setSearchScope(userSearchScope);
		userSearchControls.setReturningAttributes(userSearchAttributes.toArray(
				new String[userSearchAttributes.size()]));
    userGroupNameAttributeSet = config.getUserGroupNameAttributeSet();

    pagedResultsEnabled =   config.isPagedResultsEnabled();
    pagedResultsSize =   config.getPagedResultsSize();

    groupSearchEnabled =   config.isGroupSearchEnabled();
    groupSearchBase = config.getGroupSearchBase();
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
    extendedGroupSearchFilter =  "(&"  + extendedGroupSearchFilter + "(" + groupMemberAttributeName + "={0})"  + ")";

    groupUserMapSyncEnabled = config.isGroupUserMapSyncEnabled();

    groupSearchControls = new SearchControls();
    groupSearchControls.setSearchScope(groupSearchScope);
    String[] groupSearchAttributes = new String[]{groupNameAttribute};
    groupSearchControls.setReturningAttributes(groupSearchAttributes);

		if (LOG.isInfoEnabled()) {
			LOG.info("LdapUserGroupBuilder initialization completed with --  "
					+ "ldapUrl: " + ldapUrl 
					+ ",  ldapBindDn: " + ldapBindDn
					+ ",  ldapBindPassword: ***** " 
					+ ",  ldapAuthenticationMechanism: " + ldapAuthenticationMechanism
          + ",  searchBase: " + searchBase
          + ",  userSearchBase: " + userSearchBase
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
          + ",  groupSearchBase: " + groupSearchBase
          + ",  groupSearchScope: " + groupSearchScope
          + ",  groupObjectClass: " + groupObjectClass
          + ",  groupSearchFilter: " + groupSearchFilter
          + ",  extendedGroupSearchFilter: " + extendedGroupSearchFilter
          + ",  extendedAllGroupsSearchFilter: " + extendedAllGroupsSearchFilter
          + ",  groupMemberAttributeName: " + groupMemberAttributeName
          + ",  groupNameAttribute: " + groupNameAttribute
          + ",  groupUserMapSyncEnabled: " + groupUserMapSyncEnabled
      );
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
    NamingEnumeration<SearchResult> userSearchResultEnum = null;
    NamingEnumeration<SearchResult> groupSearchResultEnum = null;
		try {
			createLdapContext();
		  int total;
      // Activate paged results
      byte[] cookie = null;
      if (pagedResultsEnabled)   {
        ldapContext.setRequestControls(new Control[]{
          new PagedResultsControl(pagedResultsSize, Control.NONCRITICAL) });
      }

			int counter = 0;
			do {
				userSearchResultEnum = ldapContext
					.search(userSearchBase, extendedUserSearchFilter,
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

					if (userNameCaseConversionFlag) {
						if (userNameLowerCaseFlag) {
							userName = userName.toLowerCase() ;
						}
						else {
							userName = userName.toUpperCase() ;
						}
					}

          Set<String> groups = new HashSet<String>();

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
                groups.add(gName);
              }
            }
          }

        if (groupSearchEnabled && groupUserMapSyncEnabled) {
            LOG.info("groupSearch and groupUserMapSync are enabled, would search for groups and compute memberships");
            groupSearchResultEnum = ldapContext
              .search(groupSearchBase, extendedGroupSearchFilter,
                new Object[]{userEntry.getNameInNamespace()},
                groupSearchControls);
            Set<String> computedGroups = new HashSet<String>();
            while (groupSearchResultEnum.hasMore()) {
              final SearchResult groupEntry = groupSearchResultEnum.next();
              if (groupEntry != null) {
                String gName = (String) groupEntry.getAttributes()
                  .get(groupNameAttribute).get();
                if (groupNameCaseConversionFlag) {
                  if (groupNameLowerCaseFlag) {
                    gName = gName.toLowerCase();
                  } else {
                    gName = gName.toUpperCase();
                  }
                }
                computedGroups.add(gName);
              }
            }
            if (LOG.isInfoEnabled())  {
                 LOG.info("computed groups for user: " + userName +", groups: " + computedGroups);
            }
            groups.addAll(computedGroups);
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
            if (pagedResultsEnabled)   {
		          ldapContext.setRequestControls(new Control[]{
		        		  new PagedResultsControl(PAGE_SIZE, cookie, Control.CRITICAL) });
            }
			} while (cookie != null);
			LOG.info("LDAPUserGroupBuilder.updateSink() completed with user count: "
					+ counter);

      if (groupSearchEnabled && !groupUserMapSyncEnabled) {
        if (LOG.isInfoEnabled())  {
          LOG.info("groupSearch enabled and groupUserMapSync not enabled, "
             + "would search for groups, would not compute memberships");
        }
        Set <String> groupNames = new HashSet<String>();
        groupSearchResultEnum = ldapContext
          .search(groupSearchBase, extendedAllGroupsSearchFilter,
            groupSearchControls);

        while (groupSearchResultEnum.hasMore()) {
          final SearchResult groupEntry = groupSearchResultEnum.next();
	        if (groupEntry.getAttributes().get(groupNameAttribute) == null) {
		        continue;
	        }
          String gName = (String) groupEntry.getAttributes()
            .get(groupNameAttribute).get();
          if (groupNameCaseConversionFlag) {
            if (groupNameLowerCaseFlag) {
              gName = gName.toLowerCase();
            } else {
              gName = gName.toUpperCase();
            }
          }
          groupNames.add(gName);
        }
        if (LOG.isInfoEnabled())  {
          LOG.info("found groups from ldap source: " + groupNames);
        }

        // TODO: push groupNames to ranger
        //  POST http://<IP>:6080/service/xusers/secure/groups     create group
        //  PUT http://<IP>:6080/service/xusers/secure/groups/{id}    update group
        //  sink.addOrUpdateUser(groupNames);

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
