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

package org.apache.ranger.ldapconfigcheck;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class UserSync {
    private static final String[] userNameAttrValues = {
            "sAMAccountName", "uid", "cn"
    };

    /* Not needed as this is read from the second occurrence of objectClass attribute from user entry */
    private static final String[] userObjClassValues = {
            "person", "posixAccount"
    };
    private static final String[] userGroupMemAttrValues = {
            "memberOf", "ismemberOf"
    };

    private static final String[] groupObjectClassValues = {
            "group", "groupOfNames", "posixGroup"
    };
    private static final String[] groupNameAttrValues    = {
            "distinguishedName", "cn"
    };
    private static final String[] groupMemAttrValues     = {
            "member", "memberUid"
    };

    private String userNameAttribute;
    private String userObjClassName;
    private String userGroupMemberName;
    private String groupMemberName;
    private String groupNameAttrName;
    private String groupObjClassName;
    private String groupSearchBase;
    private String groupSearchFilter;
    private String userSearchBase;
    private String userSearchFilter;
    private String searchBase;
    private String groupName;
    private final PrintStream logFile;
    private final PrintStream ambariProps;
    private final PrintStream installProps;
    private final LdapConfig config;

    public UserSync(LdapConfig config, PrintStream logFile, PrintStream ambariProps, PrintStream installProps) {
        this.config       = config;
        this.logFile      = logFile;
        this.ambariProps  = ambariProps;
        this.installProps = installProps;

        initUserSync();
    }

    public String getUserNameAttribute() {
        return userNameAttribute;
    }

    public String getUserObjClassName() {
        return userObjClassName;
    }

    public String getUserGroupMemberName() {
        return userGroupMemberName;
    }

    public String getGroupMemberName() {
        return groupMemberName;
    }

    public String getGroupNameAttrName() {
        return groupNameAttrName;
    }

    public String getGroupObjClassName() {
        return groupObjClassName;
    }

    public String getGroupSearchBase() {
        return groupSearchBase;
    }

    public String getUserSearchBase() {
        return userSearchBase;
    }

    public String getSearchBase() {
        return searchBase;
    }

    public void findUserProperties(LdapContext ldapContext) throws Throwable {
        // 1. find basic user properties
        // 2. find user search base and user search filter by passing basic attributes
        findBasicUserProperties(ldapContext, true);
        findAdvUserProperties(ldapContext, true);
    }

    public void getAllUsers(LdapContext ldapContext) throws Throwable {
        int                             noOfUsers            = 0;
        Attribute                       userNameAttr         = null;
        Attribute                       groupMemberAttr;
        NamingEnumeration<SearchResult> userSearchResultEnum = null;
        SearchControls                  userSearchControls   = new SearchControls();
        Set<String>                     userSearchAttributes = new HashSet<>();

        userSearchControls.setSearchScope(config.getUserSearchScope());

        if (userNameAttribute != null) {
            userSearchAttributes.add(userNameAttribute);
        }

        if (userGroupMemberName != null) {
            userSearchAttributes.add(userGroupMemberName);
        }

        if (!userSearchAttributes.isEmpty()) {
            userSearchControls.setReturningAttributes(userSearchAttributes.toArray(new String[userSearchAttributes.size()]));
        } else {
            userSearchControls.setReturningAttributes(new java.lang.String[] {"*", "+"});
        }

        String extendedUserSearchFilter = "(objectclass=" + userObjClassName + ")";

        if (userSearchFilter != null && !userSearchFilter.trim().isEmpty()) {
            String customFilter = userSearchFilter.trim();

            if (!customFilter.startsWith("(")) {
                customFilter = "(" + customFilter + ")";
            }

            extendedUserSearchFilter = "(&" + extendedUserSearchFilter + customFilter + ")";
        }

        byte[] cookie = null;

        logFile.println();
        logFile.println("INFO: First 20 Users and associated groups are:");

        try {
            do {
                userSearchResultEnum = ldapContext.search(userSearchBase, extendedUserSearchFilter, userSearchControls);

                while (userSearchResultEnum.hasMore()) {
                    final SearchResult userEntry = userSearchResultEnum.next();

                    if (userEntry == null) {
                        logFile.println("WARN: userEntry null");
                        continue;
                    }

                    Attributes attributes = userEntry.getAttributes();

                    if (attributes == null) {
                        logFile.println("WARN: Attributes missing for entry " + userEntry.getNameInNamespace());
                        continue;
                    }

                    if (userNameAttribute == null || userNameAttribute.isEmpty()) {
                        for (String userNameAttrValue : userNameAttrValues) {
                            userNameAttr = attributes.get(userNameAttrValue);
                            if (userNameAttr != null) {
                                userNameAttribute = userNameAttrValue;
                                break;
                            }
                        }

                        if (userNameAttr == null) {
                            logFile.print("WARN: Failed to find any of ( ");

                            for (String userNameAttrValue : userNameAttrValues) {
                                logFile.print(userNameAttrValue + " ");
                            }

                            logFile.println(") for entry " + userEntry.getNameInNamespace());
                            continue;
                        }
                    } else {
                        userNameAttr = attributes.get(userNameAttribute);

                        if (userNameAttr == null) {
                            logFile.println("WARN: Failed to find " + userNameAttribute + " for entry " + userEntry.getNameInNamespace());
                            continue;
                        }
                    }

                    String userName = userNameAttr.get().toString();

                    if (userName == null || userName.trim().isEmpty()) {
                        logFile.println("WARN: " + userNameAttribute + " empty for entry " + userEntry.getNameInNamespace());
                        continue;
                    }

                    userName = userName.toLowerCase();

                    Set<String> groups = new HashSet<>();

                    groupMemberAttr = attributes.get(userGroupMemberName);

                    if (groupMemberAttr != null) {
                        NamingEnumeration<?> groupEnum = groupMemberAttr.getAll();

                        while (groupEnum.hasMore()) {
                            String groupRes = groupEnum.next().toString();

                            groups.add(groupRes);

                            if (groupName == null || groupName.isEmpty()) {
                                groupName = groupRes;
                            }
                        }
                    }

                    if (noOfUsers < 20) {
                        logFile.println("Username: " + userName + ", Groups: " + groups);
                    }

                    noOfUsers++;
                }

                // Examine the paged results control response
                Control[] controls = ldapContext.getResponseControls();

                if (controls != null) {
                    for (Control control : controls) {
                        if (control instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;

                            cookie = prrc.getCookie();
                        }
                    }
                } else {
                    logFile.println("WARN: No controls were sent from the server");
                }

                // Re-activate paged results
                if (config.isPagedResultsEnabled()) {
                    ldapContext.setRequestControls(new Control[] {new PagedResultsControl(config.getPagedResultsSize(), cookie, Control.CRITICAL)});
                }
            }
            while (cookie != null);

            logFile.println("\nINFO: Total no. of users = " + noOfUsers);
        } catch (NamingException ne) {
            String msg = "Exception occured while retreiving users\n";

            if ((config.getUserNameAttribute() != null && !config.getUserNameAttribute().isEmpty()) ||
                    (config.getUserObjectClass() != null && !config.getUserObjectClass().isEmpty()) ||
                    (config.getGroupNameAttribute() != null && !config.getGroupNameAttribute().isEmpty()) ||
                    (config.getUserSearchBase() != null && !config.getUserSearchBase().isEmpty()) ||
                    (config.getUserSearchFilter() != null && !config.getUserSearchFilter().isEmpty())) {
                throw new Exception("Please verify values for:\n ranger.usersync.ldap.user.nameattribute\n " + "ranger.usersync.ldap.user.objectclass\n" + "ranger.usersync.ldap.user.groupnameattribute\n" + "ranger.usersync.ldap.user.searchbase\n" + "ranger.usersync.ldap.user.searchfilter\n");
            } else {
                throw new Exception(msg + ne);
            }
        } finally {
            if (userSearchResultEnum != null) {
                userSearchResultEnum.close();
            }
        }
    }

    public void findGroupProperties(LdapContext ldapContext) throws Throwable {
        // find basic group attributes/properties
        // find group search base and group search filter
        // Get all groups

        if (groupName == null || groupName.isEmpty()) {
            // Perform basic user search and get the group name from the user's group attribute name.
            findBasicUserProperties(ldapContext, false);
        }

        if (groupName == null || groupName.isEmpty()) {
            // Perform adv user search and get the group name from the user's group attribute name.
            findAdvUserProperties(ldapContext, false);
        }

        findBasicGroupProperties(ldapContext);
        findAdvGroupProperties(ldapContext);
    }

    public void getAllGroups(LdapContext ldapContext) throws Throwable {
        int                             noOfGroups            = 0;
        Attribute                       groupNameAttr;
        Attribute                       groupMemberAttr;
        NamingEnumeration<SearchResult> groupSearchResultEnum = null;
        SearchControls                  groupSearchControls   = new SearchControls();
        Set<String>                     groupSearchAttributes = new HashSet<>();

        groupSearchControls.setSearchScope(config.getGroupSearchScope());

        groupSearchAttributes.add(groupNameAttrName);
        groupSearchAttributes.add(groupMemberName);
        groupSearchAttributes.add("distinguishedName");

        groupSearchControls.setReturningAttributes(groupSearchAttributes.toArray(new String[groupSearchAttributes.size()]));

        String extendedGroupSearchFilter = "(objectclass=" + groupObjClassName + ")";

        if (groupSearchFilter != null && !groupSearchFilter.trim().isEmpty()) {
            String customFilter = groupSearchFilter.trim();

            if (!customFilter.startsWith("(")) {
                customFilter = "(" + customFilter + ")";
            }

            extendedGroupSearchFilter = "(&" + extendedGroupSearchFilter + customFilter + ")";
        }

        try {
            groupSearchResultEnum = ldapContext.search(groupSearchBase, extendedGroupSearchFilter, groupSearchControls);

            logFile.println("\nINFO: First 20 Groups and associated Users are:");

            while (groupSearchResultEnum.hasMore()) {
                final SearchResult groupEntry = groupSearchResultEnum.next();

                if (groupEntry == null) {
                    continue;
                }

                Attributes groupAttributes = groupEntry.getAttributes();

                if (groupAttributes == null) {
                    logFile.println("WARN: Attributes missing for entry " + groupEntry.getNameInNamespace());
                    continue;
                }

                groupMemberAttr = groupAttributes.get(groupMemberName);

                Set<String> users = new HashSet<>();

                if (groupMemberAttr != null) {
                    NamingEnumeration<?> userEnum = groupMemberAttr.getAll();

                    while (userEnum.hasMore()) {
                        String userRes = userEnum.next().toString();

                        users.add(userRes);
                    }
                }

                groupNameAttr = groupAttributes.get(groupNameAttrName);

                if (noOfGroups < 20) {
                    logFile.println("Group name: " + groupNameAttr.get().toString() + ", Users: " + users);
                }

                noOfGroups++;
            }

            logFile.println("\nINFO: Total no. of groups = " + noOfGroups);
        } catch (NamingException ne) {
            String msg = "Exception occured while retreiving groups\n";

            if ((config.getGroupNameAttribute() != null && !config.getGroupNameAttribute().isEmpty()) ||
                    (config.getGroupObjectClass() != null && !config.getGroupObjectClass().isEmpty()) ||
                    (config.getUserGroupMemberAttributeName() != null && !config.getUserGroupMemberAttributeName().isEmpty()) ||
                    (config.getGroupSearchBase() != null && !config.getGroupSearchBase().isEmpty()) ||
                    (config.getGroupSearchFilter() != null && !config.getGroupSearchFilter().isEmpty())) {
                throw new Exception("Please verify values for:\n ranger.usersync.group.memberattributename\n " + "ranger.usersync.group.nameattribute\n" + "ranger.usersync.group.objectclass\n" + "ranger.usersync.group.searchbase\n" + "ranger.usersync.group.searchfilter\n");
            } else {
                throw new Exception(msg + ne);
            }
        } finally {
            if (groupSearchResultEnum != null) {
                groupSearchResultEnum.close();
            }
        }
    }

    private void initUserSync() {
        try {
            String bindDn = config.getLdapBindDn();

            userObjClassName    = config.getUserObjectClass();
            userNameAttribute   = config.getUserNameAttribute();
            userGroupMemberName = config.getUserGroupNameAttribute();
            userSearchBase      = config.getUserSearchBase();
            userSearchFilter    = config.getUserSearchFilter();
            groupObjClassName   = config.getGroupObjectClass();
            groupNameAttrName   = config.getGroupNameAttribute();
            groupMemberName     = config.getUserGroupMemberAttributeName();
            groupSearchBase     = config.getGroupSearchBase();
            groupSearchFilter   = config.getGroupSearchFilter();

            //String userName = null;
            if (bindDn.contains("@")) {
                //userName = bindDn.substring(0, bindDn.indexOf("@"));
                searchBase = bindDn.substring(bindDn.indexOf("@") + 1);
                searchBase = "dc=".concat(searchBase);
                searchBase = searchBase.replaceAll("\\.", ",dc=");
            } else {
                int dcIndex = bindDn.toLowerCase().indexOf("dc=");

                //userName = bindDn.substring(bindDn.indexOf("=") + 1, dcIndex - 1);
                searchBase = bindDn.substring(dcIndex);
            }
        } catch (Throwable t) {
            logFile.println("ERROR: Failed to initialize the user sync properties " + t);
        }
    }

    // Use the provided bind dn or the user search base and user search filter for sample user and determine the basic user attribute.
    private void findBasicUserProperties(LdapContext ldapContext, boolean isOutputNeeded) throws Throwable {
        String         bindDn             = config.getLdapBindDn();
        String         userSFilter        = config.getUserSearchFilter();
        String         userSBase          = config.getUserSearchBase();
        Attribute      userNameAttr       = null;
        Attribute      groupMemberAttr;
        SearchControls userSearchControls = new SearchControls();

        userSearchControls.setSearchScope(config.getUserSearchScope());
        userSearchControls.setReturningAttributes(new java.lang.String[] {"*", "+"});

        int                             noOfUsers            = 0;
        NamingEnumeration<SearchResult> userSearchResultEnum = null;

        try {
            if (userSBase == null || userSBase.isEmpty()) {
                if (bindDn.contains("@")) {
                    userSBase = bindDn.substring(bindDn.indexOf("@") + 1);
                    userSBase = "dc=".concat(userSBase);
                    userSBase = userSBase.replaceAll("\\.", ",dc=");
                } else {
                    //int dcIndex = bindDn.toLowerCase().indexOf("dc=");
                    userSBase = bindDn.substring(bindDn.indexOf(",") + 1);
                }
            }

            if (userSFilter == null || userSFilter.isEmpty()) {
                if (bindDn.contains("@")) {
                    userSFilter = "userPrincipalName=" + bindDn;
                } else {
                    int cnEndIndex = bindDn.indexOf(",");

                    userSFilter = bindDn.substring(0, cnEndIndex);
                }
            }

            try {
                userSearchResultEnum = ldapContext.search(userSBase, userSFilter, userSearchControls);

                while (userSearchResultEnum.hasMore()) {
                    if (noOfUsers >= 5) {
                        break;
                    }

                    final SearchResult userEntry = userSearchResultEnum.next();

                    if (userEntry == null) {
                        logFile.println("WARN: userEntry null");
                        continue;
                    }

                    Attributes attributes = userEntry.getAttributes();

                    if (attributes == null) {
                        logFile.println("WARN: Attributes missing for entry " + userEntry.getNameInNamespace());
                        continue;
                    }

                    if (userNameAttribute == null || userNameAttribute.isEmpty()) {
                        for (String nameAttrValue : userNameAttrValues) {
                            userNameAttr = attributes.get(nameAttrValue);

                            if (userNameAttr != null) {
                                userNameAttribute = nameAttrValue;
                                break;
                            }
                        }

                        if (userNameAttr == null) {
                            logFile.print("WARN: Failed to find any of ( ");

                            for (String userNameAttrValue : userNameAttrValues) {
                                logFile.print(userNameAttrValue + " ");
                            }

                            logFile.println(") for entry " + userEntry.getNameInNamespace());
                            continue;
                        }
                    } else {
                        userNameAttr = attributes.get(userNameAttribute);

                        if (userNameAttr == null) {
                            logFile.println("WARN: Failed to find " + userNameAttribute + " for entry " + userEntry.getNameInNamespace());
                            continue;
                        }
                    }

                    String userName = (String) userNameAttr.get();

                    if (userName == null || userName.trim().isEmpty()) {
                        logFile.println("WARN: " + userNameAttribute + " empty for entry " + userEntry.getNameInNamespace());
                        continue;
                    }

                    userName = userName.toLowerCase();

                    Attribute            userObjClassAttr = attributes.get("objectClass");
                    NamingEnumeration<?> userObjClassEnum = userObjClassAttr.getAll();
                    String               userObjClass     = null;

                    while (userObjClassEnum.hasMore()) {
                        userObjClass = userObjClassEnum.next().toString();

                        if (userObjClassName == null || userObjClassName.isEmpty()) {
                            if (userObjClass != null) {
                                for (String userObjClassValue : userObjClassValues) {
                                    if (userObjClass.equalsIgnoreCase(userObjClassValue)) {
                                        userObjClassName = userObjClass;
                                        break;
                                    }
                                }
                            } else {
                                logFile.println("WARN: Failed to find objectClass attribute for " + userName);
                            }
                        }
                    }

                    if (userObjClassName == null || userObjClassName.isEmpty()) {
                        userObjClassName = userObjClass;
                    }

                    for (String userGroupMemAttrValue : userGroupMemAttrValues) {
                        groupMemberAttr = attributes.get(userGroupMemAttrValue);

                        if (groupMemberAttr != null) {
                            userGroupMemberName = userGroupMemAttrValue;
                            groupName           = groupMemberAttr.get(0).toString();
                            break;
                        }
                    }

                    noOfUsers++;
                }
            } catch (NamingException ne) {
                String msg = "Exception occurred while discovering basic user properties:\n" + "ranger.usersync.ldap.user.nameattribute\n" + "ranger.usersync.ldap.user.objectclass\n" + "ranger.usersync.ldap.user.groupnameattribute\n";

                if ((config.getUserSearchBase() != null && !config.getUserSearchBase().isEmpty()) || (config.getUserSearchFilter() != null && !config.getUserSearchFilter().isEmpty())) {
                    throw new Exception(msg + "Please verify values for ranger.usersync.ldap.user.searchbase and ranger.usersync.ldap.user.searchfilter");
                } else {
                    throw new Exception(msg + ne);
                }
            }

            if (isOutputNeeded) {
                installProps.println("# Possible values for user search related properties:");
                installProps.println("SYNC_LDAP_USER_NAME_ATTRIBUTE=" + userNameAttribute);
                installProps.println("SYNC_LDAP_USER_OBJECT_CLASS=" + userObjClassName);
                installProps.println("SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE=" + userGroupMemberName);

                ambariProps.println("# Possible values for user search related properties:");
                ambariProps.println("ranger.usersync.ldap.user.nameattribute=" + userNameAttribute);
                ambariProps.println("ranger.usersync.ldap.user.objectclass=" + userObjClassName);
                ambariProps.println("ranger.usersync.ldap.user.groupnameattribute=" + userGroupMemberName);
            }
        } finally {
            try {
                if (userSearchResultEnum != null) {
                    userSearchResultEnum.close();
                }
            } catch (NamingException ne) {
                logFile.println("ERROR: Exception occurred while closing user search result: " + ne.getMessage());
            }
        }
    }

    private void findAdvUserProperties(LdapContext ldapContext, boolean isOutputNeeded) throws Throwable {
        int                             noOfUsers;
        NamingEnumeration<SearchResult> userSearchResultEnum = null;
        SearchControls                  userSearchControls   = new SearchControls();

        userSearchControls.setSearchScope(config.getUserSearchScope());

        if (userNameAttribute != null && !userNameAttribute.isEmpty()) {
            Set<String> userSearchAttributes = new HashSet<>();

            userSearchAttributes.add(userNameAttribute);
            userSearchAttributes.add(userGroupMemberName);
            userSearchAttributes.add("distinguishedName");

            userSearchControls.setReturningAttributes(userSearchAttributes.toArray(new String[userSearchAttributes.size()]));
        } else {
            userSearchControls.setReturningAttributes(new java.lang.String[] {"*", "+"});
        }

        String extendedUserSearchFilter = "(objectclass=" + userObjClassName + ")";

        try {
            HashMap<String, Integer> ouOccurences = new HashMap<>();

            if (userSearchBase == null || userSearchBase.isEmpty()) {
                userSearchResultEnum = ldapContext.search(searchBase, extendedUserSearchFilter, userSearchControls);
            } else {
                userSearchResultEnum = ldapContext.search(userSearchBase, extendedUserSearchFilter, userSearchControls);
            }

            noOfUsers = 0;

            while (userSearchResultEnum.hasMore()) {
                if (noOfUsers >= 20) {
                    break;
                }

                final SearchResult userEntry = userSearchResultEnum.next();

                if (userEntry == null) {
                    logFile.println("WARN: userEntry null");
                    continue;
                }

                Attributes attributes = userEntry.getAttributes();

                if (attributes == null) {
                    logFile.println("WARN: Attributes missing for entry " + userEntry.getNameInNamespace());
                    continue;
                }

                String    dnValue;
                Attribute dnAttr = attributes.get("distinguishedName");

                if (dnAttr != null) {
                    dnValue = dnAttr.get().toString();

                    String ouStr     = "OU=";
                    int    indexOfOU = dnValue.indexOf(ouStr);

                    if (indexOfOU > 0) {
                        dnValue = dnValue.substring(indexOfOU);
                    } else {
                        dnValue = dnValue.substring(dnValue.indexOf(",") + 1);
                    }
                } else {
                    // If distinguishedName is not found, strip off the userName from the long name for OU or subdomain
                    dnValue = userEntry.getNameInNamespace();
                    dnValue = dnValue.substring(dnValue.indexOf(",") + 1);
                }

                Integer ouOccrs = ouOccurences.get(dnValue);

                if (ouOccrs == null) {
                    ouOccrs = 0;
                }

                int val = ouOccrs;

                ouOccrs = ++val;

                ouOccurences.put(dnValue, ouOccrs);

                noOfUsers++;
            }

            if (!ouOccurences.isEmpty()) {
                Set<String> keys      = ouOccurences.keySet();
                int         maxOUOccr = 0;

                for (String key : keys) {
                    int ouOccurVal = ouOccurences.get(key);

                    logFile.println("INFO: No. of users from " + key + " = " + ouOccurVal);

                    if (ouOccurVal > maxOUOccr) {
                        maxOUOccr      = ouOccurVal;
                        userSearchBase = key;
                    }
                }
            }

            if (userSearchFilter == null || userSearchFilter.isEmpty()) {
                userSearchFilter = userNameAttribute + "=*";
            }

            if (isOutputNeeded) {
                installProps.println("SYNC_LDAP_USER_SEARCH_BASE=" + userSearchBase);
                installProps.println("SYNC_LDAP_USER_SEARCH_FILTER=" + userSearchFilter);

                ambariProps.println("ranger.usersync.ldap.user.searchbase=" + userSearchBase);
                ambariProps.println("ranger.usersync.ldap.user.searchfilter=" + userSearchFilter);
            }
        } catch (NamingException ne) {
            String msg = "Exception occured while discovering user properties:\n" + "ranger.usersync.ldap.user.searchbase\n" + "ranger.usersync.ldap.user.searchfilter\n";
            if ((config.getUserNameAttribute() != null && !config.getUserNameAttribute().isEmpty()) ||
                    (config.getUserObjectClass() != null && !config.getUserObjectClass().isEmpty()) ||
                    (config.getGroupNameAttribute() != null && !config.getGroupNameAttribute().isEmpty())) {
                throw new Exception("Please verify values for ranger.usersync.ldap.user.nameattribute, ranger.usersync.ldap.user.objectclass, and ranger.usersync.ldap.user.groupnameattribute");
            } else {
                throw new Exception(msg + ne);
            }
        } finally {
            if (userSearchResultEnum != null) {
                userSearchResultEnum.close();
            }
        }
    }

    private void findBasicGroupProperties(LdapContext ldapContext) throws Throwable {
        int                             noOfGroups;
        Attribute                       groupNameAttr;
        String                          groupBase;
        String                          groupFilter;
        Attribute                       groupMemberAttr;
        NamingEnumeration<SearchResult> groupSearchResultEnum = null;
        SearchControls                  groupSearchControls   = new SearchControls();

        groupSearchControls.setSearchScope(config.getGroupSearchScope());

        try {
            if (groupName == null || groupName.isEmpty()) {
                groupSearchResultEnum = ldapContext.search(searchBase, null);
            } else {
                int baseIndex = groupName.indexOf(",");

                groupBase             = groupName.substring(baseIndex + 1);
                groupFilter           = groupName.substring(0, baseIndex);
                groupSearchResultEnum = ldapContext.search(groupBase, groupFilter, groupSearchControls);
            }

            noOfGroups = 0;

            while (groupSearchResultEnum.hasMore()) {
                if (noOfGroups >= 1) {
                    break;
                }

                final SearchResult groupEntry = groupSearchResultEnum.next();

                if (groupEntry == null) {
                    continue;
                }

                Attributes groupAttributes = groupEntry.getAttributes();

                if (groupAttributes == null) {
                    logFile.println("WARN: Attributes missing for entry " + groupEntry.getNameInNamespace());
                    continue;
                }

                Attribute groupObjClassAttr = groupAttributes.get("objectClass");

                if (groupObjClassAttr != null) {
                    NamingEnumeration<?> groupObjClassEnum = groupObjClassAttr.getAll();

                    while (groupObjClassEnum.hasMore()) {
                        String groupObjClassStr = groupObjClassEnum.next().toString();

                        for (String groupObjectClassValue : groupObjectClassValues) {
                            if (groupObjClassStr.equalsIgnoreCase(groupObjectClassValue)) {
                                groupObjClassName = groupObjClassStr;
                                break;
                            }
                        }
                    }
                } else {
                    logFile.println("WARN: Failed to find group objectClass attribute for " + groupEntry.getNameInNamespace());
                    continue;
                }

                if (groupNameAttrName == null || groupNameAttrName.isEmpty()) {
                    for (String groupNameAttrValue : groupNameAttrValues) {
                        groupNameAttr = groupAttributes.get(groupNameAttrValue);

                        if (groupNameAttr != null) {
                            groupNameAttrName = groupNameAttrValue;
                            break;
                        }
                    }
                }

                for (String groupMemAttrValue : groupMemAttrValues) {
                    groupMemberAttr = groupAttributes.get(groupMemAttrValue);

                    if (groupMemberAttr != null) {
                        groupMemberName = groupMemAttrValue;
                        break;
                    }
                }

                noOfGroups++;
            }

            installProps.println("\n# Possible values for group search related properties:");
            installProps.println("SYNC_GROUP_MEMBER_ATTRIBUTE_NAME=" + groupMemberName);
            installProps.println("SYNC_GROUP_NAME_ATTRIBUTE=" + groupNameAttrName);
            installProps.println("SYNC_GROUP_OBJECT_CLASS=" + groupObjClassName);

            ambariProps.println("\n# Possible values for group search related properties:");
            ambariProps.println("ranger.usersync.group.memberattributename=" + groupMemberName);
            ambariProps.println("ranger.usersync.group.nameattribute=" + groupNameAttrName);
            ambariProps.println("ranger.usersync.group.objectclass=" + groupObjClassName);
        } finally {
            if (groupSearchResultEnum != null) {
                groupSearchResultEnum.close();
            }
        }
    }

    private void findAdvGroupProperties(LdapContext ldapContext) throws Throwable {
        int                             noOfGroups            = 0;
        NamingEnumeration<SearchResult> groupSearchResultEnum = null;
        SearchControls                  groupSearchControls   = new SearchControls();
        Set<String>                     groupSearchAttributes = new HashSet<>();

        groupSearchControls.setSearchScope(config.getGroupSearchScope());

        groupSearchAttributes.add(groupNameAttrName);
        groupSearchAttributes.add(groupMemberName);
        groupSearchAttributes.add("distinguishedName");

        groupSearchControls.setReturningAttributes(groupSearchAttributes.toArray(new String[groupSearchAttributes.size()]));

        String extendedGroupSearchFilter = "(objectclass=" + groupObjClassName + ")";

        try {
            HashMap<String, Integer> ouOccurences = new HashMap<>();

            if (groupSearchBase == null || groupSearchBase.isEmpty()) {
                groupSearchResultEnum = ldapContext.search(searchBase, extendedGroupSearchFilter, groupSearchControls);
            } else {
                groupSearchResultEnum = ldapContext.search(groupSearchBase, extendedGroupSearchFilter, groupSearchControls);
            }

            while (groupSearchResultEnum.hasMore()) {
                if (noOfGroups >= 20) {
                    break;
                }

                final SearchResult groupEntry = groupSearchResultEnum.next();

                if (groupEntry == null) {
                    continue;
                }

                Attributes groupAttributes = groupEntry.getAttributes();

                if (groupAttributes == null) {
                    logFile.println("WARN: Attributes missing for entry " + groupEntry.getNameInNamespace());
                    continue;
                }

                String    dnValue;
                Attribute dnAttr = groupAttributes.get("distinguishedName");

                if (dnAttr != null) {
                    dnValue = dnAttr.get().toString();

                    String ouStr     = "OU=";
                    int    indexOfOU = dnValue.indexOf(ouStr);

                    if (indexOfOU > 0) {
                        dnValue = dnValue.substring(indexOfOU);
                    } else {
                        dnValue = dnValue.substring(dnValue.indexOf(",") + 1);
                    }
                } else {
                    // If distinguishedName is not found, strip off the userName from the long name for OU or subdomain
                    dnValue = groupEntry.getNameInNamespace();
                    dnValue = dnValue.substring(dnValue.indexOf(",") + 1);
                }

                Integer ouOccrs = ouOccurences.get(dnValue);

                if (ouOccrs == null) {
                    ouOccrs = 0;
                }

                int val = ouOccrs;

                ouOccrs = ++val;

                ouOccurences.put(dnValue, ouOccrs);

                noOfGroups++;
            }

            if (!ouOccurences.isEmpty()) {
                Set<String> keys      = ouOccurences.keySet();
                int         maxOUOccr = 0;

                for (String key : keys) {
                    int ouOccurVal = ouOccurences.get(key);

                    logFile.println("INFO: No. of groups from " + key + " = " + ouOccurVal);

                    if (ouOccurVal > maxOUOccr) {
                        maxOUOccr       = ouOccurVal;
                        groupSearchBase = key;
                    }
                }
            }

            if (groupSearchFilter == null || groupSearchFilter.isEmpty()) {
                groupSearchFilter = groupNameAttrName + "=*";
            }

            installProps.println("SYNC_GROUP_SEARCH_BASE=" + groupSearchBase);
            installProps.println("SYNC_LDAP_GROUP_SEARCH_FILTER=" + groupSearchFilter);

            ambariProps.println("ranger.usersync.group.searchbase=" + groupSearchBase);
            ambariProps.println("ranger.usersync.group.searchfilter=" + groupSearchFilter);
        } finally {
            if (groupSearchResultEnum != null) {
                groupSearchResultEnum.close();
            }
        }
    }
}
