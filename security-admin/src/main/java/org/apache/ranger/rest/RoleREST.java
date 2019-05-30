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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.RoleDBStore;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerRoleService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerRoleList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Path("roles")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class RoleREST {
    private static final Log LOG = LogFactory.getLog(RoleREST.class);

    private static List<String> INVALID_USERS = new ArrayList<>();

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RoleDBStore roleStore;

    @Autowired
    RangerRoleService roleService;

    @Autowired
    XUserService xUserService;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    RangerValidatorFactory validatorFactory;

    @Autowired
    RangerBizUtil bizUtil;

    static {
        INVALID_USERS.add(RangerPolicyEngine.USER_CURRENT);
        INVALID_USERS.add(RangerPolicyEngine.RESOURCE_OWNER);
    }

    @POST
    @Path("/roles")
    public RangerRole createRole(RangerRole role) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createRole("+ role + ")");
        }

        RangerRole ret;
        try {
            ensureAdminAccess();
            if (containsInvalidMember(role.getUsers())) {
                throw new Exception("Invalid role user(s)");
            }
            sanitizeRole(role);
            ret = roleStore.createRole(role);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("createRole(" + role + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createRole("+ role + "):" +  ret);
        }
        return ret;
    }

    @PUT
    @Path("/roles/{id}")
    public RangerRole updateRole(@PathParam("id") Long roleId,
                                                 RangerRole role) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateRole(id=" + roleId +", " + role + ")");
        }

        if (role.getId() != null && !roleId.equals(role.getId())) {
            throw restErrorUtil.createRESTException("roleId mismatch!!");
        } else {
            role.setId(roleId);
        }
        RangerRole ret;
        try {
            ensureAdminAccess();
            sanitizeRole(role);
            if (containsInvalidMember(role.getUsers())) {
                throw new Exception("Invalid role user(s)");
            }
            ret = roleStore.updateRole(role);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("updateRole(" + role + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== updateRole(id=" + roleId +", " + role + "):" + ret);
        }
        return ret;
    }

    @DELETE
    @Path("/roles/name/{name}")
    public void deleteRole(@PathParam("name") String roleName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteRole(name=" + roleName + ")");
        }
        try {
            ensureAdminAccess();
            roleStore.deleteRole(roleName);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteRole(" + roleName + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteRole(name=" + roleName + ")");
        }
    }

    @DELETE
    @Path("/roles/{id}")
    public void deleteRole(@PathParam("id") Long roleId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteRole(id=" + roleId + ")");
        }
        try {
            ensureAdminAccess();
            roleStore.deleteRole(roleId);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteRole(" + roleId + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteRole(id=" + roleId + ")");
        }
    }

    @GET
    @Path("/roles/name/{name}")
    public RangerRole getRole(@PathParam("name") String roleName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getRole(name=" + roleName + ")");
        }
        RangerRole ret;
        try {
            ret = roleStore.getRole(roleName);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getRole(" + roleName + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getRole(name=" + roleName + "):" + ret);
        }
        return ret;
    }

    @GET
    @Path("/roles/{id}")
    public RangerRole getRole(@PathParam("id") Long id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getRole(id=" + id + ")");
        }
        RangerRole ret;
        try {
            ret = roleStore.getRole(id);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getRole(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getRole(id=" + id + "):" + ret);
        }
        return ret;
    }

    @GET
    @Path("/roles")
    public RangerRoleList getAllRoles(@Context HttpServletRequest request) {
        RangerRoleList ret = new RangerRoleList();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAllRoles()");
        }
        SearchFilter filter = searchUtil.getSearchFilter(request, roleService.sortFields);
        List<RangerRole> roles;
        try {
            roles = roleStore.getRoles(filter);
            ret.setRoleList(roles);
            if (roles != null) {
                ret.setTotalCount(roles.size());
                ret.setSortBy(filter.getSortBy());
                ret.setSortType(filter.getSortType());
                ret.setResultSize(roles.size());
            }
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getRoles() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAllRoles():" + ret);
        }
        return ret;
    }

    /*
        This API is used to add users and groups with/without GRANT privileges to this Role. It follows add-or-update semantics
     */
    @PUT
    @Path("/roles/{id}/addUsersAndGroups")
    public RangerRole addUsersAndGroups(Long roleId, List<String> users, List<String> groups, Boolean isAdmin) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ", isAdmin=" + isAdmin + ")");
        }

        RangerRole role;

        try {
            // Real processing
            ensureAdminAccess();
            if (containsInvalidUser(users)) {
                throw new Exception("Invalid role user(s)");
            }

            role = getRole(roleId);

            Set<RangerRole.RoleMember> roleUsers = new HashSet<>();
            Set<RangerRole.RoleMember> roleGroups = new HashSet<>();

            for (RangerRole.RoleMember user : role.getUsers()) {
                if (user.getIsAdmin() == isAdmin) {
                    roleUsers.add(user);
                }
            }
            for (String user : users) {
                roleUsers.add(new RangerRole.RoleMember(user, isAdmin));
            }

            for (RangerRole.RoleMember group : role.getGroups()) {
                if (group.getIsAdmin() == isAdmin) {
                    roleGroups.add(group);
                }
            }
            for (String group : groups) {
                roleGroups.add(new RangerRole.RoleMember(group, isAdmin));
            }
            role.setUsers(new ArrayList<>(roleUsers));
            role.setGroups(new ArrayList<>(roleGroups));

            role = roleStore.updateRole(role);

        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("addUsersAndGroups() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ", isAdmin=" + isAdmin + ")");
        }

        return role;
    }

    /*
        This API is used to remove users and groups, without regard to their GRANT privilege, from this Role.
     */
    @PUT
    @Path("/roles/{id}/removeUsersAndGroups")
    public RangerRole removeUsersAndGroups(Long roleId, List<String> users, List<String> groups) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ")");
        }
        RangerRole role;

        try {
            // Real processing
            ensureAdminAccess();
            role = getRole(roleId);

            for (String user : users) {
                Iterator<RangerRole.RoleMember> iter = role.getUsers().iterator();
                while (iter.hasNext()) {
                    RangerRole.RoleMember member = iter.next();
                    if (StringUtils.equals(member.getName(), user)) {
                        iter.remove();
                        break;
                    }
                }
            }
            for (String group : groups) {
                Iterator<RangerRole.RoleMember> iter = role.getGroups().iterator();
                while (iter.hasNext()) {
                    RangerRole.RoleMember member = iter.next();
                    if (StringUtils.equals(member.getName(), group)) {
                        iter.remove();
                        break;
                    }
                }
            }

            role = roleStore.updateRole(role);

        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("removeUsersAndGroups() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ")");
        }

        return role;
    }

    /*
        This API is used to remove GRANT privilege from listed users and groups.
     */
    @PUT
    @Path("/roles/{id}/removeAdminFromUsersAndGroups")
    public RangerRole removeAdminFromUsersAndGroups(Long roleId, List<String> users, List<String> groups) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeAdminFromUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ")");
        }
        RangerRole role;
        try {
            // Real processing
            ensureAdminAccess();
            role = getRole(roleId);

            for (String user : users) {
                for (RangerRole.RoleMember member : role.getUsers()) {
                    if (StringUtils.equals(member.getName(), user) && member.getIsAdmin()) {
                        member.setIsAdmin(false);
                    }
                }
            }
            for (String group : groups) {
                for (RangerRole.RoleMember member : role.getGroups()) {
                    if (StringUtils.equals(member.getName(), group) && member.getIsAdmin()) {
                        member.setIsAdmin(false);
                    }
                }
            }

            role = roleStore.updateRole(role);

        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("removeAdminFromUsersAndGroups() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeAdminFromUsersAndGroups(id=" + roleId + ", users=" + Arrays.toString(users.toArray()) + ", groups=" + Arrays.toString(groups.toArray()) + ")");
        }

        return role;
    }

    private void sanitizeRole(RangerRole role) {
        if (role != null) {
            role.setRoles(null);
            role.setRoles(null);
        }
    }

    private void ensureAdminAccess() throws Exception {
        UserSessionBase usb = ContextUtil.getCurrentUserSession();

        if (usb == null || !usb.isUserAdmin()) {
            throw new Exception("User does not have permission for this operation");
        }
    }

    private boolean containsInvalidMember(List<RangerRole.RoleMember> users) {
        boolean ret = false;
        for (RangerRole.RoleMember user : users) {
            for (String invalidUser : INVALID_USERS) {
                if (StringUtils.equals(user.getName(), invalidUser)) {
                    ret = true;
                    break;
                }
            }
            if (ret) {
                break;
            }
        }
        return ret;
    }

    private boolean containsInvalidUser(List<String> users) {
        return CollectionUtils.isNotEmpty(users) && CollectionUtils.containsAny(users, INVALID_USERS);
    }

}

