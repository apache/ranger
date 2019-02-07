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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.SecurityZoneDBStore;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.model.validation.RangerSecurityZoneValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("zones")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class SecurityZoneREST {
    private static final Log LOG = LogFactory.getLog(SecurityZoneREST.class);

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    SecurityZoneDBStore securityZoneStore;

    @Autowired
    ServiceDBStore svcStore;

    @Autowired
    RangerValidatorFactory validatorFactory;
    
    @Autowired
    RangerBizUtil bizUtil;

    @POST
    @Path("/zones")
    public RangerSecurityZone createSecurityZone(RangerSecurityZone securityZone) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createSecurityZone("+ securityZone + ")");
        }
        RangerSecurityZone ret;
        try {
        	ensureAdminAccess();
            removeEmptyEntries(securityZone);
            RangerSecurityZoneValidator validator = validatorFactory.getSecurityZoneValidator(svcStore, securityZoneStore);
            validator.validate(securityZone, RangerValidator.Action.CREATE);
            ret = securityZoneStore.createSecurityZone(securityZone);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("createSecurityZone(" + securityZone + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createSecurityZone("+ securityZone + "):" +  ret);
        }
        return ret;
    }

    @PUT
    @Path("/zones/{id}")
    public RangerSecurityZone updateSecurityZone(@PathParam("id") Long zoneId,
                                                 RangerSecurityZone securityZone) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateSecurityZone(id=" + zoneId +", " + securityZone + ")");
        }

        ensureAdminAccess();
        removeEmptyEntries(securityZone);
        if (securityZone.getId() != null && !zoneId.equals(securityZone.getId())) {
            throw restErrorUtil.createRESTException("zoneId mismatch!!");
        } else {
            securityZone.setId(zoneId);
        }
        RangerSecurityZone ret;
        try {
            RangerSecurityZoneValidator validator = validatorFactory.getSecurityZoneValidator(svcStore, securityZoneStore);
            validator.validate(securityZone, RangerValidator.Action.UPDATE);
            ret = securityZoneStore.updateSecurityZoneById(securityZone);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("updateSecurityZone(" + securityZone + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== updateSecurityZone(id=" + zoneId +", " + securityZone + "):" + ret);
        }
        return ret;
    }

    @DELETE
    @Path("/zones/name/{name}")
    public void deleteSecurityZone(@PathParam("name") String zoneName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteSecurityZone(name=" + zoneName + ")");
        }
        try {
        	ensureAdminAccess();
            RangerSecurityZoneValidator validator = validatorFactory.getSecurityZoneValidator(svcStore, securityZoneStore);
            validator.validate(zoneName, RangerValidator.Action.DELETE);
            securityZoneStore.deleteSecurityZoneByName(zoneName);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteSecurityZone(" + zoneName + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteSecurityZone(name=" + zoneName + ")");
        }
    }

    @DELETE
    @Path("/zones/{id}")
    public void deleteSecurityZone(@PathParam("id") Long zoneId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteSecurityZone(id=" + zoneId + ")");
        }
        try {
        	ensureAdminAccess();
            RangerSecurityZoneValidator validator = validatorFactory.getSecurityZoneValidator(svcStore, securityZoneStore);
            validator.validate(zoneId, RangerValidator.Action.DELETE);
            securityZoneStore.deleteSecurityZoneById(zoneId);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("deleteSecurityZone(" + zoneId + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteSecurityZone(id=" + zoneId + ")");
        }
    }

    @GET
    @Path("/zones/name/{name}")
    public RangerSecurityZone getSecurityZone(@PathParam("name") String zoneName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getSecurityZone(name=" + zoneName + ")");
        }
        RangerSecurityZone ret;
        try {
            ret = securityZoneStore.getSecurityZoneByName(zoneName);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getSecurityZone(" + zoneName + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getSecurityZone(name=" + zoneName + "):" + ret);
        }
        return ret;
    }

    @GET
    @Path("/zones/{id}")
    public RangerSecurityZone getSecurityZone(@PathParam("id") Long id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getSecurityZone(id=" + id + ")");
        }
        RangerSecurityZone ret;
        try {
            ret = securityZoneStore.getSecurityZone(id);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getSecurityZone(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getSecurityZone(id=" + id + "):" + ret);
        }
        return ret;
    }

    @GET
    @Path("/zones")
    public List<RangerSecurityZone> getAllZones() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAllZones()");
        }
        List<RangerSecurityZone> ret;
        try {
            ret = securityZoneStore.getSecurityZones(null);
        } catch(WebApplicationException excp) {
            throw excp;
        } catch(Throwable excp) {
            LOG.error("getSecurityZones() failed", excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAllZones():" + ret);
        }
        return ret;
    }
    
	private void ensureAdminAccess(){
		if(!bizUtil.isAdmin()){
			String userName = bizUtil.getCurrentUserLoginId();
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "Ranger Securtiy Zone is not accessible for user '" + userName + "'.", true);
		}
	}

	private void removeEmptyEntries(RangerSecurityZone securityZone) {
		bizUtil.removeEmptyStrings(securityZone.getAdminUsers());
		bizUtil.removeEmptyStrings(securityZone.getAdminUserGroups());
		bizUtil.removeEmptyStrings(securityZone.getAuditUsers());
		bizUtil.removeEmptyStrings(securityZone.getAuditUserGroups());
		Map<String, RangerSecurityZoneService> serviceResouceMap=securityZone.getServices();
		if(serviceResouceMap!=null) {
			Set<Map.Entry<String, RangerSecurityZoneService>> serviceResouceMapEntries = serviceResouceMap.entrySet();
			Iterator<Map.Entry<String, RangerSecurityZoneService>> iterator=serviceResouceMapEntries.iterator();
			while (iterator.hasNext()){
				Map.Entry<String, RangerSecurityZoneService> serviceResouceMapEntry = iterator.next();
				RangerSecurityZoneService rangerSecurityZoneService=serviceResouceMapEntry.getValue();
				List<HashMap<String, List<String>>> resources=rangerSecurityZoneService.getResources();
				if(resources!=null) {
					for (Map<String, List<String>> resource : resources) {
						if (resource!=null) {
							for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
								List<String> resourceValues  = entry.getValue();
								bizUtil.removeEmptyStrings(resourceValues);
							}
						}
					}
				}
			}
		}
	}
}
