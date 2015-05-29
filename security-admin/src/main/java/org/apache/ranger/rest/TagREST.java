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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import java.util.ArrayList;
import java.util.List;

@Path(TagRESTConstants.TAGDEF_NAME_AND_VERSION)

@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class TagREST {

    private static final Log LOG = LogFactory.getLog(TagREST.class);

    @Autowired
    RESTErrorUtil restErrorUtil;

    /*
    @Autowired
    TagFileStore tagStore;
    */

    private TagFileStore tagStore;
    public TagREST() {
        tagStore = TagFileStore.getInstance();
    }

    @POST
    @Path(TagRESTConstants.TAGS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerTagDef createTagDef(RangerTagDef tagDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createTagDef(" + tagDef + ")");
        }

        RangerTagDef ret;

        try {
            //RangerTagDefValidator validator = validatorFactory.getTagDefValidator(tagStore);
            //validator.validate(tagDef, Action.CREATE);
            ret = tagStore.createTagDef(tagDef);
        } catch(Exception excp) {
            LOG.error("createTagDef(" + tagDef + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.createTagDef(" + tagDef + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.TAG_RESOURCE + "/{name}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerTagDef updateTagDef(@PathParam("name") String name, RangerTagDef tagDef) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateTagDef(" + name + ")");
        }
        if (tagDef.getName() == null) {
            tagDef.setName(name);
        } else if (!tagDef.getName().equals(name)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "tag name mismatch", true);
        }

        RangerTagDef ret;

        try {
            ret = tagStore.updateTagDef(tagDef);
        } catch (Exception excp) {
            LOG.error("updateTagDef(" + name + ") failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateTagDef(" + name + ")");
        }

        return ret;
    }

    @DELETE
    @Path(TagRESTConstants.TAG_RESOURCE + "/{name}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteTagDef(@PathParam("name") String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagDef(" + name + ")");
        }

        try {
            //RangerTagDefValidator validator = validatorFactory.getTagDefValidator(tagStore);
            //validator.validate(guid, Action.DELETE);
            tagStore.deleteTagDef(name);
        } catch(Exception excp) {
            LOG.error("deleteTagDef(" + name + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteTagDef(" + name + ")");
        }
    }

    @GET
    @Path(TagRESTConstants.TAG_RESOURCE+"/{name}")
    @Produces({ "application/json", "application/xml" })
    public RangerTagDef getTagDefByName(@PathParam("name") String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagDefByName(" + name + ")");
        }

        RangerTagDef ret;

        try {
            ret = tagStore.getTagDef(name);
        } catch(Exception excp) {
            LOG.error("getTagDefByName(" + name + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagDefByName(" + name + "): " + ret);
        }

        return ret;
    }

    @GET
    @Path(TagRESTConstants.TAGS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<RangerTagDef> getTagDefs() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagDefs()");
        }

        List<RangerTagDef> ret;

        try {
            ret = tagStore.getTagDefs(new SearchFilter());
        } catch(Exception excp) {
            LOG.error("getTagDefByName() failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagDefs()");
        }

        return ret;
    }

    @POST
    @Path(TagRESTConstants.RESOURCES_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerResource createResource(RangerResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createResource(" + resource + ")");
        }

        RangerResource ret;

        try {
            //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
            //validator.validate(resource, Action.CREATE);
            ret = tagStore.createResource(resource);
        } catch(Exception excp) {
            LOG.error("createResource(" + resource + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.createResource(" + resource + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "/{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerResource updateResource(@PathParam("id") Long id, RangerResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateResource(" + id + ")");
        }

        if (resource.getId() == null) {
            resource.setId(id);
        } else if (!resource.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "resource id mismatch", true);
        }

        RangerResource ret;

        try {
            //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
            //validator.validate(resource, Action.UPDATE);
            ret = tagStore.updateResource(resource);
        } catch(Exception excp) {
            LOG.error("updateResource(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateResource(" + resource + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "/{id}/" +TagRESTConstants.ACTION_SUB_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerResource updateResource(@PathParam("id") final Long id, @DefaultValue(TagRESTConstants.ACTION_ADD) @QueryParam(TagRESTConstants.ACTION_OP) String op, List<RangerResource.RangerResourceTag> resourceTagList) {

        RangerResource ret;

        if (op.equals(TagRESTConstants.ACTION_ADD) ||
                op.equals(TagRESTConstants.ACTION_REPLACE) ||
                op.equals(TagRESTConstants.ACTION_DELETE)) {
            RangerResource oldResource;
            try {
                oldResource = tagStore.getResource(id);
            } catch (Exception excp) {
                LOG.error("getResource(" + id + ") failed", excp);

                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
            }
            List<RangerResource.RangerResourceTag> oldTagsAndValues = oldResource.getTags();

            switch (op) {
                case TagRESTConstants.ACTION_ADD:
                    oldTagsAndValues.addAll(resourceTagList);
                    break;
                case TagRESTConstants.ACTION_REPLACE:
                    oldResource.setTags(resourceTagList);
                    break;
                case TagRESTConstants.ACTION_DELETE:
                    oldTagsAndValues.removeAll(resourceTagList);
                    break;
                default:
                    break;
            }
            oldResource.setTags(oldTagsAndValues);

            try {
                //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
                //validator.validate(resource, Action.UPDATE);
                ret = tagStore.updateResource(oldResource);
            } catch (Exception excp) {
                LOG.error("updateResource(" + id + ") failed", excp);

                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
            }
        } else {
            LOG.error("updateResource(" + id + ") failed, invalid operation " + op);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, "invalid update operation", true);
        }

        return ret;
    }

    @DELETE
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "/{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public void deleteResource(@PathParam("id") Long id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteResource(" + id + ")");
        }
        try {
            //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
            //validator.validate(guid, Action.DELETE);
            tagStore.deleteResource(id);
        } catch (Exception excp) {
            LOG.error("deleteResource(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteResource(" + id + ")");
        }

    }

    @GET
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "/{id}")
    @Produces({ "application/json", "application/xml" })
    public RangerResource getResource(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getResource(" + id + ")");
        }

        RangerResource ret;

        try {
            ret = tagStore.getResource(id);
        } catch(Exception excp) {
            LOG.error("getResource(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getResource(" + id + "): " + ret);
        }

        return ret;
    }

    @GET
    @Path(TagRESTConstants.RESOURCES_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<RangerResource> getResources(@DefaultValue("") @QueryParam(TagRESTConstants.TAG_SERVICE_NAME_PARAM) String tagServiceName,
                                             @DefaultValue("") @QueryParam(TagRESTConstants.COMPONENT_TYPE_PARAM) String componentType) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getResources(" + tagServiceName + ", " + componentType + ")");
        }

        List<RangerResource> ret;

        try {
            ret = tagStore.getResources(tagServiceName, componentType);
        } catch(Exception excp) {
            LOG.error("getResources(" + tagServiceName + ", " + componentType + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(ret == null) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
        }

        List<RangerResource> toBeFilteredOut = new ArrayList<RangerResource>();

        for (RangerResource rangerResource : ret) {
            if (CollectionUtils.isEmpty(rangerResource.getTags())) {
                toBeFilteredOut.add(rangerResource);
            }
        }
        ret.removeAll(toBeFilteredOut);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getResources(" + tagServiceName + "): " + ret);
        }

        return ret;
    }
}
