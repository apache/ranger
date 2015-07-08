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
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.plugin.model.RangerTaggedResourceKey;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.TagServiceResources;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
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
    ServiceRESTStore svcStore;
    */

    @Autowired
    ServiceDBStore svcStore;

    /*
    @Autowired
    TagFileStore tagStore;
    */

    private TagStore tagStore;

    public TagREST() {
    }

    @PostConstruct
    public void initStore() {
        tagStore = TagFileStore.getInstance();
        tagStore.setServiceStore(svcStore);
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
    public RangerTaggedResource createResource(RangerTaggedResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createResource(" + resource + ")");
        }

        RangerTaggedResource ret;

        try {
            //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
            //validator.validate(resource, Action.CREATE);
            ret = tagStore.createTaggedResource(resource, false);
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
    public RangerTaggedResource updateResource(@PathParam("id") Long id, RangerTaggedResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateResource(" + id + ")");
        }

        if (resource.getId() == null) {
            resource.setId(id);
        } else if (!resource.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "resource id mismatch", true);
        }

        RangerTaggedResource ret;

        try {
            //RangerResourceValidator validator = validatorFactory.getResourceValidator(tagStore);
            //validator.validate(resource, Action.UPDATE);
            ret = tagStore.updateTaggedResource(resource);
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

    public RangerTaggedResource updateResource(@PathParam("id") final Long id, @DefaultValue(TagRESTConstants.ACTION_ADD) @QueryParam(TagRESTConstants.ACTION_OP) String op, List<RangerTaggedResource.RangerResourceTag> resourceTagList) {

        RangerTaggedResource ret;

        if (op.equals(TagRESTConstants.ACTION_ADD) ||
                op.equals(TagRESTConstants.ACTION_REPLACE) ||
                op.equals(TagRESTConstants.ACTION_DELETE)) {
            RangerTaggedResource oldResource;
            try {
                oldResource = tagStore.getResource(id);
            } catch (Exception excp) {
                LOG.error("getResource(" + id + ") failed", excp);

                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
            }
            List<RangerTaggedResource.RangerResourceTag> oldTagsAndValues = oldResource.getTags();

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
                ret = tagStore.updateTaggedResource(oldResource);
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
    public RangerTaggedResource getResource(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getResource(" + id + ")");
        }

        RangerTaggedResource ret;

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

    // This API is typically used by plug-in to get selected tagged resources from RangerAdmin

    @GET
    @Path(TagRESTConstants.RESOURCES_UPDATED_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public TagServiceResources getResources(@QueryParam(TagRESTConstants.SERVICE_NAME_PARAM) String serviceName,
                                                   @QueryParam(TagRESTConstants.TAG_TIMESTAMP_PARAM) Long lastTimestamp) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getResources(" + serviceName + ", " + lastTimestamp + ")");
        }

        TagServiceResources ret = null;

        try {
            ret = tagStore.getResources(serviceName, lastTimestamp);
        } catch(Exception excp) {
            LOG.error("getResources(" + serviceName + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<==> TagREST.getResources(" + serviceName + ", " + lastTimestamp + ")");
        }

        return ret;
    }

    // This API is typically used by GUI to get all available tags from RangerAdmin

    @GET
    @Path(TagRESTConstants.TAGNAMES_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<String> getTagNames(@QueryParam(TagRESTConstants.SERVICE_NAME_PARAM) String serviceName) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagNames(" + serviceName + ")");
        }
        List<String> tagNames = null;

        try {
            tagNames = tagStore.getTags(serviceName);
        } catch(Exception excp) {
            LOG.error("getTags(" + serviceName + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagNames(" + serviceName + ")");
        }
        return tagNames;
    }

    // This API is typically used by GUI to help lookup available tags from RangerAdmin to help tag-policy writer. It
    // may also be used to validate configuration parameters of a tag-service

    @GET
    @Path(TagRESTConstants.LOOKUP_TAGS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<String> lookupTags(@QueryParam(TagRESTConstants.SERVICE_NAME_PARAM) String serviceName,
                                    @DefaultValue(".*") @QueryParam(TagRESTConstants.TAG_PATTERN_PARAM) String tagNamePattern) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.lookupTags(" + serviceName  + ", " + tagNamePattern + ")");
        }
        List<String> matchingTagNames = null;

        try {
            matchingTagNames = tagStore.lookupTags(serviceName, tagNamePattern);
        } catch(Exception excp) {
            LOG.error("lookupTags(" + serviceName + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.lookupTags(" + serviceName + ")");
        }
        return matchingTagNames;
    }

    // The following APIs will be typically used by tag-sync module

    // to get all tagged resources in RangerAdmin

    @GET
    @Path(TagRESTConstants.RESOURCES_ALL_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public TagServiceResources getAllTaggedResources() throws Exception {
        String emptyString = "";
        return getResources(emptyString, 0L);
    }

    // to create or update a tagged resource with associated tags in RangerAdmin

    @PUT
    @Path(TagRESTConstants.RESOURCE_SET_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public RangerTaggedResource setResource(RangerTaggedResourceKey key, List<RangerTaggedResource.RangerResourceTag> tags) throws Exception {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.setResource()");
        }

        RangerTaggedResource ret = null;

        RangerTaggedResource  taggedResource = new RangerTaggedResource(key, tags);

        try {
            ret = tagStore.createTaggedResource(taggedResource, true);        // Create or Update
        } catch(Exception excp) {
            LOG.error("setResource() failed", excp);
            LOG.error("Could not create taggedResource, " + taggedResource);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.setResource()");
        }

        return ret;
    }

    // to create or update a list of tagged resources with associated tags in RangerAdmin

    @PUT
    @Path(TagRESTConstants.RESOURCES_SET_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<RangerTaggedResource> setResources(List<RangerTaggedResourceKey> keys, List<RangerTaggedResource.RangerResourceTag> tags) throws Exception {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.setResources()");
        }
        List<RangerTaggedResource> ret = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(keys)) {
            for (RangerTaggedResourceKey key : keys) {
                try {
                    RangerTaggedResource taggedResource = setResource(key, tags);
                    if (taggedResource != null) {
                        ret.add(taggedResource);
                    }
                }
                catch(Exception e) {
                    // Ignore
                }
            }
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.setResources()");
        }
        return ret;
    }

    // to update a tagged resource by adding or removing tags from it in RangerAdmin

    @PUT
    @Path(TagRESTConstants.RESOURCE_UPDATE_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public RangerTaggedResource updateResourceTags(RangerTaggedResourceKey key, List<RangerTaggedResource.RangerResourceTag> tagsToAdd,
                                 List<RangerTaggedResource.RangerResourceTag> tagsToDelete) throws Exception {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateResource()");
        }

        RangerTaggedResource ret = null;
        RangerTaggedResource oldResource = null;
        try {
            oldResource = tagStore.getResource(key);
        } catch (Exception excp) {
            LOG.error("getResource(" + key + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if (oldResource != null) {
            List<RangerTaggedResource.RangerResourceTag> tags = oldResource.getTags();

            if (CollectionUtils.isNotEmpty(tagsToAdd)) {
                tags.addAll(tagsToAdd);
            }

            if (CollectionUtils.isNotEmpty(tagsToDelete)) {
                tags.removeAll(tagsToDelete);
            }

            oldResource.setTags(tags);

            try {
                ret = tagStore.updateTaggedResource(oldResource);
            } catch (Exception excp) {
                LOG.error("updateResource(" + key + ") failed", excp);

                throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
            }
        } else {
            LOG.error("updateResourceTags() could not find tagged resource with key=" + key);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateResource()");
        }

        return ret;
    }
}
