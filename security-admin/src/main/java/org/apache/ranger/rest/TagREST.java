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
import org.apache.commons.collections.ListUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.biz.ServiceDBStore;
//import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.store.TagValidator;

import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
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

	@Autowired
	ServiceDBStore svcStore;

	//@Autowired
	//TagDBStore tagStore;

    TagStore tagStore;

    //@Autowired
    //TagValidator validator;

    TagValidator validator;

    public TagREST() {
	}

	@PostConstruct
	public void initStore() {
        tagStore = TagFileStore.getInstance();
        tagStore.setServiceStore(svcStore);
        validator = new TagValidator();
        validator.setTagStore(tagStore);
	}

    @POST
    @Path(TagRESTConstants.TAGDEFS_RESOURCE)
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
    @Path(TagRESTConstants.TAGDEF_RESOURCE + "/{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerTagDef updateTagDef(@PathParam("id") Long id, RangerTagDef tagDef) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateTagDef(" + id + ")");
        }
        if (tagDef.getId() == null) {
            tagDef.setId(id);
        } else if (!tagDef.getId().equals(id)) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "tag name mismatch", true);
        }

        RangerTagDef ret;

        try {
            ret = tagStore.updateTagDef(tagDef);
        } catch (Exception excp) {
            LOG.error("updateTagDef(" + id + ") failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateTagDef(" + id + ")");
        }

        return ret;
    }

    @DELETE
    @Path(TagRESTConstants.TAGDEF_RESOURCE + "/{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteTagDef(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagDef(" + id + ")");
        }

        try {
            //RangerTagDefValidator validator = validatorFactory.getTagDefValidator(tagStore);
            //validator.validate(guid, Action.DELETE);
            tagStore.deleteTagDefById(id);
        } catch(Exception excp) {
            LOG.error("deleteTagDef(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteTagDef(" + id + ")");
        }
    }

    @GET
    @Path(TagRESTConstants.TAGDEF_RESOURCE+"/{name}")
    @Produces({ "application/json", "application/xml" })
    public List<RangerTagDef> getTagDefByName(@PathParam("name") String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagDefByName(" + name + ")");
        }

        List<RangerTagDef> ret;

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
    @Path(TagRESTConstants.TAGDEFS_RESOURCE)
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
    @Path(TagRESTConstants.TAGS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerTag createTag(RangerTag tag) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createTag(" + tag + ")");
        }

        RangerTag ret;

        try {
            validator.preCreateTag(tag);
            ret = tagStore.createTag(tag);
        } catch(Exception excp) {
            LOG.error("createTag(" + tag + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.createTag(" + tag + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.TAG_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerTag updateTagById(@PathParam("id") Long id, RangerTag tag) {

        RangerTag ret;

        try {
            validator.preUpdateTagById(id, tag);
            ret = tagStore.updateTag(tag);
        } catch (Exception excp) {
            LOG.error("updateTag() failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateTag(): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.TAG_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerTag updateTagByExternalId(@PathParam("externalId") String externalId, RangerTag tag) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateTagByExternalId(" + externalId + ")");
        }

        RangerTag ret;

        try {
            validator.preUpdateTagByExternalId(externalId, tag);
            ret = tagStore.updateTag(tag);
        } catch (Exception excp) {
            LOG.error("updateTagByExternalId(" + externalId + ") failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateTagByExternalId(" + externalId + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.TAG_RESOURCE + "name/{name}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerTag updateTagByName(@PathParam("name") String name, RangerTag tag) {

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateTagByName(" + name + ")");
        }

        RangerTag ret;

        try {
            validator.preUpdateTagByName(name, tag);
            ret = tagStore.updateTag(tag);
        } catch (Exception excp) {
            LOG.error("updateTagByName(" + name + ") failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateTagByName(" + name + "): " + ret);
        }

        return ret;
    }

    @DELETE
    @Path(TagRESTConstants.TAG_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public void deleteTagById(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagById(" + id +")");
        }

        try {
            validator.preDeleteTagById(id);
            tagStore.deleteTagById(id);
        } catch(Exception excp) {
            LOG.error("deleteTag() failed", excp);
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteTag()");
        }
    }

    @DELETE
    @Path(TagRESTConstants.TAG_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteTagByExternalId(@PathParam("externalId") String externalId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagByExternalId(" + externalId + ")");
        }

        try {
            RangerTag exist = validator.preDeleteTagByExternalId(externalId);
            tagStore.deleteTagById(exist.getId());
        } catch(Exception excp) {
            LOG.error("deleteTagByExternalId(" + externalId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteTagByExternalId(" + externalId + ")");
        }
    }

    @DELETE
    @Path(TagRESTConstants.TAG_RESOURCE + "name/{name}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteTagByName(@PathParam("name") String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagByName(" + name + ")");
        }

        try {
            RangerTag exist = validator.preDeleteTagByName(name);
            tagStore.deleteTagById(exist.getId());
        } catch(Exception excp) {
            LOG.error("deleteTagByName(" + name + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteTagByName(" + name + ")");
        }
    }

    @GET
    @Path(TagRESTConstants.TAGS_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    public RangerTag getTagById(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagById(" + id + ")");
        }
        RangerTag ret;

        try {
            ret = tagStore.getTagById(id);
        } catch(Exception excp) {
            LOG.error("getTagById(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagById(" + id + "): " + ret);
        }

        return ret;
    }

    @GET
    @Path(TagRESTConstants.TAGS_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    public List<RangerTag> getTagsByExternalId(@PathParam("externalId") String externalId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagsByExternalId(" + externalId + ")");
        }
        List<RangerTag> ret;

        try {
            ret = tagStore.getTagsByExternalId(externalId);
        } catch(Exception excp) {
            LOG.error("getTagsByExternalId(" + externalId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagsByExternalId(" + externalId + "): " + ret);
        }

        return ret;
    }

    @GET
    @Path(TagRESTConstants.TAGS_RESOURCE + "name/{name}")
    @Produces({ "application/json", "application/xml" })
    public List<RangerTag> getTagsByName(@PathParam("name") String name) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getTagsByName(" + name + ")");
        }
        List<RangerTag> ret;

        try {
            ret = tagStore.getTagsByName(name);
        } catch(Exception excp) {
            LOG.error("getTagsByName(" + name + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getTagsByName(" + name + "): " + ret);
        }

        return ret;
    }

    @GET
    @Path(TagRESTConstants.TAGS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    public List<RangerTag> getAllTags() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getAllTags()");
        }

        List<RangerTag> ret;

        try {
            ret = tagStore.getTags(new SearchFilter());
        } catch(Exception excp) {
            LOG.error("getAllTags() failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if (CollectionUtils.isEmpty(ret)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getAllTags() - No tags found");
            }
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getAllTags(): " + ret);
        }

        return ret;
    }

    @POST
    @Path(TagRESTConstants.RESOURCES_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerServiceResource createServiceResource(RangerServiceResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createServiceResource(" + resource + ")");
        }

        RangerServiceResource ret;

        try {
            validator.preCreateServiceResource(resource);
            ret = tagStore.createServiceResource(resource);
        } catch(Exception excp) {
            LOG.error("createServiceResource(" + resource + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.createServiceResource(" + resource + "): " + ret);
        }

        return ret;
    }

    @PUT
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerServiceResource updateServiceResourceById(@PathParam("id") Long id, RangerServiceResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateServiceResourceById(" + id + ")");
        }
        RangerServiceResource ret;

        try {
            validator.preUpdateServiceResourceById(id, resource);
            ret = tagStore.updateServiceResource(resource);
        } catch(Exception excp) {
            LOG.error("updateServiceResourceById(" + resource + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateServiceResourceById(" + id + "): " + ret);
        }
        return ret;
    }

    @PUT
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")

    public RangerServiceResource updateServiceResourceByExternalId(@PathParam("externalId") String externalId, RangerServiceResource resource) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.updateServiceResourceByExternalId(" + externalId + ", " + resource + ")");
        }
        RangerServiceResource ret;
        try {
            validator.preUpdateServiceResourceByExternalId(externalId, resource);
            ret = tagStore.updateServiceResource(resource);
        } catch(Exception excp) {
            LOG.error("updateServiceResourceByExternalId(" + externalId + ", " + resource + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.updateServiceResourceByExternalId(" + externalId + ", " + resource + "): " + ret);
        }
        return ret;
    }

    @DELETE
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteServiceResourceById(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteServiceResourceById(" + id + ")");
        }
        try {
            validator.preDeleteServiceResourceById(id);
            tagStore.deleteServiceResourceById(id);
        } catch (Exception excp) {
            LOG.error("deleteServiceResourceById() failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteServiceResourceById(" + id + ")");
        }
    }

    @DELETE
    @Path(TagRESTConstants.RESOURCE_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteServiceResourceByExternalId(@PathParam("externalId") String externalId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteServiceResourceByExternalId(" + externalId + ")");
        }

        try {
            RangerServiceResource exist = validator.preDeleteServiceResourceByExternalId(externalId);
            tagStore.deleteServiceResourceById(exist.getId());
        } catch(Exception excp) {
            LOG.error("deleteServiceResourceByExternalId(" + externalId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.deleteServiceResourceByExternalId(" + externalId + ")");
        }
    }

    @GET
    @Path(TagRESTConstants.RESOURCES_RESOURCE + "{id}")
    @Produces({ "application/json", "application/xml" })
    public RangerServiceResource getServiceResourceById(@PathParam("id") Long id) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getServiceResourceById(" + id + ")");
        }
        RangerServiceResource ret;
        try {
            ret = tagStore.getServiceResourceById(id);
        } catch(Exception excp) {
            LOG.error("getServiceResourceById(" + id + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getServiceResourceById(" + id + "): " + ret);
        }
        return ret;
    }

    @GET
    @Path(TagRESTConstants.RESOURCES_RESOURCE + "externalId/{externalId}")
    @Produces({ "application/json", "application/xml" })
    public List<RangerServiceResource> getServiceResourcesByExternalId(@PathParam("externalId") String externalId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getServiceResourceByExternalId(" + externalId + ")");
        }
        List<RangerServiceResource> ret;
        try {
            ret = tagStore.getServiceResourcesByExternalId(externalId);
        } catch(Exception excp) {
            LOG.error("getServiceResourceByExternalId(" + externalId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getServiceResourceByExternalId(" + externalId + "): " + ret);
        }
        return ret;
    }

    @POST
    @Path(TagRESTConstants.TAGRESOURCEMAPS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public RangerTagResourceMap createTagResourceMap(@QueryParam("externalResourceId") String externalResourceId,
                                                     @QueryParam("externalTagId") String externalTagId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createTagResourceMap(" + externalResourceId + ", " + externalTagId + ")");
        }

        RangerTagResourceMap tagResourceMap;

        try {
            tagResourceMap = validator.preCreateTagResourceMap(externalResourceId, externalTagId);
            tagResourceMap = tagStore.createTagResourceMap(tagResourceMap);
        } catch(Exception excp) {
            LOG.error("createTagResourceMap(" + externalResourceId + ", " + externalTagId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.createTagResourceMap(" + externalResourceId + ", " + externalTagId + ")");
        }

        return tagResourceMap;
    }

    @DELETE
    @Path(TagRESTConstants.TAGRESOURCEMAPS_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public void deleteTagResourceMap(@QueryParam("externalResourceId") String externalResourceId,
                                     @QueryParam("externalTagId") String externalTagId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagResourceMap(" + externalResourceId + ", " + externalTagId + ")");
        }

        try {
            RangerTagResourceMap exist = validator.preDeleteTagResourceMap(externalResourceId, externalTagId);
            tagStore.deleteTagResourceMapById(exist.getId());
        } catch(Exception excp) {
            LOG.error("deleteTagResourceMap(" + externalResourceId + ", " + externalTagId + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.deleteTagResourceMap(" + externalResourceId + ", " + externalTagId + ")");
        }
    }

    /*
        This leads to a WARNING in catalina.out -
        WARNING: The following warnings have been detected with resource and/or provider classes:
        WARNING: A HTTP GET method, public java.util.List org.apache.ranger.rest.TagREST.getServiceResources(org.apache.ranger.plugin.model.RangerServiceResource) throws java.lang.Exception, should not consume any entity.
        Hence commented out..
     */
    /*
    @GET
    @Path(TagRESTConstants.RESOURCES_RESOURCE)
    @Produces({ "application/json", "application/xml" })
    //@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public List<RangerServiceResource> getServiceResources(RangerServiceResource resource) throws Exception {

        List<RangerServiceResource> ret = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getServiceResources(" + resource + ")");
        }
        ret = tagStore.getServiceResourcesByServiceAndResourceSpec(resource.getServiceName(), resource.getResourceSpec());
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== TagREST.getServiceResources(" + resource + ")");
        }
        return ret;
    }
    */

    // This API is typically used by plug-in to get selected tagged resources from RangerAdmin

    @GET
    @Path(TagRESTConstants.TAGS_DOWNLOAD + "{serviceName}")
    @Produces({ "application/json", "application/xml" })
    public ServiceTags getServiceTagsIfUpdated(@PathParam("serviceName") String serviceName,
                                                   @QueryParam(TagRESTConstants.LAST_KNOWN_TAG_VERSION_PARAM) Long lastKnownVersion, @QueryParam("pluginId") String pluginId) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> TagREST.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + pluginId + ")");
        }

        ServiceTags ret = null;

        try {
            ret = tagStore.getServiceTagsIfUpdated(serviceName, lastKnownVersion);
        } catch(Exception excp) {
            LOG.error("getServiceTagsIfUpdated(" + serviceName + ") failed", excp);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<==> TagREST.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + pluginId + ")");
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
                                    @DefaultValue(".*") @QueryParam(TagRESTConstants.PATTERN_PARAM) String tagNamePattern) {
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

}
