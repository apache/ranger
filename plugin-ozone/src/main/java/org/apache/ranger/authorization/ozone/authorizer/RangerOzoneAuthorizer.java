
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

package org.apache.ranger.authorization.ozone.authorizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authz.util.RangerResourceNameParser;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerInlinePolicy;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RangerOzoneAuthorizer implements IAccessAuthorizer {
	public static final String ACCESS_TYPE_READ        = "read";
	public static final String ACCESS_TYPE_WRITE       = "write";
	public static final String ACCESS_TYPE_CREATE      = "create";
	public static final String ACCESS_TYPE_LIST        = "list";
	public static final String ACCESS_TYPE_DELETE      = "delete";
	public static final String ACCESS_TYPE_READ_ACL    = "read_acl";
	public static final String ACCESS_TYPE_WRITE_ACL   = "write_acl";
	public static final String ACCESS_TYPE_ASSUME_ROLE = "assume_role";
	public static final String ACCESS_TYPE_ALL         = "all";

	public static final String KEY_RESOURCE_VOLUME = "volume";
	public static final String KEY_RESOURCE_BUCKET = "bucket";
	public static final String KEY_RESOURCE_KEY    = "key";
	public static final String KEY_RESOURCE_ROLE   = "role";

	private static final String S3_VOLUME_NAME = "s3Vol";

	private static final Logger PERF_OZONEAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("ozoneauth.request");

    private static final Logger LOG = LoggerFactory.getLogger(RangerOzoneAuthorizer.class);

	private static volatile RangerBasePlugin rangerPlugin = null;

	public RangerOzoneAuthorizer() {
		RangerBasePlugin plugin = rangerPlugin;

		if (plugin == null) {
			synchronized (RangerOzoneAuthorizer.class) {
				plugin = rangerPlugin;

				if (plugin == null) {
					plugin = new RangerBasePlugin("ozone", "ozone");
					plugin.init(); // this will initialize policy engine and policy refresher

					RangerDefaultAuditHandler auditHandler = new RangerDefaultAuditHandler();
					plugin.setResultProcessor(auditHandler);

					rangerPlugin = plugin;
				}
			}
		}
	}

    RangerOzoneAuthorizer(RangerBasePlugin plugin) {
        rangerPlugin = plugin;
    }

	@Override
	public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
		boolean returnValue = false;
		if (ozoneObject == null) {
			LOG.error("Ozone object is null!!");
			return returnValue;
		}
		OzoneObj ozoneObj = (OzoneObj) ozoneObject;
		UserGroupInformation ugi = context.getClientUgi();
		ACLType operation = context.getAclRights();
		String resource = ozoneObj.getPath();

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerOzoneAuthorizer.checkAccess with operation = " + operation + ", resource = " +
					resource + ", store type = " + OzoneObj.StoreType.values() + ", ugi = " + ugi + ", ip = " +
					context.getIp() + ", resourceType = " + ozoneObj.getResourceType() + ")");
		}

		RangerBasePlugin plugin = rangerPlugin;

		if (plugin == null) {
			MiscUtil.logErrorMessageByInterval(LOG,
					"Authorizer is still not initialized");
			return returnValue;
		}

		//TODO: If sorce type is S3 and resource is volume, then allow it by default
		if (ozoneObj.getStoreType() == OzoneObj.StoreType.S3 && ozoneObj.getResourceType() == OzoneObj.ResourceType.VOLUME) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("If store type is s3 and resource is volume, then we allow it by default!  Returning true");
			}
			LOG.warn("Allowing access by default since source type is S3 and resource type is Volume!!");
			return true;
		}

		RangerPerfTracer perf = null;

		if (RangerPerfTracer.isPerfTraceEnabled(PERF_OZONEAUTH_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_OZONEAUTH_REQUEST_LOG, "RangerOzoneAuthorizer.authorize(resource=" + resource + ")");
		}

		Date eventTime = new Date();
		String accessType = mapToRangerAccessType(operation);
		if (accessType == null) {
			MiscUtil.logErrorMessageByInterval(LOG,
					"Unsupported access type. operation=" + operation) ;
			LOG.error("Unsupported access type. operation=" + operation + ", resource=" + resource);
			return returnValue;
		}
		String action = accessType;
		String clusterName = plugin.getClusterName();

		RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
		rangerRequest.setUser(ugi.getShortUserName());
		rangerRequest.setUserGroups(Sets.newHashSet(ugi.getGroupNames()));
		rangerRequest.setClientIPAddress(context.getIp().getHostAddress());
		rangerRequest.setRemoteIPAddress(context.getIp().getHostAddress());
		rangerRequest.setAccessTime(eventTime);

		RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
		rangerResource.setOwnerUser(context.getOwnerName());

		rangerRequest.setResource(rangerResource);
		rangerRequest.setAccessType(accessType);
		rangerRequest.setAction(action);
		rangerRequest.setRequestData(resource);
		rangerRequest.setClusterName(clusterName);

		if (ozoneObj.getResourceType() == OzoneObj.ResourceType.VOLUME) {
			rangerResource.setValue(KEY_RESOURCE_VOLUME, ozoneObj.getVolumeName());
		} else if (ozoneObj.getResourceType() == OzoneObj.ResourceType.BUCKET || ozoneObj.getResourceType() == OzoneObj.ResourceType.KEY) {
			rangerResource.setValue(KEY_RESOURCE_VOLUME, ozoneObj.getStoreType() == OzoneObj.StoreType.S3 ? S3_VOLUME_NAME : ozoneObj.getVolumeName());
			rangerResource.setValue(KEY_RESOURCE_BUCKET, ozoneObj.getBucketName());
			if (ozoneObj.getResourceType() == OzoneObj.ResourceType.KEY) {
				rangerResource.setValue(KEY_RESOURCE_KEY, ozoneObj.getKeyName());
			}
		} else {
			LOG.error("Unsupported resource = " + resource);
			MiscUtil.logErrorMessageByInterval(LOG, "Unsupported resource type " + ozoneObj.getResourceType() + " for resource = " + resource
					+ ", request=" + rangerRequest);
			return returnValue;
		}

		try {
			if (StringUtils.isNotBlank(context.getSessionPolicy())) {
				rangerRequest.setInlinePolicy(JsonUtilsV2.jsonToObj(context.getSessionPolicy(), RangerInlinePolicy.class));
			}

			RangerAccessResult result = plugin
					.isAccessAllowed(rangerRequest);
			if (result == null) {
				LOG.error("Ranger Plugin returned null. Returning false");
			} else {
				returnValue = result.getIsAllowed();
			}
		} catch (Throwable t) {
			LOG.error("Error while calling isAccessAllowed(). request="
					+ rangerRequest, t);
		}
		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("rangerRequest=" + rangerRequest + ", return="
					+ returnValue);
		}
		return returnValue;
	}

    @Override
    public String generateAssumeRoleSessionPolicy(AssumeRoleRequest assumeRoleRequest) throws OMException {
        LOG.debug("==> RangerOzoneAuthorizer.generateAssumeRoleSessionPolicy(assumeRoleRequest={})", assumeRoleRequest);

        if (assumeRoleRequest == null) {
            throw new OMException("invalid request: null", OMException.ResultCodes.INVALID_REQUEST);
        } else if (assumeRoleRequest.getClientUgi() == null) {
            throw new OMException("invalid request: request.clientUgi null", OMException.ResultCodes.INVALID_REQUEST);
        } else if (assumeRoleRequest.getTargetRoleName() == null) {
            throw new OMException("invalid request: request.targetRoleName null", OMException.ResultCodes.INVALID_REQUEST);
        }

        RangerBasePlugin plugin = rangerPlugin;

        if (plugin == null) {
            throw new OMException("Ranger authorizer not initialized", OMException.ResultCodes.INTERNAL_ERROR);
        }

        UserGroupInformation     ugi      = assumeRoleRequest.getClientUgi();
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl(Collections.singletonMap(KEY_RESOURCE_ROLE, assumeRoleRequest.getTargetRoleName()));
        RangerAccessRequestImpl  request  = new RangerAccessRequestImpl(resource, ACCESS_TYPE_ASSUME_ROLE, ugi.getShortUserName(), Sets.newHashSet(ugi.getGroupNames()), null);

        try {
            RangerAccessResult result = plugin.isAccessAllowed(request);

            if (result != null && result.getIsAccessDetermined() && result.getIsAllowed()) {
                final Set<OzoneGrant>                ozoneGrants = assumeRoleRequest.getGrants();
                final List<RangerInlinePolicy.Grant> inlineGrants;

                if (ozoneGrants == null) { // allow all permissions
                    inlineGrants = null;
                } else if (ozoneGrants.isEmpty()) { // don't allow any permission
                    inlineGrants = Collections.singletonList(new RangerInlinePolicy.Grant());
                } else { // allow explicitly specified permissions
                    inlineGrants = ozoneGrants.stream().map(g -> toRangerGrant(g, plugin)).collect(Collectors.toList());
                }

                RangerInlinePolicy inlinePolicy = new RangerInlinePolicy(RangerPrincipal.PREFIX_ROLE + assumeRoleRequest.getTargetRoleName(), RangerInlinePolicy.Mode.INLINE, inlineGrants, ugi.getShortUserName());
                String             ret          = JsonUtilsV2.objToJson(inlinePolicy);

                LOG.debug("<== RangerOzoneAuthorizer.generateAssumeRoleSessionPolicy(assumeRoleRequest={}): ret={}", assumeRoleRequest, ret);

                return ret;
            } else {
                throw new OMException("Permission denied", OMException.ResultCodes.ACCESS_DENIED);
            }
        } catch (OMException excp) {
            throw excp;
        } catch (Throwable t) {
            LOG.error("isAccessAllowed() failed. request = {}", request, t);

            throw new OMException("Ranger authorizer failed", t, OMException.ResultCodes.INTERNAL_ERROR);
        }
    }

	private String mapToRangerAccessType(ACLType operation) {
		String rangerAccessType = null;
		switch (operation) {
			case READ:
				rangerAccessType = ACCESS_TYPE_READ;
				break;
			case WRITE:
				rangerAccessType = ACCESS_TYPE_WRITE;
				break;
			case CREATE:
				rangerAccessType = ACCESS_TYPE_CREATE;
				break;
			case DELETE:
				rangerAccessType = ACCESS_TYPE_DELETE;
				break;
			case LIST:
				rangerAccessType = ACCESS_TYPE_LIST;
				break;
			case READ_ACL:
				rangerAccessType = ACCESS_TYPE_READ_ACL;
				break;
			case WRITE_ACL:
				rangerAccessType = ACCESS_TYPE_WRITE_ACL;
				break;
		}
		return rangerAccessType;
	}

    private static RangerInlinePolicy.Grant toRangerGrant(OzoneGrant ozoneGrant, RangerBasePlugin plugin) {
        RangerInlinePolicy.Grant ret = new RangerInlinePolicy.Grant();

        if (ozoneGrant.getObjects() != null) {
            ret.setResources(ozoneGrant.getObjects().stream().map(o -> toRrn(o, plugin)).filter(Objects::nonNull).collect(Collectors.toSet()));
        }

        if (ozoneGrant.getPermissions() != null) {
            ret.setPermissions(ozoneGrant.getPermissions().stream().map(RangerOzoneAuthorizer::toRangerPermission).filter(Objects::nonNull).collect(Collectors.toSet()));
        }

        LOG.debug("toRangerGrant(ozoneGrant={}): ret={}", ozoneGrant, ret);

        return ret;
    }

    private static String toRrn(IOzoneObj obj, RangerBasePlugin plugin) {
        OzoneObj            ozoneObj = (OzoneObj) obj;
        Map<String, String> resource = new HashMap<>();
        String              resType  = null;

        switch (ozoneObj.getResourceType()) {
            case VOLUME:
                resType = KEY_RESOURCE_VOLUME;

                resource.put(KEY_RESOURCE_VOLUME, ozoneObj.getVolumeName());
                break;

            case BUCKET:
                resType = KEY_RESOURCE_BUCKET;

                resource.put(KEY_RESOURCE_VOLUME, ozoneObj.getStoreType() == OzoneObj.StoreType.S3 ? S3_VOLUME_NAME : ozoneObj.getVolumeName());
                resource.put(KEY_RESOURCE_BUCKET, ozoneObj.getBucketName());
                break;

            case KEY:
                resType = KEY_RESOURCE_KEY;

                resource.put(KEY_RESOURCE_VOLUME, ozoneObj.getStoreType() == OzoneObj.StoreType.S3 ? S3_VOLUME_NAME : ozoneObj.getVolumeName());
                resource.put(KEY_RESOURCE_BUCKET, ozoneObj.getBucketName());
                resource.put(KEY_RESOURCE_KEY, ozoneObj.getKeyName());
                break;
        }

        RangerResourceNameParser rrnParser = resType != null ? plugin.getServiceDefHelper().getRrnParser(resType) : null;
        String                   ret       = rrnParser != null ? (resType + RangerResourceNameParser.RRN_RESOURCE_TYPE_SEP + rrnParser.toResourceName(resource)) : null;

        LOG.debug("toRrn(ozoneObj={}): ret={}", ozoneObj, ret);

        return ret;
    }


    private static String toRangerPermission(ACLType acl) {
        switch (acl) {
            case READ:
                return ACCESS_TYPE_READ;
            case WRITE:
                return ACCESS_TYPE_WRITE;
            case CREATE:
                return ACCESS_TYPE_CREATE;
            case LIST:
                return ACCESS_TYPE_LIST;
            case DELETE:
                return ACCESS_TYPE_DELETE;
            case READ_ACL:
                return ACCESS_TYPE_READ_ACL;
            case WRITE_ACL:
                return ACCESS_TYPE_WRITE_ACL;
            case ALL:
                return ACCESS_TYPE_ALL;
            case NONE:
            case ASSUME_ROLE: // ASSUME_ROLE is not supported in session policy
                return null;
        }

        return null;
    }
}

