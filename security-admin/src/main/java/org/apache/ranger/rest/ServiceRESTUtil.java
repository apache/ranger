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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ServiceRESTUtil {
	private static final Log LOG = LogFactory.getLog(ServiceRESTUtil.class);

	private enum POLICYITEM_TYPE {
		ALLOW, DENY, ALLOW_EXCEPTIONS, DENY_EXCEPTIONS
	}

	static public boolean processGrantRequest(RangerPolicy policy, GrantRevokeRequest grantRequest) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.processGrantRequest()");
		}

		boolean policyUpdated = false;

		// replace all existing privileges for users and groups
		if (grantRequest.getReplaceExistingPermissions()) {
			policyUpdated = removeUsersAndGroupsFromPolicy(policy, grantRequest.getUsers(), grantRequest.getGroups());
		}

		//Build a policy and set up policyItem in it to mimic grant request
		RangerPolicy appliedPolicy = new RangerPolicy();

		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

		policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
		policyItem.getUsers().addAll(grantRequest.getUsers());
		policyItem.getGroups().addAll(grantRequest.getGroups());

		List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

		Set<String> accessTypes = grantRequest.getAccessTypes();
		for (String accessType : accessTypes) {
			accesses.add(new RangerPolicy.RangerPolicyItemAccess(accessType, true));
		}

		policyItem.setAccesses(accesses);

		appliedPolicy.getPolicyItems().add(policyItem);

		processApplyPolicy(policy, appliedPolicy);

		policyUpdated = true;

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.processGrantRequest() : " + policyUpdated);
		}

		return policyUpdated;
	}

	static public boolean processRevokeRequest(RangerPolicy existingRangerPolicy, GrantRevokeRequest revokeRequest) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.processRevokeRequest()");
		}

		boolean policyUpdated = false;

		// remove all existing privileges for users and groups
		if (revokeRequest.getReplaceExistingPermissions()) {
			policyUpdated = removeUsersAndGroupsFromPolicy(existingRangerPolicy, revokeRequest.getUsers(), revokeRequest.getGroups());
		} else {
			//Build a policy and set up policyItem in it to mimic revoke request
			RangerPolicy appliedRangerPolicy = new RangerPolicy();

			RangerPolicy.RangerPolicyItem appliedRangerPolicyItem = new RangerPolicy.RangerPolicyItem();

			appliedRangerPolicyItem.setDelegateAdmin(revokeRequest.getDelegateAdmin());
			appliedRangerPolicyItem.getUsers().addAll(revokeRequest.getUsers());
			appliedRangerPolicyItem.getGroups().addAll(revokeRequest.getGroups());

			List<RangerPolicy.RangerPolicyItemAccess> appliedRangerPolicyItemAccess = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

			Set<String> appliedPolicyItemAccessType = revokeRequest.getAccessTypes();
			for (String accessType : appliedPolicyItemAccessType) {
				appliedRangerPolicyItemAccess.add(new RangerPolicy.RangerPolicyItemAccess(accessType, false));
			}

			appliedRangerPolicyItem.setAccesses(appliedRangerPolicyItemAccess);

			appliedRangerPolicy.getPolicyItems().add(appliedRangerPolicyItem);

			List<RangerPolicy.RangerPolicyItem> appliedRangerPolicyItems = appliedRangerPolicy.getPolicyItems();
			//processApplyPolicyForItemType(existingRangerPolicy, appliedRangerPolicy, POLICYITEM_TYPE.ALLOW);
			if (CollectionUtils.isNotEmpty(appliedRangerPolicyItems)) {
				Set<String> users = new HashSet<String>();
				Set<String> groups = new HashSet<String>();

				Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
				Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();

				// Extract users and groups specified in appliedPolicy items
				extractUsersAndGroups(appliedRangerPolicyItems, users, groups);

				// Split existing policyItems for users and groups extracted from appliedPolicyItem into userPolicyItems and groupPolicyItems
				splitExistingPolicyItems(existingRangerPolicy, users, userPolicyItems, groups, groupPolicyItems);

				for (RangerPolicy.RangerPolicyItem tempPolicyItem : appliedRangerPolicyItems) {
					List<String> appliedPolicyItemsUser = tempPolicyItem.getUsers();
					for (String user : appliedPolicyItemsUser) {
						RangerPolicy.RangerPolicyItem[] rangerPolicyItems = userPolicyItems.get(user);
						if(rangerPolicyItems!=null && rangerPolicyItems.length>0){
							if(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()]!=null){
								removeAccesses(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()], tempPolicyItem.getAccesses());
								if(!CollectionUtils.isEmpty(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].getAccesses())){
									rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].setDelegateAdmin(revokeRequest.getDelegateAdmin());
								}else{
									rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].setDelegateAdmin(Boolean.FALSE);
								}
							}
							if(rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()]!=null){
								removeAccesses(rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()], tempPolicyItem.getAccesses());
								rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()].setDelegateAdmin(Boolean.FALSE);
							}
						}
					}
				}
				for (RangerPolicy.RangerPolicyItem tempPolicyItem : appliedRangerPolicyItems) {
					List<String> appliedPolicyItemsGroup = tempPolicyItem.getGroups();
					for (String group : appliedPolicyItemsGroup) {
						RangerPolicy.RangerPolicyItem[] rangerPolicyItems = groupPolicyItems.get(group);
						if(rangerPolicyItems!=null && rangerPolicyItems.length>0){
							if(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()]!=null){
								removeAccesses(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()], tempPolicyItem.getAccesses());
								if(!CollectionUtils.isEmpty(rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].getAccesses())){
									rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].setDelegateAdmin(revokeRequest.getDelegateAdmin());
								}else{
									rangerPolicyItems[POLICYITEM_TYPE.ALLOW.ordinal()].setDelegateAdmin(Boolean.FALSE);
								}
							}
							if(rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()]!=null){
								removeAccesses(rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()], tempPolicyItem.getAccesses());
								rangerPolicyItems[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()].setDelegateAdmin(Boolean.FALSE);
							}
						}
					}
				}
				// Add modified/new policyItems back to existing policy
				mergeProcessedPolicyItems(existingRangerPolicy, userPolicyItems, groupPolicyItems);
				compactPolicy(existingRangerPolicy);
			}

			policyUpdated = true;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.processRevokeRequest() : " + policyUpdated);
		}

		return policyUpdated;
	}

	static public void processApplyPolicy(RangerPolicy existingPolicy, RangerPolicy appliedPolicy) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.processApplyPolicy()");
		}

		processApplyPolicyForItemType(existingPolicy, appliedPolicy, POLICYITEM_TYPE.ALLOW);
		processApplyPolicyForItemType(existingPolicy, appliedPolicy, POLICYITEM_TYPE.DENY);
		processApplyPolicyForItemType(existingPolicy, appliedPolicy, POLICYITEM_TYPE.ALLOW_EXCEPTIONS);
		processApplyPolicyForItemType(existingPolicy, appliedPolicy, POLICYITEM_TYPE.DENY_EXCEPTIONS);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.processApplyPolicy()");
		}
	}

	static private void processApplyPolicyForItemType(RangerPolicy existingPolicy, RangerPolicy appliedPolicy, POLICYITEM_TYPE policyItemType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.processApplyPolicyForItemType()");
		}

		List<RangerPolicy.RangerPolicyItem> appliedPolicyItems = null;

		switch (policyItemType) {
			case ALLOW:
				appliedPolicyItems = appliedPolicy.getPolicyItems();
				break;
			case DENY:
				appliedPolicyItems = appliedPolicy.getDenyPolicyItems();
				break;
			case ALLOW_EXCEPTIONS:
				appliedPolicyItems = appliedPolicy.getAllowExceptions();
				break;
			case DENY_EXCEPTIONS:
				appliedPolicyItems = appliedPolicy.getDenyExceptions();
				break;
			default:
				LOG.warn("processApplyPolicyForItemType(): invalid policyItemType=" + policyItemType);
		}

		if (CollectionUtils.isNotEmpty(appliedPolicyItems)) {

			Set<String> users = new HashSet<String>();
			Set<String> groups = new HashSet<String>();

			Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
			Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();

			// Extract users and groups specified in appliedPolicy items
			extractUsersAndGroups(appliedPolicyItems, users, groups);

			// Split existing policyItems for users and groups extracted from appliedPolicyItem into userPolicyItems and groupPolicyItems
			splitExistingPolicyItems(existingPolicy, users, userPolicyItems, groups, groupPolicyItems);

			// Apply policyItems of given type in appliedPolicy to policyItems extracted from existingPolicy
			applyPolicyItems(appliedPolicyItems, policyItemType, userPolicyItems, groupPolicyItems);

			// Add modified/new policyItems back to existing policy
			mergeProcessedPolicyItems(existingPolicy, userPolicyItems, groupPolicyItems);

			compactPolicy(existingPolicy);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.processApplyPolicyForItemType()");
		}
	}

	static private void extractUsersAndGroups(List<RangerPolicy.RangerPolicyItem> policyItems, Set<String> users, Set<String> groups) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.extractUsersAndGroups()");
		}
		if (CollectionUtils.isNotEmpty(policyItems)) {
			for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
				if (CollectionUtils.isNotEmpty(policyItem.getUsers())) {
					users.addAll(policyItem.getUsers());
				}
				if (CollectionUtils.isNotEmpty(policyItem.getGroups())) {
					groups.addAll(policyItem.getGroups());
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.extractUsersAndGroups()");
		}
	}

	static private void splitExistingPolicyItems(RangerPolicy existingPolicy,
												 Set<String> users, Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems, Set<String> groups,
												 Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems) {

		if (existingPolicy == null
				|| users == null || userPolicyItems == null
				|| groups == null || groupPolicyItems == null) {
			return;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.splitExistingPolicyItems()");
		}

		List<RangerPolicy.RangerPolicyItem> allowItems = existingPolicy.getPolicyItems();
		List<RangerPolicy.RangerPolicyItem> denyItems = existingPolicy.getDenyPolicyItems();
		List<RangerPolicy.RangerPolicyItem> allowExceptionItems = existingPolicy.getAllowExceptions();
		List<RangerPolicy.RangerPolicyItem> denyExceptionItems = existingPolicy.getDenyExceptions();

		for (String user : users) {
			RangerPolicy.RangerPolicyItem value[] = userPolicyItems.get(user);
			if (value == null) {
				value = new RangerPolicy.RangerPolicyItem[4];
				userPolicyItems.put(user, value);
			}

			RangerPolicy.RangerPolicyItem policyItem = null;

			policyItem = splitAndGetConsolidatedPolicyItemForUser(allowItems, user);
			value[POLICYITEM_TYPE.ALLOW.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForUser(denyItems, user);
			value[POLICYITEM_TYPE.DENY.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForUser(allowExceptionItems, user);
			value[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForUser(denyExceptionItems, user);
			value[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()] = policyItem;
		}

		for (String group : groups) {
			RangerPolicy.RangerPolicyItem value[] = groupPolicyItems.get(group);
			if (value == null) {
				value = new RangerPolicy.RangerPolicyItem[4];
				groupPolicyItems.put(group, value);
			}

			RangerPolicy.RangerPolicyItem policyItem = null;

			policyItem = splitAndGetConsolidatedPolicyItemForGroup(allowItems, group);
			value[POLICYITEM_TYPE.ALLOW.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForGroup(denyItems, group);
			value[POLICYITEM_TYPE.DENY.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForGroup(allowExceptionItems, group);
			value[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()] = policyItem;
			policyItem = splitAndGetConsolidatedPolicyItemForGroup(denyExceptionItems, group);
			value[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()] = policyItem;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.splitExistingPolicyItems()");
		}
	}

	static private RangerPolicy.RangerPolicyItem splitAndGetConsolidatedPolicyItemForUser(List<RangerPolicy.RangerPolicyItem> policyItems, String user) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForUser()");
		}

		RangerPolicy.RangerPolicyItem ret = null;

		if (CollectionUtils.isNotEmpty(policyItems)) {
			for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
				List<String> users = policyItem.getUsers();
				if (users.contains(user)) {
					if (ret == null) {
						ret = new RangerPolicy.RangerPolicyItem();
					}
					ret.getUsers().add(user);
					if (policyItem.getDelegateAdmin()) {
						ret.setDelegateAdmin(Boolean.TRUE);
					}
					addAccesses(ret, policyItem.getAccesses());

					// Remove this user from existingPolicyItem
					users.remove(user);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForUser()");
		}

		return ret;
	}

	static private RangerPolicy.RangerPolicyItem splitAndGetConsolidatedPolicyItemForGroup(List<RangerPolicy.RangerPolicyItem> policyItems, String group) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");
		}

		RangerPolicy.RangerPolicyItem ret = null;

		if (CollectionUtils.isNotEmpty(policyItems)) {
			for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
				List<String> groups = policyItem.getGroups();
				if (groups.contains(group)) {
					if (ret == null) {
						ret = new RangerPolicy.RangerPolicyItem();
					}
					ret.getGroups().add(group);
					if (policyItem.getDelegateAdmin()) {
						ret.setDelegateAdmin(Boolean.TRUE);
					}
					addAccesses(ret, policyItem.getAccesses());

					// Remove this group from existingPolicyItem
					groups.remove(group);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");
		}

		return ret;
	}

	static private void applyPolicyItems(List<RangerPolicy.RangerPolicyItem> appliedPolicyItems, POLICYITEM_TYPE policyItemType, Map<String, RangerPolicy.RangerPolicyItem[]> existingUserPolicyItems,
										 Map<String, RangerPolicy.RangerPolicyItem[]> existingGroupPolicyItems) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.applyPolicyItems()");
		}

		for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
			List<String> users = policyItem.getUsers();
			for (String user : users) {
				RangerPolicy.RangerPolicyItem[] items = existingUserPolicyItems.get(user);

				if (items == null) {
					// Should not get here
					LOG.warn("Should not have come here..");
					items = new RangerPolicy.RangerPolicyItem[4];
					existingUserPolicyItems.put(user, items);
				}

				addPolicyItemForUser(items, policyItemType.ordinal(), user, policyItem);

				switch (policyItemType) {
					case ALLOW:
						removeAccesses(items[POLICYITEM_TYPE.DENY.ordinal()], policyItem.getAccesses());
						removeAccesses(items[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()], policyItem.getAccesses());
						addPolicyItemForUser(items, POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal(), user, policyItem);
						break;
					case DENY:
						removeAccesses(items[POLICYITEM_TYPE.ALLOW.ordinal()], policyItem.getAccesses());
						addPolicyItemForUser(items, POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal(), user, policyItem);
						removeAccesses(items[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()], policyItem.getAccesses());
						break;
					case ALLOW_EXCEPTIONS:
						removeAccesses(items[POLICYITEM_TYPE.ALLOW.ordinal()], policyItem.getAccesses());
						break;
					case DENY_EXCEPTIONS:
						break;
					default:
						LOG.warn("Should not have come here..");
						break;
				}
			}
		}

		for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
			List<String> groups = policyItem.getGroups();
			for (String group : groups) {
				RangerPolicy.RangerPolicyItem[] items = existingGroupPolicyItems.get(group);

				if (items == null) {
					// Should not get here
					items = new RangerPolicy.RangerPolicyItem[4];
					existingGroupPolicyItems.put(group, items);
				}

				addPolicyItemForGroup(items, policyItemType.ordinal(), group, policyItem);

				switch (policyItemType) {
					case ALLOW:
						removeAccesses(items[POLICYITEM_TYPE.DENY.ordinal()], policyItem.getAccesses());
						removeAccesses(items[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()], policyItem.getAccesses());
						addPolicyItemForGroup(items, POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal(), group, policyItem);
						break;
					case DENY:
						removeAccesses(items[POLICYITEM_TYPE.ALLOW.ordinal()], policyItem.getAccesses());
						addPolicyItemForGroup(items, POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal(), group, policyItem);
						removeAccesses(items[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()], policyItem.getAccesses());
						break;
					case ALLOW_EXCEPTIONS:
						removeAccesses(items[POLICYITEM_TYPE.ALLOW.ordinal()], policyItem.getAccesses());
						break;
					case DENY_EXCEPTIONS:
						break;
					default:
						break;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.applyPolicyItems()");
		}
	}

	static private void mergeProcessedPolicyItems(RangerPolicy existingPolicy, Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems,
												  Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.mergeProcessedPolicyItems()");
		}

		for (Map.Entry<String, RangerPolicy.RangerPolicyItem[]> entry : userPolicyItems.entrySet()) {
			RangerPolicy.RangerPolicyItem[] items = entry.getValue();

			RangerPolicy.RangerPolicyItem item = null;

			item = items[POLICYITEM_TYPE.ALLOW.ordinal()];
			if (item != null) {
				existingPolicy.getPolicyItems().add(item);
			}

			item = items[POLICYITEM_TYPE.DENY.ordinal()];
			if (item != null) {
				existingPolicy.getDenyPolicyItems().add(item);
			}

			item = items[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()];
			if (item != null) {
				existingPolicy.getAllowExceptions().add(item);
			}

			item = items[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()];
			if (item != null) {
				existingPolicy.getDenyExceptions().add(item);
			}
		}

		for (Map.Entry<String, RangerPolicy.RangerPolicyItem[]> entry : groupPolicyItems.entrySet()) {
			RangerPolicy.RangerPolicyItem[] items = entry.getValue();

			RangerPolicy.RangerPolicyItem item = null;

			item = items[POLICYITEM_TYPE.ALLOW.ordinal()];
			if (item != null) {
				existingPolicy.getPolicyItems().add(item);
			}

			item = items[POLICYITEM_TYPE.DENY.ordinal()];
			if (item != null) {
				existingPolicy.getDenyPolicyItems().add(item);
			}

			item = items[POLICYITEM_TYPE.ALLOW_EXCEPTIONS.ordinal()];
			if (item != null) {
				existingPolicy.getAllowExceptions().add(item);
			}

			item = items[POLICYITEM_TYPE.DENY_EXCEPTIONS.ordinal()];
			if (item != null) {
				existingPolicy.getDenyExceptions().add(item);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.mergeProcessedPolicyItems()");
		}
	}

	static private boolean addAccesses(RangerPolicy.RangerPolicyItem policyItem, List<RangerPolicy.RangerPolicyItemAccess> accesses) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.addAccesses()");
		}

		boolean ret = false;

		for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
			RangerPolicy.RangerPolicyItemAccess policyItemAccess = null;
			String accessType = access.getType();

			for (RangerPolicy.RangerPolicyItemAccess itemAccess : policyItem.getAccesses()) {
				if (StringUtils.equals(itemAccess.getType(), accessType)) {
					policyItemAccess = itemAccess;
					break;
				}
			}

			if (policyItemAccess != null) {
				if (!policyItemAccess.getIsAllowed()) {
					policyItemAccess.setIsAllowed(Boolean.TRUE);
					ret = true;
				}
			} else {
				policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess(accessType, Boolean.TRUE));
				ret = true;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.addAccesses() " + ret);
		}
		return ret;
	}

	static private boolean removeAccesses(RangerPolicy.RangerPolicyItem policyItem, List<RangerPolicy.RangerPolicyItemAccess> accesses) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.removeAccesses()");
		}

		boolean ret = false;

		if (policyItem != null) {
			for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
				String accessType = access.getType();

				int numOfAccesses = policyItem.getAccesses().size();

				for (int i = 0; i < numOfAccesses; i++) {
					RangerPolicy.RangerPolicyItemAccess itemAccess = policyItem.getAccesses().get(i);

					if (StringUtils.equals(itemAccess.getType(), accessType)) {
						policyItem.getAccesses().remove(i);
						numOfAccesses--;
						i--;

						ret = true;
					}
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.removeAccesses() " + ret);
		}
		return ret;
	}

	static private void compactPolicy(RangerPolicy policy) {
		policy.setPolicyItems(mergePolicyItems(policy.getPolicyItems()));
		policy.setDenyPolicyItems(mergePolicyItems(policy.getDenyPolicyItems()));
		policy.setAllowExceptions(mergePolicyItems(policy.getAllowExceptions()));
		policy.setDenyExceptions(mergePolicyItems(policy.getDenyExceptions()));
	}

	static private List<RangerPolicy.RangerPolicyItem> mergePolicyItems(List<RangerPolicy.RangerPolicyItem> policyItems) {
		List<RangerPolicy.RangerPolicyItem> ret = new ArrayList<RangerPolicy.RangerPolicyItem>();

		if (CollectionUtils.isNotEmpty(policyItems)) {
			Map<String, RangerPolicy.RangerPolicyItem> matchedPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem>();

			for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
				if((CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) ||
				   (CollectionUtils.isEmpty(policyItem.getAccesses()) && !policyItem.getDelegateAdmin())) {
					continue;
				}

				if (policyItem.getConditions().size() > 1) {
					ret.add(policyItem);
					continue;
				}
				TreeSet<String> accesses = new TreeSet<String>();

				for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
					accesses.add(access.getType());
				}
				if (policyItem.getDelegateAdmin()) {
					accesses.add("delegateAdmin");
				}

				String allAccessesString = accesses.toString();

				RangerPolicy.RangerPolicyItem matchingPolicyItem = matchedPolicyItems.get(allAccessesString);

				if (matchingPolicyItem != null) {
					addDistinctItems(policyItem.getUsers(), matchingPolicyItem.getUsers());
					addDistinctItems(policyItem.getGroups(), matchingPolicyItem.getGroups());
				} else {
					matchedPolicyItems.put(allAccessesString, policyItem);
				}
			}

			for (Map.Entry<String, RangerPolicy.RangerPolicyItem> entry : matchedPolicyItems.entrySet()) {
				ret.add(entry.getValue());
			}
		}

		return ret;
	}

	static void addPolicyItemForUser(RangerPolicy.RangerPolicyItem[] items, int typeOfItems, String user, RangerPolicy.RangerPolicyItem policyItem) {

		if (items[typeOfItems] == null) {
			RangerPolicy.RangerPolicyItem newItem = new RangerPolicy.RangerPolicyItem();
			newItem.getUsers().add(user);

			items[typeOfItems] = newItem;
		}

		addAccesses(items[typeOfItems], policyItem.getAccesses());

		if (policyItem.getDelegateAdmin()) {
			items[typeOfItems].setDelegateAdmin(Boolean.TRUE);
		}
	}

	static void addPolicyItemForGroup(RangerPolicy.RangerPolicyItem[] items, int typeOfItems, String group, RangerPolicy.RangerPolicyItem policyItem) {

		if (items[typeOfItems] == null) {
			RangerPolicy.RangerPolicyItem newItem = new RangerPolicy.RangerPolicyItem();
			newItem.getGroups().add(group);

			items[typeOfItems] = newItem;
		}

		addAccesses(items[typeOfItems], policyItem.getAccesses());

		if (policyItem.getDelegateAdmin()) {
			items[typeOfItems].setDelegateAdmin(Boolean.TRUE);
		}
	}

	static private void addDistinctItems(List<String> fromItems, List<String> toItems) {
		for (String fromItem : fromItems) {
			if (! toItems.contains(fromItem)) {
				toItems.add(fromItem);
			}
		}
	}

	static private boolean removeUsersAndGroupsFromPolicy(RangerPolicy policy, Set<String> users, Set<String> groups) {
		boolean policyUpdated = false;

		List<RangerPolicy.RangerPolicyItem> policyItems = policy.getPolicyItems();

		int numOfItems = policyItems.size();

		for(int i = 0; i < numOfItems; i++) {
			RangerPolicy.RangerPolicyItem policyItem = policyItems.get(i);

			if(CollectionUtils.containsAny(policyItem.getUsers(), users)) {
				policyItem.getUsers().removeAll(users);

				policyUpdated = true;
			}

			if(CollectionUtils.containsAny(policyItem.getGroups(), groups)) {
				policyItem.getGroups().removeAll(groups);

				policyUpdated = true;
			}

			if(CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
				policyItems.remove(i);
				numOfItems--;
				i--;

				policyUpdated = true;
			}
		}

		return policyUpdated;
	}

	static boolean containsRangerCondition(RangerPolicy policy) {
		boolean ret = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTUtil.containsRangerCondition(" + policy +")");
		}

		if (policy != null) {
			List<RangerPolicy.RangerPolicyItem> allItems = new ArrayList<RangerPolicy.RangerPolicyItem>();

			allItems.addAll(policy.getPolicyItems());
			allItems.addAll(policy.getDenyPolicyItems());
			allItems.addAll(policy.getAllowExceptions());
			allItems.addAll(policy.getDenyExceptions());

			for (RangerPolicy.RangerPolicyItem policyItem : allItems) {
				if (!policyItem.getConditions().isEmpty()) {
					ret = true;
					break;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTUtil.containsRangerCondition(" + policy +"): " + ret);
		}

		return ret;
	}
}
