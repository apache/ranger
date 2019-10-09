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

package org.apache.ranger.plugin.policyengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.RoleStore;
import org.apache.ranger.plugin.store.SecurityZoneStore;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

public class RangerPolicyEngineCache {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineCache.class);

	private final Map<String, RangerPolicyEngine> policyEngineCache = new HashMap<String, RangerPolicyEngine>();

	synchronized final RangerPolicyEngine getPolicyEngine(String serviceName, ServiceStore svcStore, RoleStore roleStore, SecurityZoneStore zoneStore, RangerPolicyEngineOptions options) {
		RangerPolicyEngine ret = null;

		if(serviceName != null) {
			ret = policyEngineCache.get(serviceName);

			if(svcStore != null) {
				long        policyVersion;
				long        roleVersion;
				RangerRoles rangerRoles;
				boolean     isRolesUpdated = true;

				try {
					if (ret == null) {
						policyVersion = -1L;
						roleVersion = -1L;
						rangerRoles = roleStore.getRangerRoles(serviceName, roleVersion);

						if (rangerRoles == null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("There are no roles in ranger-admin for service:" + serviceName +"]");
							}
						}
					} else {
						policyVersion = ret.getPolicyVersion();

						if (ret.getRangerRoles() != null && ret.getRangerRoles().getRoleVersion() != null) {
							roleVersion = ret.getRangerRoles().getRoleVersion();
						} else {
							roleVersion = -1L;
						}

						rangerRoles = roleStore.getRangerRoles(serviceName, roleVersion);

						if (rangerRoles == null) {
							rangerRoles = ret.getRangerRoles();
							isRolesUpdated = false;
						}
					}

					ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, policyVersion, false);

					if (policies != null) {
						if (policies.getPolicyVersion() != null && !policies.getPolicyVersion().equals(policyVersion)) {
							ServicePolicies updatedServicePolicies = getUpdatedServicePolicies(serviceName, policies, svcStore, zoneStore);

							if (ret == null) {
								ret = addPolicyEngine(updatedServicePolicies, rangerRoles, options);
							} else {
								ret = updatePolicyEngine(ret, updatedServicePolicies, rangerRoles, options);
							}
						} else {
							LOG.error("policies object is null or its version is null for getPolicyEngine(" + serviceName + ") !!");
							LOG.error("Returning old policy engine");
						}
					} else {
						if (ret == null) {
							LOG.error("getPolicyEngine(" + serviceName + "): failed to get any policies from service-store");
						} else {
							if (isRolesUpdated) {
								ret.setRangerRoles(rangerRoles);
							}
						}
					}

				} catch(Exception excp) {
					LOG.error("getPolicyEngine(" + serviceName + "): failed to get latest policies from service-store", excp);
				}
			}
		}

		return ret;
	}


	private RangerPolicyEngine addPolicyEngine(ServicePolicies policies, RangerRoles rangerRoles, RangerPolicyEngineOptions options) {
		RangerServiceDef serviceDef = policies.getServiceDef();
		String serviceType = (serviceDef != null) ? serviceDef.getName() : "";

		RangerPluginContext rangerPluginContext = new RangerPluginContext(serviceType);
		RangerPolicyEngine ret = new RangerPolicyEngineImpl("ranger-admin", policies, options, rangerPluginContext, rangerRoles);

		policyEngineCache.put(policies.getServiceName(), ret);

		return ret;
	}

	private RangerPolicyEngine updatePolicyEngine(RangerPolicyEngine policyEngine, ServicePolicies policies, RangerRoles rangerRoles, RangerPolicyEngineOptions options) {
		final RangerPolicyEngine ret;


		if (CollectionUtils.isNotEmpty(policies.getPolicyDeltas())) {
			RangerPolicyEngine updatedEngine = policyEngine.cloneWithDelta(policies, rangerRoles);
			if (updatedEngine != null) {
				policyEngineCache.put(policies.getServiceName(), updatedEngine);
				ret = updatedEngine;
			} else {
				LOG.warn("Could not cloneWithDelta policyEngine to policyVersion:[" + policies.getPolicyVersion() + "]");
				LOG.warn("Retaining old policyEngine with policyVersion:[" + policyEngine.getPolicyVersion() + "]");
				ret = policyEngine;
			}
		} else {
			ret = addPolicyEngine(policies, rangerRoles, options);
			((RangerPolicyEngineImpl)policyEngine).setIsShared(false);
		}

		return ret;
	}

	private ServicePolicies getUpdatedServicePolicies(String serviceName, ServicePolicies policies, ServiceStore svcStore, SecurityZoneStore zoneStore) throws  Exception{
		ServicePolicies ret = policies;
		if (ret == null) {
			ret = svcStore.getServicePoliciesIfUpdated(serviceName, -1L, false);
		}
		if (zoneStore != null) {
			Map<String, RangerSecurityZone.RangerSecurityZoneService> securityZones = zoneStore.getSecurityZonesForService(serviceName);
			if (MapUtils.isNotEmpty(securityZones)) {
				ret = getUpdatedServicePoliciesForZones(ret, securityZones);
			}
		}
		return ret;
	}

	public static ServicePolicies getUpdatedServicePoliciesForZones(ServicePolicies servicePolicies, Map<String, RangerSecurityZone.RangerSecurityZoneService> securityZones) {

		final ServicePolicies ret;

		if (MapUtils.isNotEmpty(securityZones)) {
			ret = new ServicePolicies();
			ret.setServiceDef(servicePolicies.getServiceDef());
			ret.setServiceId(servicePolicies.getServiceId());
			ret.setServiceName(servicePolicies.getServiceName());
			ret.setAuditMode(servicePolicies.getAuditMode());
			ret.setPolicyVersion(servicePolicies.getPolicyVersion());
			ret.setPolicyUpdateTime(servicePolicies.getPolicyUpdateTime());

			Map<String, ServicePolicies.SecurityZoneInfo> securityZonesInfo = new HashMap<>();

			if (CollectionUtils.isEmpty(servicePolicies.getPolicyDeltas())) {

				List<RangerPolicy> allPolicies = new ArrayList<>(servicePolicies.getPolicies());

				for (Map.Entry<String, RangerSecurityZone.RangerSecurityZoneService> entry : securityZones.entrySet()) {

					List<RangerPolicy> zonePolicies = extractZonePolicies(allPolicies, entry.getKey());

					if (CollectionUtils.isNotEmpty(zonePolicies)) {
						allPolicies.removeAll(zonePolicies);
					}

					ServicePolicies.SecurityZoneInfo securityZoneInfo = new ServicePolicies.SecurityZoneInfo();
					securityZoneInfo.setZoneName(entry.getKey());
					securityZoneInfo.setPolicies(zonePolicies);
					securityZoneInfo.setResources(entry.getValue().getResources());

					securityZoneInfo.setContainsAssociatedTagService(false);

					securityZonesInfo.put(entry.getKey(), securityZoneInfo);
				}

				ret.setPolicies(allPolicies);
				ret.setTagPolicies(servicePolicies.getTagPolicies());
			} else {
				List<RangerPolicyDelta> allPolicyDeltas = new ArrayList<>(servicePolicies.getPolicyDeltas());

				for (Map.Entry<String, RangerSecurityZone.RangerSecurityZoneService> entry : securityZones.entrySet()) {

					List<RangerPolicyDelta> zonePolicyDeltas = extractZonePolicyDeltas(allPolicyDeltas, entry.getKey());

					if (CollectionUtils.isNotEmpty(zonePolicyDeltas)) {
						allPolicyDeltas.removeAll(zonePolicyDeltas);
					}

					ServicePolicies.SecurityZoneInfo securityZoneInfo = new ServicePolicies.SecurityZoneInfo();
					securityZoneInfo.setZoneName(entry.getKey());
					securityZoneInfo.setPolicyDeltas(zonePolicyDeltas);
					securityZoneInfo.setResources(entry.getValue().getResources());

					securityZoneInfo.setContainsAssociatedTagService(false);

					securityZonesInfo.put(entry.getKey(), securityZoneInfo);
				}

				ret.setPolicyDeltas(allPolicyDeltas);
			}
			ret.setSecurityZones(securityZonesInfo);
		} else {
			ret = servicePolicies;
		}

		return ret;
	}

	private static List<RangerPolicy> extractZonePolicies(final List<RangerPolicy> allPolicies, final String zoneName) {

		final List<RangerPolicy> ret = new ArrayList<>();

		for (RangerPolicy policy : allPolicies) {
			if (policy.getIsEnabled() && StringUtils.equals(policy.getZoneName(), zoneName)) {
				ret.add(policy);
			}
		}

		return ret;
	}

	private static List<RangerPolicyDelta> extractZonePolicyDeltas(final List<RangerPolicyDelta> allPolicyDeltas, final String zoneName) {

		final List<RangerPolicyDelta> ret = new ArrayList<>();

		for (RangerPolicyDelta delta : allPolicyDeltas) {
			if (StringUtils.equals(delta.getZoneName(), zoneName)) {
				ret.add(delta);
			}
		}

		return ret;
	}
}
