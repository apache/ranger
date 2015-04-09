package org.apache.ranger.plugin.policyengine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;

public class RangerPolicyDbCache {
	private static final Log LOG = LogFactory.getLog(RangerPolicyDbCache.class);

	private static final RangerPolicyDbCache sInstance = new RangerPolicyDbCache();

	private final Map<String, RangerPolicyDb> policyDbCache = Collections.synchronizedMap(new HashMap<String, RangerPolicyDb>());

	public static RangerPolicyDbCache getInstance() {
		return sInstance;
	}

	public RangerPolicyDb getPolicyDb(String serviceName, ServiceStore svcStore) {
		RangerPolicyDb ret = null;

		if(serviceName != null) {
			ret = policyDbCache.get(serviceName);

			long policyVersion = ret != null ? ret.getPolicyVersion() : -1;

			if(svcStore != null) {
				try {
					ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, policyVersion);

					if(policies != null) {
						if(ret == null) {
							ret = new RangerPolicyDb(policies);

							policyDbCache.put(serviceName, ret);
						} else if(policies.getPolicyVersion() != null && !policies.getPolicyVersion().equals(policyVersion)) {
							ret = new RangerPolicyDb(policies);

							policyDbCache.put(serviceName, ret);
						}
					}
				} catch(Exception excp) {
					LOG.error("getPolicyDbForService(" + serviceName + "): failed to get latest policies from service-store", excp);
				}
			}
		}

		return ret;
	}
}
