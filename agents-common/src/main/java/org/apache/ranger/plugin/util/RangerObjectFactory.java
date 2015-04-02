package org.apache.ranger.plugin.util;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.validation.RangerPolicyResourceSignature;

public class RangerObjectFactory {
	public RangerPolicyResourceSignature createPolicyResourceSignature(RangerPolicy policy) {
		return new RangerPolicyResourceSignature(policy);
	}
}
