package org.apache.ranger.plugin.conditionevaluator;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;

public interface RangerConditionEvaluator {

	void init(RangerPolicyItemCondition condition);
	boolean isMatched(RangerAccessRequest request);
}
