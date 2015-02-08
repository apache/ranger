package org.apache.ranger.plugin.conditionevaluator;

import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;

public interface RangerConditionEvaluator {

	void init(RangerPolicyItemCondition condition);
	boolean isMatched(String value);
}
