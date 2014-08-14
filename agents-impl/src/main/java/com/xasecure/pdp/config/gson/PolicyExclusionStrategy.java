package com.xasecure.pdp.config.gson;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

public class PolicyExclusionStrategy implements ExclusionStrategy {

	@Override
	public boolean shouldSkipClass(Class<?> objectClass) {
		return (objectClass.getAnnotation(ExcludeSerialization.class) != null) ;
	}

	@Override
	public boolean shouldSkipField(FieldAttributes aFieldAttributes) {
		return  (aFieldAttributes.getAnnotation(ExcludeSerialization.class) != null) ;
	}

}
