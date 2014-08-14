package com.xasecure.view;

/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

/**
 * List wrapper class for VXPolicy
 * @author tushar
 */

import java.util.*;
import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.xasecure.common.view.*;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VXPolicyList extends VList {
	private static final long serialVersionUID = 1L;
	List<VXPolicy> vXPolicies = new ArrayList<VXPolicy>();

	public VXPolicyList() {
		super();
	}

	public VXPolicyList(List<VXPolicy> objList) {
		super(objList);
		this.vXPolicies = objList;
	}

	/**
	 * @return the vXPolicies
	 */
	public List<VXPolicy> getVXPolicies() {
		return vXPolicies;
	}

	/**
	 * @param vXPolicies
	 *            the vXPolicies to set
	 */
	public void setVXPolicies(List<VXPolicy> vXPolicies) {
		this.vXPolicies = vXPolicies;
	}

	@Override
	public int getListSize() {
		if (vXPolicies != null) {
			return vXPolicies.size();
		}
		return 0;
	}

	@Override
	public List<VXPolicy> getList() {
		return vXPolicies;
	}

}
