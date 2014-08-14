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
 * List wrapper class for VXPermObj
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
public class VXPermObjList extends VList {
	private static final long serialVersionUID = 1L;
	List<VXPermObj> vXPermObjs = new ArrayList<VXPermObj>();

	public VXPermObjList() {
		super();
	}

	public VXPermObjList(List<VXPermObj> objList) {
		super(objList);
		this.vXPermObjs = objList;
	}

	/**
	 * @return the vXPermObjs
	 */
	public List<VXPermObj> getVXPermObjs() {
		return vXPermObjs;
	}

	/**
	 * @param vXPermObjs
	 *            the vXPermObjs to set
	 */
	public void setVXPermObjs(List<VXPermObj> vXPermObjs) {
		this.vXPermObjs = vXPermObjs;
	}

	@Override
	public int getListSize() {
		if (vXPermObjs != null) {
			return vXPermObjs.size();
		}
		return 0;
	}

	@Override
	public List<VXPermObj> getList() {
		return vXPermObjs;
	}

}
