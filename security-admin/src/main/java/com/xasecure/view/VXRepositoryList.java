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
 * List wrapper class for VXRepository
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
public class VXRepositoryList extends VList {
	private static final long serialVersionUID = 1L;
	List<VXRepository> vXRepositories = new ArrayList<VXRepository>();

	public VXRepositoryList() {
		super();
	}

	public VXRepositoryList(List<VXRepository> objList) {
		super(objList);
		this.vXRepositories = objList;
	}

	/**
	 * @return the vXRepositories
	 */
	public List<VXRepository> getVXRepositories() {
		return vXRepositories;
	}

	/**
	 * @param vXRepositories
	 *            the vXRepositories to set
	 */
	public void setVXRepositories(List<VXRepository> vXRepositories) {
		this.vXRepositories = vXRepositories;
	}

	@Override
	public int getListSize() {
		if (vXRepositories != null) {
			return vXRepositories.size();
		}
		return 0;
	}

	@Override
	public List<VXRepository> getList() {
		return vXRepositories;
	}

}
