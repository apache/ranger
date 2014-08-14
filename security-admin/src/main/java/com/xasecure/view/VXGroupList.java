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
 * List wrapper class for VXGroup
 * 
 */

import java.util.*;
import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.xasecure.common.view.*;

@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VXGroupList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXGroup> vXGroups = new ArrayList<VXGroup>();

    public VXGroupList() {
	super();
    }

    public VXGroupList(List<VXGroup> objList) {
	super(objList);
	this.vXGroups = objList;
    }

    /**
     * @return the vXGroups
     */
    public List<VXGroup> getVXGroups() {
	return vXGroups;
    }

    /**
     * @param vXGroups
     *            the vXGroups to set
     */
    public void setVXGroups(List<VXGroup> vXGroups) {
	this.vXGroups = vXGroups;
    }

    @Override
    public int getListSize() {
	if (vXGroups != null) {
	    return vXGroups.size();
	}
	return 0;
    }

    @Override
    public List<VXGroup> getList() {
	return vXGroups;
    }

}
