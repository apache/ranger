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
 * List wrapper class for VXResource
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
public class VXResourceList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXResource> vXResources = new ArrayList<VXResource>();

    public VXResourceList() {
	super();
    }

    public VXResourceList(List<VXResource> objList) {
	super(objList);
	this.vXResources = objList;
    }

    /**
     * @return the vXResources
     */
    public List<VXResource> getVXResources() {
	return vXResources;
    }

    /**
     * @param vXResources
     *            the vXResources to set
     */
    public void setVXResources(List<VXResource> vXResources) {
	this.vXResources = vXResources;
    }

    @Override
    public int getListSize() {
	if (vXResources != null) {
	    return vXResources.size();
	}
	return 0;
    }

    @Override
    public List<VXResource> getList() {
	return vXResources;
    }

}
