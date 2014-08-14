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
 * List wrapper class for VXPermMap
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
public class VXPermMapList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXPermMap> vXPermMaps = new ArrayList<VXPermMap>();

    public VXPermMapList() {
	super();
    }

    public VXPermMapList(List<VXPermMap> objList) {
	super(objList);
	this.vXPermMaps = objList;
    }

    /**
     * @return the vXPermMaps
     */
    public List<VXPermMap> getVXPermMaps() {
	return vXPermMaps;
    }

    /**
     * @param vXPermMaps
     *            the vXPermMaps to set
     */
    public void setVXPermMaps(List<VXPermMap> vXPermMaps) {
	this.vXPermMaps = vXPermMaps;
    }

    @Override
    public int getListSize() {
	if (vXPermMaps != null) {
	    return vXPermMaps.size();
	}
	return 0;
    }

    @Override
    public List<VXPermMap> getList() {
	return vXPermMaps;
    }

}
