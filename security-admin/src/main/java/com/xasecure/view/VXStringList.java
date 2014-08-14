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
 * List wrapper class for VXString
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
public class VXStringList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXString> vXStrings = new ArrayList<VXString>();

    public VXStringList() {
	super();
    }

    public VXStringList(List<VXString> objList) {
	super(objList);
	this.vXStrings = objList;
    }

    /**
     * @return the vXStrings
     */
    public List<VXString> getVXStrings() {
	return vXStrings;
    }

    /**
     * @param vXStrings
     *            the vXStrings to set
     */
    public void setVXStrings(List<VXString> vXStrings) {
	this.vXStrings = vXStrings;
    }

    @Override
    public int getListSize() {
	if (vXStrings != null) {
	    return vXStrings.size();
	}
	return 0;
    }

    @Override
    public List<VXString> getList() {
	return vXStrings;
    }

}
