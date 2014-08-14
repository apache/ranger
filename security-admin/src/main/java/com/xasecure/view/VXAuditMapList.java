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
 * List wrapper class for VXAuditMap
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
public class VXAuditMapList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXAuditMap> vXAuditMaps = new ArrayList<VXAuditMap>();

    public VXAuditMapList() {
	super();
    }

    public VXAuditMapList(List<VXAuditMap> objList) {
	super(objList);
	this.vXAuditMaps = objList;
    }

    /**
     * @return the vXAuditMaps
     */
    public List<VXAuditMap> getVXAuditMaps() {
	return vXAuditMaps;
    }

    /**
     * @param vXAuditMaps
     *            the vXAuditMaps to set
     */
    public void setVXAuditMaps(List<VXAuditMap> vXAuditMaps) {
	this.vXAuditMaps = vXAuditMaps;
    }

    @Override
    public int getListSize() {
	if (vXAuditMaps != null) {
	    return vXAuditMaps.size();
	}
	return 0;
    }

    @Override
    public List<VXAuditMap> getList() {
	return vXAuditMaps;
    }

}
