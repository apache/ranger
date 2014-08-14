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
 * List wrapper class for VXAccessAudit
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
public class VXAccessAuditList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXAccessAudit> vXAccessAudits = new ArrayList<VXAccessAudit>();

    public VXAccessAuditList() {
	super();
    }

    public VXAccessAuditList(List<VXAccessAudit> objList) {
	super(objList);
	this.vXAccessAudits = objList;
    }

    /**
     * @return the vXAccessAudits
     */
    public List<VXAccessAudit> getVXAccessAudits() {
	return vXAccessAudits;
    }

    /**
     * @param vXAccessAudits
     *            the vXAccessAudits to set
     */
    public void setVXAccessAudits(List<VXAccessAudit> vXAccessAudits) {
	this.vXAccessAudits = vXAccessAudits;
    }

    @Override
    public int getListSize() {
	if (vXAccessAudits != null) {
	    return vXAccessAudits.size();
	}
	return 0;
    }

    @Override
    public List<VXAccessAudit> getList() {
	return vXAccessAudits;
    }

}
