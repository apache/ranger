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
 * List wrapper class for VXPolicyExportAudit
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
public class VXPolicyExportAuditList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXPolicyExportAudit> vXPolicyExportAudits = new ArrayList<VXPolicyExportAudit>();

    public VXPolicyExportAuditList() {
	super();
    }

    public VXPolicyExportAuditList(List<VXPolicyExportAudit> objList) {
	super(objList);
	this.vXPolicyExportAudits = objList;
    }

    /**
     * @return the vXPolicyExportAudits
     */
    public List<VXPolicyExportAudit> getVXPolicyExportAudits() {
	return vXPolicyExportAudits;
    }

    /**
     * @param vXPolicyExportAudits
     *            the vXPolicyExportAudits to set
     */
    public void setVXPolicyExportAudits(List<VXPolicyExportAudit> vXPolicyExportAudits) {
	this.vXPolicyExportAudits = vXPolicyExportAudits;
    }

    @Override
    public int getListSize() {
	if (vXPolicyExportAudits != null) {
	    return vXPolicyExportAudits.size();
	}
	return 0;
    }

    @Override
    public List<VXPolicyExportAudit> getList() {
	return vXPolicyExportAudits;
    }

}
