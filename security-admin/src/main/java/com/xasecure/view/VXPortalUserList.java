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
 * List wrapper class for VXPortalUser
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
public class VXPortalUserList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXPortalUser> vXPortalUsers = new ArrayList<VXPortalUser>();

    public VXPortalUserList() {
	super();
    }

    public VXPortalUserList(List<VXPortalUser> objList) {
	super(objList);
	this.vXPortalUsers = objList;
    }

    /**
     * @return the vXPortalUsers
     */
    public List<VXPortalUser> getVXPortalUsers() {
	return vXPortalUsers;
    }

    /**
     * @param vXPortalUsers
     *            the vXPortalUsers to set
     */
    public void setVXPortalUsers(List<VXPortalUser> vXPortalUsers) {
	this.vXPortalUsers = vXPortalUsers;
    }

    @Override
    public int getListSize() {
	if (vXPortalUsers != null) {
	    return vXPortalUsers.size();
	}
	return 0;
    }

    @Override
    public List<VXPortalUser> getList() {
	return vXPortalUsers;
    }

}
