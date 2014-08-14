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
 * List wrapper class for VXGroupUser
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
public class VXGroupUserList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXGroupUser> vXGroupUsers = new ArrayList<VXGroupUser>();

    public VXGroupUserList() {
	super();
    }

    public VXGroupUserList(List<VXGroupUser> objList) {
	super(objList);
	this.vXGroupUsers = objList;
    }

    /**
     * @return the vXGroupUsers
     */
    public List<VXGroupUser> getVXGroupUsers() {
	return vXGroupUsers;
    }

    /**
     * @param vXGroupUsers
     *            the vXGroupUsers to set
     */
    public void setVXGroupUsers(List<VXGroupUser> vXGroupUsers) {
	this.vXGroupUsers = vXGroupUsers;
    }

    @Override
    public int getListSize() {
	if (vXGroupUsers != null) {
	    return vXGroupUsers.size();
	}
	return 0;
    }

    @Override
    public List<VXGroupUser> getList() {
	return vXGroupUsers;
    }

}
