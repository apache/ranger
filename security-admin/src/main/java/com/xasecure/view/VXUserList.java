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
 * List wrapper class for VXUser
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
public class VXUserList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXUser> vXUsers = new ArrayList<VXUser>();

    public VXUserList() {
	super();
    }

    public VXUserList(List<VXUser> objList) {
	super(objList);
	this.vXUsers = objList;
    }

    /**
     * @return the vXUsers
     */
    public List<VXUser> getVXUsers() {
	return vXUsers;
    }

    /**
     * @param vXUsers
     *            the vXUsers to set
     */
    public void setVXUsers(List<VXUser> vXUsers) {
	this.vXUsers = vXUsers;
    }

    @Override
    public int getListSize() {
	if (vXUsers != null) {
	    return vXUsers.size();
	}
	return 0;
    }

    @Override
    public List<VXUser> getList() {
	return vXUsers;
    }

}
