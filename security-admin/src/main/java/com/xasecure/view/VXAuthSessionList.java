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
 * List wrapper class for VXAuthSession
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
public class VXAuthSessionList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXAuthSession> vXAuthSessions = new ArrayList<VXAuthSession>();

    public VXAuthSessionList() {
	super();
    }

    public VXAuthSessionList(List<VXAuthSession> objList) {
	super(objList);
	this.vXAuthSessions = objList;
    }

    /**
     * @return the vXAuthSessions
     */
    public List<VXAuthSession> getVXAuthSessions() {
	return vXAuthSessions;
    }

    /**
     * @param vXAuthSessions
     *            the vXAuthSessions to set
     */
    public void setVXAuthSessions(List<VXAuthSession> vXAuthSessions) {
	this.vXAuthSessions = vXAuthSessions;
    }

    @Override
    public int getListSize() {
	if (vXAuthSessions != null) {
	    return vXAuthSessions.size();
	}
	return 0;
    }

    @Override
    public List<VXAuthSession> getList() {
	return vXAuthSessions;
    }

}
