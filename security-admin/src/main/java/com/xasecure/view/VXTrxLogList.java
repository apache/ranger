package com.xasecure.view;
/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
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
public class VXTrxLogList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXTrxLog> vXTrxLogs = new ArrayList<VXTrxLog>();

    public VXTrxLogList() {
	super();
    }

    public VXTrxLogList(List<VXTrxLog> objList) {
	super(objList);
	this.vXTrxLogs = objList;
    }

    /**
     * @return the vXTrxLogs
     */
    public List<VXTrxLog> getVXTrxLogs() {
	return vXTrxLogs;
    }

    /**
     * @param vXTrxLogs
     *            the vXTrxLogs to set
     */
    public void setVXTrxLogs(List<VXTrxLog> vXTrxLogs) {
	this.vXTrxLogs = vXTrxLogs;
    }

    @Override
    public int getListSize() {
	if (vXTrxLogs != null) {
	    return vXTrxLogs.size();
	}
	return 0;
    }

    @Override
    public List<VXTrxLog> getList() {
	return vXTrxLogs;
    }

}
