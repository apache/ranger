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
 * List wrapper class for VXCredentialStore

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
public class VXCredentialStoreList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXCredentialStore> vXCredentialStores = new ArrayList<VXCredentialStore>();

    public VXCredentialStoreList() {
	super();
    }

    public VXCredentialStoreList(List<VXCredentialStore> objList) {
	super(objList);
	this.vXCredentialStores = objList;
    }

    /**
     * @return the vXCredentialStores
     */
    public List<VXCredentialStore> getVXCredentialStores() {
	return vXCredentialStores;
    }

    /**
     * @param vXCredentialStores
     *            the vXCredentialStores to set
     */
    public void setVXCredentialStores(List<VXCredentialStore> vXCredentialStores) {
	this.vXCredentialStores = vXCredentialStores;
    }

    @Override
    public int getListSize() {
	if (vXCredentialStores != null) {
	    return vXCredentialStores.size();
	}
	return 0;
    }

    @Override
    public List<VXCredentialStore> getList() {
	return vXCredentialStores;
    }

}
