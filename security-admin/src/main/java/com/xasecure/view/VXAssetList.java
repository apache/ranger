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
 * List wrapper class for VXAsset
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
public class VXAssetList extends VList {
	private static final long serialVersionUID = 1L;
    List<VXAsset> vXAssets = new ArrayList<VXAsset>();

    public VXAssetList() {
	super();
    }

    public VXAssetList(List<VXAsset> objList) {
	super(objList);
	this.vXAssets = objList;
    }

    /**
     * @return the vXAssets
     */
    public List<VXAsset> getVXAssets() {
	return vXAssets;
    }

    /**
     * @param vXAssets
     *            the vXAssets to set
     */
    public void setVXAssets(List<VXAsset> vXAssets) {
	this.vXAssets = vXAssets;
    }

    @Override
    public int getListSize() {
	if (vXAssets != null) {
	    return vXAssets.size();
	}
	return 0;
    }

    @Override
    public List<VXAsset> getList() {
	return vXAssets;
    }

}
