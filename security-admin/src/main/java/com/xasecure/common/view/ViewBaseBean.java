package com.xasecure.common.view;

import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.xasecure.common.XACommonEnums;
import com.xasecure.entity.XXDBBase;

public class ViewBaseBean implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private XXDBBase mObj = null;

    /**
     * @return the gjObj
     */
    @XmlTransient
    @JsonIgnore
    public XXDBBase getMObj() {
	return mObj;
    }

    /**
     * @param gjObj
     *            the gjObj to set
     */
    public void setMObj(XXDBBase gjObj) {
	this.mObj = gjObj;
    }

    @XmlTransient
    @JsonIgnore
    public int getMyClassType() {
	return XACommonEnums.CLASS_TYPE_NONE;
    }
}