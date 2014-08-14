package com.xasecure.common.view;

/*
 * Copyright (c) 2014 XASecure.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure.
 */

import java.util.List;

import javax.xml.bind.annotation.*;

@XmlRootElement
public abstract class VList extends ViewBaseBean implements
	java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Start index for the result
     */
    protected int startIndex;
    /**
     * Page size used for the result
     */
    protected int pageSize;
    /**
     * Total records in the database for the given search conditions
     */
    protected long totalCount;
    /**
     * Number of rows returned for the search condition
     */
    protected int resultSize;
    /**
     * Sort type. Either desc or asc
     */
    protected String sortType;
    /**
     * Comma seperated list of the fields for sorting
     */
    protected String sortBy;

    protected long queryTimeMS = System.currentTimeMillis();

    /**
     * Default constructor. This will set all the attributes to default value.
     */
    public VList() {
    }

    /**
     * Initialize with existing list
     *
     * @param size
     */
    public VList(@SuppressWarnings("rawtypes") List objectList) {
	int size = 0;
	if (objectList != null) {
	    size = objectList.size();
	}

	startIndex = 0;
	pageSize = size;
	totalCount = size;
	resultSize = size;
	sortType = null;
	sortBy = null;
    }

    abstract public int getListSize();

    abstract public List<?> getList();

    /**
     * This method sets the value to the member attribute <b>startIndex</b>. You
     * cannot set null to the attribute.
     *
     * @param startIndex
     *            Value to set member attribute <b>startIndex</b>
     */
    public void setStartIndex(int startIndex) {
	this.startIndex = startIndex;
    }


    /**
     * This method sets the value to the member attribute <b>pageSize</b>. You
     * cannot set null to the attribute.
     *
     * @param pageSize
     *            Value to set member attribute <b>pageSize</b>
     */
    public void setPageSize(int pageSize) {
	this.pageSize = pageSize;
    }


    /**
     * This method sets the value to the member attribute <b>totalCount</b>. You
     * cannot set null to the attribute.
     *
     * @param totalCount
     *            Value to set member attribute <b>totalCount</b>
     */
    public void setTotalCount(long totalCount) {
	this.totalCount = totalCount;
    }

    

    /**
     * This method sets the value to the member attribute <b>resultSize</b>. You
     * cannot set null to the attribute.
     *
     * @param resultSize
     *            Value to set member attribute <b>resultSize</b>
     */
    public void setResultSize(int resultSize) {
	this.resultSize = resultSize;
    }

    /**
     * Returns the value for the member attribute <b>resultSize</b>
     *
     * @return int - value of member attribute <b>resultSize</b>.
     */
    public int getResultSize() {
	return getListSize();
    }

    /**
     * This method sets the value to the member attribute <b>sortType</b>. You
     * cannot set null to the attribute.
     *
     * @param sortType
     *            Value to set member attribute <b>sortType</b>
     */
    public void setSortType(String sortType) {
	this.sortType = sortType;
    }



    /**
     * This method sets the value to the member attribute <b>sortBy</b>. You
     * cannot set null to the attribute.
     *
     * @param sortBy
     *            Value to set member attribute <b>sortBy</b>
     */
    public void setSortBy(String sortBy) {
	this.sortBy = sortBy;
    }

   

  

    

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	return "VList [startIndex=" + startIndex + ", pageSize="
		+ pageSize + ", totalCount=" + totalCount
		+ ", resultSize=" + resultSize + ", sortType="
		+ sortType + ", sortBy=" + sortBy + ", queryTimeMS="
		+ queryTimeMS + "]";
    }

}
